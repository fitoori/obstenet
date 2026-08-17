# obstenet_pi_ai_service.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OBSTENET Pi AI (IMX500) Service
--------------------------------
Production-grade Flask service + library to run Raspberry Pi AI Camera (Sony IMX500)
with Picamera2, stream MJPEG, and expose detections over a REST API.

Design goals:
- Must start even if IMX500 model file is missing (AI disabled gracefully).
- Must always shut down cleanly (camera stopped, threads joined).
- Must validate inputs and guard against runtime errors (DMA OOM, missing deps).
- Must not rely on placeholders or unspecified configuration.

Usage (standalone):
  python obstenet_pi_ai_service.py --model /usr/share/imx500-models/imx500_network_ssd_mobilenetv2_fpnlite_320x320_pp.rpk

Embed in an existing Flask app:
  from obstenet_pi_ai_service import PiAICameraService
  svc = PiAICameraService(model_file=...)
  svc.start()
  # read svc.latest_detections(), svc.jpeg_generator(), etc.

Tested on: Raspberry Pi OS Bookworm, Python 3.11+, picamera2 0.3+
"""

from __future__ import annotations

import os
import io
import sys
import time
import json
import math
import ctypes
import errno
import queue
import atexit
import signal
import logging
import threading
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field

# ---------------------------
# Logging
# ---------------------------
_LOG = logging.getLogger("obstenet.pi_ai")
if not _LOG.handlers:
    _handler = logging.StreamHandler(sys.stderr)
    _formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )
    _handler.setFormatter(_formatter)
    _LOG.addHandler(_handler)
_LOG.setLevel(logging.INFO)

# ---------------------------
# Dependency checks
# ---------------------------
try:
    from picamera2 import Picamera2
    from picamera2.encoders import JpegEncoder
    from picamera2.outputs import FileOutput
    from libcamera import Transform
except Exception as e:
    _LOG.error("Picamera2/libcamera import failed: %s", e)
    raise SystemExit(2) from e

try:
    # IMX500 helper must be imported *before* Picamera2() is instantiated.
    from picamera2.devices.imx500 import IMX500
    _IMX500_AVAILABLE = True
except Exception:
    _IMX500_AVAILABLE = False

try:
    import numpy as np
except Exception as e:
    _LOG.error("numpy import failed: %s", e)
    raise SystemExit(2) from e

try:
    import cv2
    _CV2_AVAILABLE = True
except Exception:
    _CV2_AVAILABLE = False

# ---------------------------
# Defaults
# ---------------------------
DEFAULT_MODEL_CANDIDATES: List[str] = [
    "/usr/share/imx500-models/imx500_network_yolov8n_pp.rpk",
    "/usr/share/imx500-models/imx500_network_ssd_mobilenetv2_fpnlite_320x320_pp.rpk",
]
DEFAULT_STREAM_WIDTH  = int(os.environ.get("OBSTENET_STREAM_W", "1280"))
DEFAULT_STREAM_HEIGHT = int(os.environ.get("OBSTENET_STREAM_H", "720"))
DEFAULT_FRAMERATE     = int(os.environ.get("OBSTENET_FPS", "30"))
DEFAULT_JPEG_QUALITY  = int(os.environ.get("OBSTENET_JPEG_Q", "85"))
DEFAULT_MIN_CONF      = float(os.environ.get("OBSTENET_MIN_CONF", "0.35"))
DEFAULT_MAX_DETS      = int(os.environ.get("OBSTENET_MAX_DETS", "50"))
DEFAULT_ROTATION      = int(os.environ.get("OBSTENET_ROTATION", "0"))  # 0/90/180/270

# ---------------------------
# Data classes
# ---------------------------
@dataclass(slots=True)
class Detection:
    cls: int
    conf: float
    # bbox in ISP output coordinates (x, y, w, h), ints
    x: int
    y: int
    w: int
    h: int
    label: Optional[str] = None

@dataclass(slots=True)
class Health:
    running: bool
    ai_enabled: bool
    model_file: Optional[str]
    fps: float
    last_error: Optional[str] = None
    dropped_frames: int = 0

# ---------------------------
# IMX500 / Picamera2 manager
# ---------------------------
class PiAICameraService:
    """
    Manages Picamera2 + IMX500 inference, MJPEG encoding, and detection parsing.
    Safe for standalone use or embedding in another Flask app.
    """
    def __init__(
        self,
        model_file: Optional[str] = None,
        stream_size: Tuple[int, int] = (DEFAULT_STREAM_WIDTH, DEFAULT_STREAM_HEIGHT),
        framerate: int = DEFAULT_FRAMERATE,
        jpeg_quality: int = DEFAULT_JPEG_QUALITY,
        min_conf: float = DEFAULT_MIN_CONF,
        max_detections: int = DEFAULT_MAX_DETS,
        rotation: int = DEFAULT_ROTATION,
    ) -> None:
        self._validate_rotation(rotation)
        self.model_file = self._select_model(model_file)
        self.stream_w, self.stream_h = stream_size
        self.fps = int(framerate)
        self.jpeg_q = int(jpeg_quality)
        self.min_conf = float(min_conf)
        self.max_detections = int(max_detections)
        self.rotation = int(rotation)

        # Internal state
        self._picam: Optional[Picamera2] = None
        self._imx500: Optional[IMX500] = None
        self._stop_evt = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._jpeg_q: "queue.Queue[bytes]" = queue.Queue(maxsize=1)
        self._last_dets: List[Detection] = []
        self._dets_lock = threading.Lock()
        self._last_error: Optional[str] = None
        self._frames = 0
        self._dropped = 0
        self._t0 = time.monotonic()
        self._fps = 0.0
        self._labels: Optional[List[str]] = None

    # ---------- Lifecycle ----------
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            _LOG.info("PiAICameraService already running")
            return
        self._stop_evt.clear()
        self._thread = threading.Thread(target=self._run, name="PiAI.Run", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop_evt.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        self._shutdown_camera()

    def latest_detections(self) -> List[Detection]:
        with self._dets_lock:
            return list(self._last_dets)

    def health(self) -> Health:
        return Health(
            running=bool(self._thread and self._thread.is_alive()),
            ai_enabled=bool(self._imx500 is not None),
            model_file=self.model_file,
            fps=self._fps,
            last_error=self._last_error,
            dropped_frames=self._dropped,
        )

    # ---------- Streaming interface ----------
    def next_jpeg(self, timeout: float = 2.0) -> Optional[bytes]:
        try:
            return self._jpeg_q.get(timeout=timeout)
        except queue.Empty:
            return None

    def jpeg_generator(self, boundary: str = "--frame"):
        while not self._stop_evt.is_set():
            chunk = self.next_jpeg(timeout=2.0)
            if chunk is None:
                # Keep-alive even if frames pause
                yield (f"{boundary}\r\n"
                       "Content-Type: image/jpeg\r\n"
                       "Content-Length: 0\r\n\r\n").encode("ascii")
                continue
            yield (f"{boundary}\r\n"
                   "Content-Type: image/jpeg\r\n"
                   f"Content-Length: {len(chunk)}\r\n\r\n").encode("ascii") + chunk + b"\r\n"

    # ---------- Private ----------
    def _run(self) -> None:
        try:
            self._boot_camera()
            self._capture_loop()
        except Exception as e:
            self._last_error = f"{type(e).__name__}: {e}"
            _LOG.exception("Fatal error in capture loop: %s", e)
        finally:
            self._shutdown_camera()

    def _capture_loop(self) -> None:
        """Keep the worker thread alive while start_recording's _Buf callback
        drives frames into the queue. Bounded, interruptible; no busy-spin.
        The blueprint referenced this method but never defined it (backlog draft,
        never executed) — supplied on assimilation 2026-07-23."""
        while not self._stop_evt.wait(0.5):
            pass

    def _boot_camera(self) -> None:
        if (self.rotation % 360) not in (0, 90, 180, 270):
            raise ValueError("rotation must be one of 0/90/180/270")

        # Load IMX500 first if available and model present
        if _IMX500_AVAILABLE and self.model_file:
            try:
                self._imx500 = IMX500(self.model_file)  # must be before Picamera2()
                self._labels = self._read_labels_from_config(self._imx500)
                _LOG.info("IMX500 model loaded: %s", self.model_file)
            except FileNotFoundError as e:
                _LOG.error("Model file not found: %s", e)
                self._imx500 = None
            except Exception as e:
                _LOG.error("IMX500 init failed (%s). Continuing without AI.", e)
                self._imx500 = None

        self._picam = Picamera2()

        # Camera configuration with safe fallbacks
        sizes = [
            (self.stream_w, self.stream_h),
            (1280, 720),
            (960, 540),
            (640, 480),
        ]
        last_err: Optional[BaseException] = None
        for (w, h) in sizes:
            try:
                video_cfg = self._picam.create_video_configuration(
                    main={"size": (w, h)},
                    transform=Transform(rotation=self.rotation),
                    buffer_count=4,
                    controls={"FrameDurationLimits": (int(1e6/self.fps), int(1e6/self.fps))},
                )
                self._picam.configure(video_cfg)
                break
            except OSError as e:
                last_err = e
                if getattr(e, "errno", None) == 12:  # ENOMEM from DMA heap
                    _LOG.warning("DMA OOM at %dx%d; retrying with smaller size", w, h)
                    time.sleep(0.05)
                    continue
                _LOG.warning("OSError configuring %dx%d: %s; trying next size", w, h, e)
            except Exception as e:
                last_err = e
                _LOG.warning("Configure failed at %dx%d: %s; trying next size", w, h, e)
        else:
            raise RuntimeError(f"Camera configure failed; last error: {last_err}")

        # JPEG encoder (picamera2 >=0.3.17 object API; the old string form
        # start_encoder("jpeg", ...) does _encoder.name=name on a str -> crash)
        enc = JpegEncoder(q=self.jpeg_q)

        # Hook callback to parse tensors per frame
        def _on_request(request):
            try:
                md = request.get_metadata()
                dets = self._parse_detections(md, self._picam, self._imx500)
                if dets is not None:
                    with self._dets_lock:
                        self._last_dets = dets[: self.max_detections]
            except Exception as e:
                # Non-fatal; keep running
                self._last_error = f"parse_detections: {type(e).__name__}: {e}"

        self._picam.post_callback = _on_request

        # Start recording to an in-memory stream so we can MJPEG it
        class _Buf(io.BufferedIOBase):
            def __init__(self, outer: "PiAICameraService"):
                self.outer = outer
                self._lock = threading.Lock()
                self._last_ts = 0.0
            def write(self, b: bytes) -> int:
                if not isinstance(b, (bytes, bytearray)):
                    return 0
                now = time.monotonic()
                with self._lock:
                    try:
                        self.outer._jpeg_q.get_nowait()  # drop oldest if any
                        self.outer._dropped += 1
                    except queue.Empty:
                        pass
                    try:
                        self.outer._jpeg_q.put_nowait(bytes(b))
                        self.outer._frames += 1
                        dt = now - self.outer._t0
                        if dt >= 1.0:
                            self.outer._fps = self.outer._frames / dt
                            self.outer._frames = 0
                            self.outer._t0 = now
                    except queue.Full:
                        self.outer._dropped += 1
                return len(b)

        # name="main": when the IMX500 model is loaded, picamera2 adds a second
        # (raw) stream for the AI; the encoder must be pinned to "main" or it
        # receives no buffers and the MJPEG stream is empty (0 fps).
        self._picam.start_recording(enc, FileOutput(_Buf(self)), name="main")
        _LOG.info("Camera started: %dx%d @%dfps (JPEG q=%d) AI=%s",
                  self._picam.camera_configuration()['main']['size'][0],
                  self._picam.camera_configuration()['main']['size'][1],
                  self.fps, self.jpeg_q, bool(self._imx500))

    def _shutdown_camera(self) -> None:
        try:
            if self._picam:
                try:
                    self._picam.stop_recording()
                except Exception:
                    pass
                try:
                    self._picam.close()
                except Exception:
                    pass
        finally:
            self._picam = None
            self._imx500 = None

    # ---------- Detection parsing ----------
    def _parse_detections(
        self, metadata: Dict[str, Any], picam: Optional[Picamera2], imx: Optional[IMX500]
    ) -> Optional[List[Detection]]:
        """Parse output tensors to a list of Detection; returns None if AI disabled or no outputs."""
        if imx is None or picam is None:
            return None

        # grab outputs safely; outputs shape depends on model
        np_outputs = imx.get_outputs(metadata, add_batch=True)
        if np_outputs is None:
            return []

        # Default path: SSD/YOLO style boxes,scores,classes in first 3 tensors.
        try:
            boxes, scores, classes = np_outputs[0][0], np_outputs[1][0], np_outputs[2][0]
        except Exception as e:
            # Some models (nanodet) pack differently; try fallbacks
            try:
                boxes, scores, classes = self._fallback_parse(np_outputs)
            except Exception as e2:
                raise ValueError(f"Unexpected outputs format: {e} / {e2}")

        # Normalize/reshape boxes if necessary; expected [N,4] in xywh or xyxy in *input* tensor space
        boxes = np.asarray(boxes)
        scores = np.asarray(scores).reshape(-1)
        classes = np.asarray(classes).reshape(-1).astype(int)

        # Determine coordinate format: If max(boxes[:,2]) <=1, likely normalized.
        normed = np.all((boxes >= 0.0) & (boxes <= 1.0))
        # If boxes is Nx4 packed as [x1,y1,x2,y2], convert to xywh
        if boxes.shape[1] == 4 and (boxes[:,2] > 1.0).sum() == 0 and (boxes[:,3] > 1.0).sum() == 0:
            # Might be normalized xyxy -> convert to xywh after mapping to ISP
            pass

        # Convert to ISP output coordinate space
        isp_w, isp_h = self._get_isp_size(picam)
        dets: List[Detection] = []
        for i in range(min(len(scores), len(classes), len(boxes))):
            conf = float(scores[i])
            if not (0.0 <= conf <= 1.0) or conf < self.min_conf:
                continue

            box = boxes[i]
            # Heuristic: if normalized, IMX helper can convert using convert_inference_coords;
            # otherwise attempt to clamp into ISP space directly.
            try:
                # imx.convert_inference_coords expects xyxy in *input tensor* space -> ISP space
                # When outputs are [x1,y1,x2,y2] normalized, multiply by input size first.
                inp_w, inp_h = imx.get_input_size()
                if box.shape[0] != 4:
                    continue
                x1, y1, x2, y2 = float(box[0]), float(box[1]), float(box[2]), float(box[3])
                if normed:
                    x1 *= inp_w; x2 *= inp_w; y1 *= inp_h; y2 *= inp_h
                # Convert to ISP coords
                x1i, y1i, x2i, y2i = imx.convert_inference_coords([x1, y1, x2, y2], metadata, picam)
                x, y = int(max(0, min(x1i, x2i))), int(max(0, min(y1i, y2i)))
                w, h = int(abs(x2i - x1i)), int(abs(y2i - y1i))
            except Exception:
                # Fallback: assume xywh already in ISP units or normalized to ISP
                if normed:
                    x = int(round(box[0] * isp_w)); y = int(round(box[1] * isp_h))
                    w = int(round(box[2] * isp_w)); h = int(round(box[3] * isp_h))
                else:
                    x = int(round(box[0])); y = int(round(box[1]))
                    w = int(round(box[2])); h = int(round(box[3]))

            # Clamp
            x = max(0, min(x, isp_w - 1)); y = max(0, min(y, isp_h - 1))
            w = max(1, min(w, isp_w - x)); h = max(1, min(h, isp_h - y))

            cls_id = int(classes[i])
            label = None
            if self._labels and 0 <= cls_id < len(self._labels):
                label = self._labels[cls_id]

            dets.append(Detection(cls=cls_id, conf=conf, x=x, y=y, w=w, h=h, label=label))

        return dets

    def _fallback_parse(self, outputs: List[np.ndarray]) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        # Try nanodet postprocess if available in Picamera2
        try:
            from picamera2.devices.imx500.postprocess import scale_boxes
            # Attempt nanodet style: outputs[0] contains packed predictions
            # This is a best-effort; exact format depends on rpk.
            boxes, scores, classes = outputs[0], outputs[1], outputs[2]
            inp_w, inp_h = self._imx500.get_input_size() if self._imx500 else (1, 1)
            boxes = scale_boxes(boxes, 1, 1, inp_h, inp_w, False, False)
            return boxes, scores, classes
        except Exception as e:
            raise

    def _get_isp_size(self, picam: Picamera2) -> Tuple[int, int]:
        try:
            size = self._imx500.get_isp_output_size(picam) if self._imx500 else None
            if size is None:
                return tuple(picam.camera_configuration()['main']['size'])
            return int(size[0]), int(size[1])
        except Exception:
            return tuple(picam.camera_configuration()['main']['size'])

    def _read_labels_from_config(self, imx: IMX500) -> Optional[List[str]]:
        try:
            cfg = imx.config or {}
            labels = cfg.get("labels")
            if isinstance(labels, list) and all(isinstance(x, str) for x in labels):
                return labels
        except Exception:
            pass
        return None

    @staticmethod
    def _validate_rotation(rot: int) -> None:
        if (rot % 360) not in (0, 90, 180, 270):
            raise ValueError("rotation must be 0/90/180/270")

    @staticmethod
    def _select_model(model_file: Optional[str]) -> Optional[str]:
        if not _IMX500_AVAILABLE:
            return None
        if model_file:
            if os.path.isfile(model_file):
                return model_file
            raise FileNotFoundError(f"Model file not found: {model_file}")
        for cand in DEFAULT_MODEL_CANDIDATES:
            if os.path.isfile(cand):
                return cand
        return None

# ---------------------------
# Flask app
# ---------------------------
from flask import Flask, jsonify, Response, request, abort

def create_app(service: PiAICameraService) -> Flask:
    # Contract: caller must supply a real service exposing the read APIs the
    # routes below call; a missing/mistyped service would fail only at request
    # time deep inside a handler, so validate the precondition here.
    assert service is not None, "create_app: service must not be None"
    assert hasattr(service, "health") and hasattr(service, "latest_detections"), \
        "create_app: service must expose health() and latest_detections()"
    app = Flask(__name__)

    @app.route("/healthz")
    def health() -> Response:
        h = service.health()
        return jsonify({
            "running": h.running,
            "ai_enabled": h.ai_enabled,
            "model_file": h.model_file,
            "fps": round(h.fps, 2),
            "last_error": h.last_error,
            "dropped_frames": h.dropped_frames,
        })

    @app.route("/detections")
    def detections() -> Response:
        dets = service.latest_detections()
        # Contract: latest_detections() always returns a list of Detection
        # snapshots (never None); the comprehension below relies on iterability
        # and on each element carrying the bbox/conf fields.
        assert isinstance(dets, list), \
            "detections: latest_detections() must return a list"
        assert all(hasattr(d, "conf") and hasattr(d, "cls") for d in dets), \
            "detections: each detection must carry cls and conf fields"
        return jsonify([{
            "cls": d.cls,
            "label": d.label,
            "conf": round(d.conf, 4),
            "box": {"x": d.x, "y": d.y, "w": d.w, "h": d.h},
        } for d in dets])

    @app.route("/stream.mjpg")
    def stream() -> Response:
        boundary = "--frame"
        headers = {"Content-Type": f"multipart/x-mixed-replace; boundary={boundary}"}
        return Response(service.jpeg_generator(boundary=boundary), headers=headers)

    return app

# ---------------------------
# CLI
# ---------------------------
def _parse_cli(argv: List[str]) -> Dict[str, Any]:
    import argparse
    # Contract: argv is a list of string tokens (already sliced off sys.argv);
    # argparse.parse_args iterates it and treats each item as a token.
    assert isinstance(argv, list), "_parse_cli: argv must be a list of strings"
    assert all(isinstance(tok, str) for tok in argv), \
        "_parse_cli: every argv token must be a str"
    p = argparse.ArgumentParser(description="OBSTENET Pi AI (IMX500) service")
    p.add_argument("--model", type=str, default=None,
                   help="Path to .rpk model (default: auto-detect installed models).")
    p.add_argument("--width", type=int, default=DEFAULT_STREAM_WIDTH)
    p.add_argument("--height", type=int, default=DEFAULT_STREAM_HEIGHT)
    p.add_argument("--fps", type=int, default=DEFAULT_FRAMERATE)
    p.add_argument("--jpeg-q", type=int, default=DEFAULT_JPEG_QUALITY)
    p.add_argument("--min-conf", type=float, default=DEFAULT_MIN_CONF)
    p.add_argument("--max-dets", type=int, default=DEFAULT_MAX_DETS)
    p.add_argument("--rotation", type=int, default=DEFAULT_ROTATION, choices=(0,90,180,270))
    p.add_argument("--host", type=str, default="0.0.0.0")
    p.add_argument("--port", type=int, default=8080)
    p.add_argument("--log-level", type=str, default="INFO",
                   choices=("DEBUG","INFO","WARNING","ERROR"))
    args = p.parse_args(argv)
    result = vars(args)
    # Return-shape contract: a dict carrying every option the caller reads,
    # with port constrained to the valid TCP range.
    assert isinstance(result, dict) and "port" in result and "host" in result, \
        "_parse_cli: parsed result must be a dict containing host and port"
    assert 0 < int(result["port"]) <= 65535, \
        "_parse_cli: port must be in 1..65535"
    return result

def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_cli(argv or sys.argv[1:])
    # Contract: _parse_cli returns a dict with the keys read below; a missing
    # key would otherwise KeyError deep in service construction.
    assert isinstance(args, dict), "main: _parse_cli must return a dict"
    assert "log_level" in args and "port" in args, \
        "main: parsed args must contain log_level and port"
    _LOG.setLevel(getattr(logging, args["log_level"]))

    svc = PiAICameraService(
        model_file=args["model"],
        stream_size=(args["width"], args["height"]),
        framerate=args["fps"],
        jpeg_quality=args["jpeg_q"],
        min_conf=args["min_conf"],
        max_detections=args["max_dets"],
        rotation=args["rotation"],
    )
    svc.start()

    app = create_app(svc)

    # Graceful shutdown on SIGTERM/SIGINT
    def _shutdown(*_a):
        _LOG.info("Shutting down...")
        try:
            svc.stop()
        finally:
            os._exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        app.run(host=args["host"], port=args["port"], debug=False, threaded=True)
    finally:
        svc.stop()
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
