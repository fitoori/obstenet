#!/usr/bin/env python3
# OBSTENET — Opposable Bounded Surveillance & Tracking with Elastic Networking
# -*- coding: utf-8 -*-

# ----------------------------
# --- Servo geometry / orientation ---
PAN_RANGE_DEG: tuple[int, int]  = (-90, 90)
TILT_RANGE_DEG: tuple[int, int] = (-45, 90)
PAN_INVERT: bool  = False
TILT_INVERT: bool = True
PAN_HOME_DEG: int  = 0
TILT_HOME_DEG: int = 70   # Neutral forward; avoids starting near a mechanical stop

# --- Hardware calibration / soft limits ---
PAN_HW_OFFSET_DEG: float  = 0.0
TILT_HW_OFFSET_DEG: float = 0.0
SOFT_LIMIT_MARGIN_DEG: int = 3

# --- Motion control (slew / accel limiting) ---
PAN_SLEW_DEG_S: float  = 20.0
TILT_SLEW_DEG_S: float = 20.0
SLEW_MIN_STEP_DEG: float = 0.8
SLEW_MAX_STEP_DEG: float = 1.0

# --- Stream orientation (what users see) ---
STREAM_ROTATION: int = 0           # 0/90/180/270
STREAM_HFLIP: bool  = False
STREAM_VFLIP: bool  = False

# --- Control semantics (post-orientation) ---
INVERT_UI_UPDOWN: bool = False
INVERT_UI_LEFTRIGHT: bool = False

# --- Camera performance / quality (tuned for lower latency) ---
# Prefer smaller frame first and 2 buffers to cut pipeline latency.
FALLBACK_SIZES: list[tuple[int, int]] = [(640, 480), (960, 540), (1280, 720)]
STREAM_BUFFER_COUNT: int = 2
JPEG_QUALITY: int = 50
TARGET_FPS_MIN: int = 15           # try to keep >= this
TARGET_FPS_MAX: int = 30           # cap frame time to ~33ms

# --- Servo reliability / safety ---
SERVO_MIN_INTERVAL_S: float = 0.02   # 20ms between I²C writes
SERVO_RETRIES: int = 3
KEEPALIVE_PERIOD_S: float = 2.0
SERVO_IDLE_RELEASE_S: float = 5.0
SERVO_RELEASE_AFTER_ACTION: bool = False
SERVO_RELEASE_SETTLE_S: float = 0.15
SERVO_SEND_TIMEOUT_S: float = 6.0

# --- Gestures (macros) ---
GESTURE_AMPLITUDE_DEG: int = 15
GESTURE_CYCLES: int = 1
GESTURE_STEP_DELAY_S: float = 0.18

# --- Subprocess supervision ---
HEARTBEAT_PERIOD_S: float = 0.5
HEARTBEAT_GRACE_S:  float = 3.0

# --- Web/UI / server ---
DEFAULT_STEP_DEG: int = 10
HOST: str = "0.0.0.0"
PORT: int = 8000

# --- Resilience: camera watchdog/backoff ---
CAM_STALL_RESTART_S: float = 2.5        # no frame for this long => restart camera
CAM_MIN_UPTIME_OK_S: float = 20.0
CAM_RESTART_BACKOFF_S: float = 5.0
CAM_RESTART_BACKOFF_MAX_S: float = 60.0
CAM_RESTARTS_MAX_PER_HOUR: int = 120

# --- Resilience: request shaping ---
MOVE_MIN_INTERVAL_S: float = 0.02

import atexit
import glob
import io
import json
import logging
import os
import resource
import signal
import socket
import struct
import sys
import threading
import time
import uuid
from collections import deque, defaultdict
from dataclasses import dataclass
from typing import Optional, Dict, Tuple, List

import faulthandler
from flask import Flask, Response, abort, jsonify, request, make_response

# Hardware libs (fail-fast with guidance)
try:
    from picamera2 import Picamera2
    from picamera2.encoders import JpegEncoder
    from picamera2.outputs import FileOutput
    from libcamera import Transform
except Exception as e:
    sys.stderr.write(
        "ERROR: Picamera2/libcamera required. Install with:\n"
        "  sudo apt update && sudo apt install -y python3-picamera2\n"
        f"Import failed: {e}\n"
    ); sys.exit(2)

try:
    import pantilthat
except Exception as e:
    sys.stderr.write(
        "ERROR: pantilthat required. Enable I2C in raspi-config and:\n"
        "  python3 -m pip install --upgrade pantilthat\n"
        f"Import failed: {e}\n"
    ); sys.exit(2)

from multiprocessing import Process, Queue as MPQueue, Event as MPEvent

# -----------------------------------------------------------------------------
# Logging / crash diagnostics
# -----------------------------------------------------------------------------
app = Flask(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = app.logger

try:
    faulthandler.enable()
    faulthandler.register(signal.SIGUSR1, all_threads=True)
except Exception:
    pass

def _thread_excepthook(args):
    log.error("Thread %s crashed: %s", getattr(args, "thread", None), args.exc_value,
              exc_info=(args.exc_type, args.exc_value, args.exc_traceback))
try:
    threading.excepthook = _thread_excepthook  # type: ignore[attr-defined]
except Exception:
    pass

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _clamp(x: float, lo: float, hi: float) -> int:
    return int(lo) if x < lo else int(hi) if x > hi else int(round(x))

@dataclass
class _Cmd:
    rid: str
    op: str               # "move"|"set"|"home"|"state"|"release"|"shutdown"
    dp: float = 0.0
    dt: float = 0.0
    pan: Optional[float] = None
    tilt: Optional[float] = None

@dataclass
class _Resp:
    rid: str
    ok: bool
    kind: str = "response"  # or "heartbeat"
    state: Optional[Dict[str, int]] = None
    error: Optional[str] = None

# -----------------------------------------------------------------------------
# Servo child process + manager (unchanged semantics; resilient)
# -----------------------------------------------------------------------------
def _servo_child(cmd_q: MPQueue, resp_q: MPQueue, stop_evt: MPEvent) -> None:
    import pantilthat, time, math  # reimport in child

    PLO, PHI = PAN_RANGE_DEG
    TLO, THI = TILT_RANGE_DEG
    p_sign = -1 if PAN_INVERT else 1
    t_sign = -1 if TILT_INVERT else 1

    try:
        if hasattr(pantilthat, "frequency"):
            pantilthat.frequency(50)
    except Exception:
        pass


    def _i2c_ping() -> bool:
        """Quickly probe likely PanTilt HAT I²C addresses to confirm the bus/device is responsive.
        Tries SMBus 'write_quick' then 'read_byte' on addresses commonly used by Pan/Tilt boards.
        Returns True on any ACK; False if no device responds.
        """
        try:
            from smbus2 import SMBus  # type: ignore
        except Exception:
            # If smbus2 isn't present, we can't actively test; assume OK to avoid false faults.
            return True
        addrs = (0x15, 0x40)  # Pimoroni PIC16F1503 (0x15) or PCA9685 (0x40)
        try:
            with SMBus(1) as _bus:
                for _addr in addrs:
                    try:
                        # Prefer a write_quick (no data) which is side-effect free if supported.
                        try:
                            _bus.write_quick(_addr)
                            return True
                        except Exception:
                            # Fallback to a harmless single byte read (many devices NACK if unsupported)
                            try:
                                _ = _bus.read_byte(_addr)
                                return True
                            except Exception:
                                pass
                    except Exception:
                        # Try next address
                        pass
        except Exception:
            # Opening bus failed
            return False
        return False

    def _safe_enable(on: bool) -> bool:
        """Enable/disable both servos with verification.
        Returns True if the device ACKed after the operation; False otherwise.
        """
        last_exc = None
        for _attempt in range(int(max(1, SERVO_RETRIES))):
            try:
                pantilthat.servo_enable(1, bool(on))
                pantilthat.servo_enable(2, bool(on))
                # Give the HAT a brief moment if enabling
                if on:
                    time.sleep(0.02)
                if _i2c_ping():
                    return True
            except Exception as e:
                last_exc = e
            # brief backoff
            try:
                time.sleep(0.05)
            except Exception:
                pass
        # If we reach here, enabling/disabling did not verify
        try:
            # Report heartbeat fault so the watchdog can kick in if needed
            resp_q.put(_Resp(rid="hb", ok=False, kind="heartbeat", error=f"servo_enable_failed: {last_exc}"))
        except Exception:
            pass
        return False
        try:
            pantilthat.servo_enable(1, on)
            pantilthat.servo_enable(2, on)
        except Exception:
            pass

    def _apply(fn, val: int) -> None:
        last_exc: Optional[Exception] = None
        for attempt in range(1, SERVO_RETRIES + 1):
            try:
                fn(val); return
            except OSError as e:
                last_exc = e
                time.sleep(0.015 * attempt)
                _safe_enable(False); time.sleep(0.010); _safe_enable(True)
            except Exception as e:
                last_exc = e; break
        raise RuntimeError(f"Servo write failed ({val}): {last_exc}")

    def _rate_limit(last_ts: float) -> float:
        now = time.monotonic()
        dt = now - last_ts
        if dt < SERVO_MIN_INTERVAL_S:
            time.sleep(SERVO_MIN_INTERVAL_S - dt)
            now = time.monotonic()
        return now

    def _write_axes(pv: int, tv: int) -> float:
        nonlocal last_apply
        pv_hw = max(-90 + SOFT_LIMIT_MARGIN_DEG, min(90 - SOFT_LIMIT_MARGIN_DEG,
                  int(round(pv + (PAN_HW_OFFSET_DEG  * p_sign)))))
        tv_hw = max(-90 + SOFT_LIMIT_MARGIN_DEG, min(90 - SOFT_LIMIT_MARGIN_DEG,
                  int(round(tv + (TILT_HW_OFFSET_DEG * t_sign)))))
        last_apply = _rate_limit(last_apply); _apply(pantilthat.pan,  pv_hw)
        last_apply = _rate_limit(last_apply); _apply(pantilthat.tilt, tv_hw)
        return last_apply

    def _ensure_enabled():
        nonlocal servo_enabled
        if not servo_enabled:
            if not _safe_enable(True):
                raise RuntimeError("servo enable/ping failed")
            servo_enabled = True

    def _slew_to(tp: Optional[int], tt: Optional[int]) -> None:
        nonlocal pan, tilt, last_cmd_ts
        tgt_p = pan  if tp is None else int(tp)
        tgt_t = tilt if tt is None else int(tt)
        dp = tgt_p - pan
        dt = tgt_t - tilt

        def steps_for(delta: int, rate_deg_s: float) -> int:
            ad = abs(float(delta))
            if ad == 0.0: return 0
            step_deg = max(SLEW_MIN_STEP_DEG, min(SLEW_MAX_STEP_DEG, rate_deg_s * SERVO_MIN_INTERVAL_S))
            return max(1, int(math.ceil(ad / step_deg)))

        n = max(steps_for(dp, float(PAN_SLEW_DEG_S)), steps_for(dt, float(TILT_SLEW_DEG_S)))
        if n == 0:
            _write_axes(pan, tilt)
            last_cmd_ts = time.monotonic()
            return
        for i in range(1, n + 1):
            ip = pan + int(round(dp * i / n))
            it = tilt + int(round(dt * i / n))
            _write_axes(ip, it)
        pan, tilt = tgt_p, tgt_t
        last_cmd_ts = time.monotonic()

    # Initial state
    pan, tilt = _clamp(PAN_HOME_DEG, PLO, PHI), _clamp(TILT_HOME_DEG, TLO, THI)
    last_apply = 0.0
    last_hb = 0.0
    last_cmd_ts = time.monotonic()
    servo_enabled = False
    if _safe_enable(True):
        servo_enabled = True
        try:
            last_apply = _write_axes(pan, tilt)
        except Exception:
            pass

    def _send_state(rid: str, ok: bool, err: Optional[str] = None):
        try:
            resp_q.put(_Resp(rid=rid, ok=ok, state={"pan": pan, "tilt": tilt}, error=err))
        except Exception:
            pass

        _ka_fail = 0
    while not stop_evt.is_set():
        now = time.monotonic()
        if now - last_hb >= HEARTBEAT_PERIOD_S:
            last_hb = now
            try: resp_q.put(_Resp(rid="hb", ok=True, kind="heartbeat"))
            except Exception: pass

        if servo_enabled and (now - last_apply) >= KEEPALIVE_PERIOD_S:
            try:
                last_apply = _write_axes(pan, tilt)
                # Verify bus still responsive; treat NACK as a keepalive failure
                if not _i2c_ping():
                    raise RuntimeError("i2c ping failed after keepalive write")
                _ka_fail = 0
            except Exception as _e:
                _ka_fail += 1
                try:
                    log.warning("Keepalive write failed (%d): %s", _ka_fail, _e)
                except Exception:
                    pass
                if _ka_fail >= 2:
                    # Exit child so watchdog can perform bus recovery and restart cleanly
                    try: resp_q.put(_Resp(rid="hb", ok=False, kind="heartbeat", error="keepalive_failed"))
                    except Exception: pass
                    break

        if servo_enabled and SERVO_IDLE_RELEASE_S > 0.0 and (now - last_cmd_ts) >= SERVO_IDLE_RELEASE_S:
            _safe_enable(False); servo_enabled = False

        try:
            cmd: _Cmd = cmd_q.get(timeout=0.25)
        except Exception:
            continue

        base = cmd
        abs_p, abs_t = base.pan, base.tilt
        dp, dt = float(base.dp or 0), float(base.dt or 0)
        while True:
            try:
                nxt: _Cmd = cmd_q.get_nowait()
            except Exception:
                break
            if nxt.op in ("move", "set"):
                abs_p = nxt.pan if nxt.pan is not None else abs_p
                abs_t = nxt.tilt if nxt.tilt is not None else abs_t
                dp += float(nxt.dp or 0); dt += float(nxt.dt or 0)
            base = nxt

        try:
            if base.op == "shutdown":
                _send_state(base.rid, True); break
            elif base.op == "release":
                _safe_enable(False); servo_enabled = False; _send_state(base.rid, True); continue
            elif base.op == "home":
                _ensure_enabled()
                pan, tilt = _clamp(PAN_HOME_DEG, PLO, PHI), _clamp(TILT_HOME_DEG, TLO, THI)
            elif base.op == "set":
                _ensure_enabled()
                if abs_p is not None: pan  = _clamp(abs_p, PLO, PHI)
                if abs_t is not None: tilt = _clamp(abs_t, TLO, THI)
            elif base.op == "move":
                _ensure_enabled()
                pan  = _clamp(pan  + ( -1 if PAN_INVERT  else 1) * dp, PLO, PHI)
                tilt = _clamp(tilt + ( -1 if TILT_INVERT else 1) * dt, TLO, THI)
            elif base.op == "state":
                _send_state(base.rid, True); continue
            else:
                _send_state(base.rid, False, f"Unknown op {base.op}"); continue

            _slew_to(pan, tilt)
            if SERVO_RELEASE_AFTER_ACTION:
                time.sleep(max(0.0, SERVO_RELEASE_SETTLE_S))
                _safe_enable(False); servo_enabled = False
            _send_state(base.rid, True)
        except Exception as e:
            _send_state(base.rid, False, str(e))

    _safe_enable(False)


# -----------------------------------------------------------------------------
# I2C bus recovery utilities (general-call reset, SCL pulse, driver reload)
# -----------------------------------------------------------------------------
def _bus_recover() -> None:
    """Attempt to recover a wedged I2C bus or slave without power-cycling.
    Safe to call even if the bus isn't wedged.
    """
    import time as _time
    import subprocess as _subprocess
    # 1) I2C General Call software reset: addr 0x00, data 0x06 (if any slave supports it)
    try:
        try:
            from smbus2 import SMBus, i2c_msg  # type: ignore
        except Exception:
            SMBus = None  # type: ignore
        if SMBus is not None:
            with SMBus(1) as _bus:
                _bus.i2c_rdwr(i2c_msg.write(0x00, [0x06]))
            _time.sleep(0.05)
    except Exception:
        pass

    # 2) Pulse SCL to free a slave holding SDA low (9–12 clocks). Use raspi-gpio.
    try:
        # Set SCL/SDA to input (no pulls), then drive SCL as output to pulse, then restore ALT0.
        for _pin in ("3","2"):  # SCL=3, SDA=2 (BCM numbering)
            _subprocess.run(["raspi-gpio","set",_pin,"pn"], check=False, stdout=_subprocess.DEVNULL, stderr=_subprocess.DEVNULL)
        _subprocess.run(["raspi-gpio","set","3","op"], check=False, stdout=_subprocess.DEVNULL, stderr=_subprocess.DEVNULL)
        for _ in range(12):
            _subprocess.run(["raspi-gpio","set","3","dl"], check=False, stdout=_subprocess.DEVNULL, stderr=_subprocess.DEVNULL)
            _time.sleep(0.002)
            _subprocess.run(["raspi-gpio","set","3","dh"], check=False, stdout=_subprocess.DEVNULL, stderr=_subprocess.DEVNULL)
            _time.sleep(0.002)
    finally:
        for _pin in ("3","2"):
            _subprocess.run(["raspi-gpio","set",_pin,"a0"], check=False, stdout=_subprocess.DEVNULL, stderr=_subprocess.DEVNULL)

    # 3) Reload the I2C controller driver as a last resort
    try:
        _subprocess.run(["sudo","modprobe","-r","i2c_bcm2835"], check=False, stdout=_subprocess.DEVNULL, stderr=_subprocess.DEVNULL)
        _subprocess.run(["sudo","modprobe","i2c_bcm2835"], check=False, stdout=_subprocess.DEVNULL, stderr=_subprocess.DEVNULL)
        _time.sleep(0.1)
    except Exception:
        pass
class ServoManager:
    def __init__(self) -> None:
        self._cmd_q: MPQueue = MPQueue(maxsize=256)
        self._resp_q: MPQueue = MPQueue(maxsize=256)
        self._stop_evt: MPEvent = MPEvent()
        self._proc: Optional[Process] = None
        self._pending: Dict[str, threading.Event] = {}
        self._results: Dict[str, _Resp] = {}
        self._lock = threading.Lock()
        self._last_hb = time.monotonic()
        self._rx_th = threading.Thread(target=self._rx_loop, name="servo-rx", daemon=True)
        self._wd_th = threading.Thread(target=self._wd_loop, name="servo-wd", daemon=True)
        self._restart_count = 0
        self._rst_lock = threading.Lock()

    def start(self) -> None:
        if self._proc and self._proc.is_alive():
            return
        self._stop_evt.clear()
        self._proc = Process(target=_servo_child, args=(self._cmd_q, self._resp_q, self._stop_evt), daemon=True)
        self._proc.start()
        if not self._rx_th.is_alive(): self._rx_th.start()
        if not self._wd_th.is_alive(): self._wd_th.start()

    def stop(self) -> None:
        try: self._send(_Cmd(rid=str(uuid.uuid4()), op="shutdown"), wait=False)
        except Exception: pass
        self._stop_evt.set()
        if self._proc:
            self._proc.join(timeout=1.5)
            if self._proc.is_alive():
                try: self._proc.terminate()
                except Exception: pass

    def _rx_loop(self) -> None:
        while True:
            try:
                resp: _Resp = self._resp_q.get(timeout=0.5)
            except Exception:
                continue
            if resp.kind == "heartbeat":
                if resp.ok:
                    self._last_hb = time.monotonic()
                else:
                    # Heartbeat reported a fault; restart child
                    try:
                        self._restart_now("heartbeat_fault")
                    except Exception:
                        pass
                continue
            with self._lock:
                self._results[resp.rid] = resp
                ev = self._pending.get(resp.rid)
            if ev: ev.set()

    def _wd_loop(self) -> None:
        while True:
            time.sleep(0.5)
            if not self._proc: continue
            dead = not self._proc.is_alive()
            stale = (time.monotonic() - self._last_hb) > HEARTBEAT_GRACE_S
            if dead or stale:
                log.warning("Servo process %s; attempting I2C recovery...", "died" if dead else "stalled")
                try:
                    if self._proc.is_alive():
                        self._proc.terminate(); self._proc.join(timeout=1.5)
                except Exception:
                    pass
                # If it still hasn't exited, perform bus recovery before deciding to restart
                try:
                    _bus_recover()
                except Exception:
                    pass
                if self._proc and self._proc.is_alive():
                    log.error("Old servo process did not exit; skipping restart to avoid bus contention.")
                    continue
                with self._lock:
                    for rid, ev in self._pending.items():
                        self._results[rid] = _Resp(rid=rid, ok=False, error="servo restart"); ev.set()
                    self._pending.clear()
                self._restart_count += 1
                self.start(); self._last_hb = time.monotonic()

    def _restart_now(self, reason: str) -> None:
        """Synchronously terminate and restart the servo child process."""
        # Prevent overlapping restarts from watchdog / rx / callers
        if not self._rst_lock.acquire(blocking=False):
            return
        try:
            log.warning("Restarting servo process (%s)...", reason)
            try:
                if self._proc and self._proc.is_alive():
                    try:
                        self._proc.terminate()
                        self._proc.join(timeout=1.5)
                    except Exception:
                        pass
                    if self._proc.is_alive():
                        try:
                            self._proc.kill()
                            self._proc.join(timeout=1.0)
                        except Exception:
                            pass
            except Exception:
                pass
            try:
                _bus_recover()
            except Exception:
                pass
            with self._lock:
                # Fail all pending waiters
                for rid, ev in self._pending.items():
                    self._results[rid] = _Resp(rid=rid, ok=False, error=f"servo restart: {reason}")
                    ev.set()
                self._pending.clear()
            self._restart_count += 1
            # Drop the old handle to make start() spawn a fresh child.
            self._proc = None
            self.start()
            self._last_hb = time.monotonic()
        finally:
            self._rst_lock.release()

    def _send(self, cmd: _Cmd, wait: bool) -> Optional[_Resp]:
        try:
            self._cmd_q.put(cmd, timeout=0.25)
        except Exception:
            from flask import abort
            abort(503, description="Servo command queue overloaded.")
        if not wait: return None
        ev = threading.Event()
        with self._lock: self._pending[cmd.rid] = ev
        wait_s = float(SERVO_SEND_TIMEOUT_S) + (float(SERVO_RELEASE_SETTLE_S) if SERVO_RELEASE_AFTER_ACTION else 0.0)
        if not ev.wait(timeout=wait_s):
            try:
                self._restart_now("response_timeout")
            except Exception:
                pass
            with self._lock:
                self._pending.pop(cmd.rid, None)
                self._results[cmd.rid] = _Resp(rid=cmd.rid, ok=False, error="servo response timeout")
            return self._results.pop(cmd.rid, None)
        with self._lock:
            resp = self._results.pop(cmd.rid, None); self._pending.pop(cmd.rid, None)
        # If command failed, assume servo stack is unhealthy and restart proactively.
        if resp is not None and not getattr(resp, 'ok', True):
            try:
                self._restart_now("command_error")
            except Exception:
                pass
        return resp

    # RPCs
    def move(self, dp: float, dt: float):
        rid = str(uuid.uuid4()); return self._send(_Cmd(rid=rid, op="move", dp=float(dp), dt=float(dt)), wait=True)
    def set(self, pan: Optional[float], tilt: Optional[float]):
        rid = str(uuid.uuid4()); return self._send(_Cmd(rid=rid, op="set", pan=pan, tilt=tilt), wait=True)
    def home(self):
        rid = str(uuid.uuid4()); return self._send(_Cmd(rid=rid, op="home"), wait=True)
    def state(self):
        rid = str(uuid.uuid4()); return self._send(_Cmd(rid=rid, op="state"), wait=True)
    def release(self):
        rid = str(uuid.uuid4()); return self._send(_Cmd(rid=rid, op="release"), wait=True)

_servo = ServoManager(); _servo.start()

# -----------------------------------------------------------------------------
# Video pipeline & camera management (lower latency + watchdog)
# -----------------------------------------------------------------------------
class _StreamingOutput(io.BufferedIOBase):
    def __init__(self) -> None:
        self.frame: Optional[bytes] = None
        self.condition = threading.Condition()
        self._last_frame_ts = 0.0
    def write(self, buf: bytes) -> int:
        if not isinstance(buf, (bytes, bytearray)): return 0
        with self.condition:
            self.frame = bytes(buf); self._last_frame_ts = time.monotonic(); self.condition.notify_all()
        return len(buf)
    def last_frame_age(self) -> Optional[float]:
        ts = self._last_frame_ts
        return None if ts == 0.0 else max(0.0, time.monotonic() - ts)

_picam2: Optional[Picamera2] = None

# Synchronization for camera reconfiguration and snapshots
_cam_lock = threading.RLock()
_snap_lock = threading.Lock()
SNAPSHOT_DIR: str = os.path.expanduser("~/snapshots")

_output = _StreamingOutput()

def _jpeg_encoder(q: int) -> JpegEncoder:
    q = max(1, min(100, int(q)))
    try: return JpegEncoder(quality=int(q))
    except TypeError: return JpegEncoder(q=int(q))

def _transform() -> Transform:
    rot = int(STREAM_ROTATION) % 360
    if rot not in (0, 90, 180, 270):
        raise ValueError("STREAM_ROTATION must be 0/90/180/270")
    return Transform(rotation=rot, hflip=bool(STREAM_HFLIP), vflip=bool(STREAM_VFLIP))

def _cma_free_bytes() -> int:
    try:
        with open("/proc/meminfo", "r", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith("CmaFree:"):
                    return int(line.split()[1]) * 1024
    except Exception:
        pass
    return 0

def _candidate_sizes() -> List[Tuple[int, int]]:
    # Prefer lower latency sizes first; if CMA is very tight, keep 640x480.
    cma = _cma_free_bytes()
    if cma and cma < 80 * 1024 * 1024:
        return [(640, 480), (960, 540)]
    if cma and cma < 140 * 1024 * 1024:
        return [(640, 480), (960, 540), (1280, 720)]
    return FALLBACK_SIZES[:]

def _configure_camera() -> None:
    """Lower-latency profile: small frame first, 2 buffers, target ~30fps."""
    global _picam2
    if _picam2 is None:
        _picam2 = Picamera2()
    last_err: Optional[Exception] = None
    tried: List[str] = []
    for (w, h) in _candidate_sizes():
        tried.append(f"{w}x{h}")
        try:
            cfg = _picam2.create_video_configuration(
                main={"size": (int(w), int(h)), "format": "YUV420"},
                transform=_transform(),
                buffer_count=int(STREAM_BUFFER_COUNT)
            )
            _picam2.configure(cfg)
            # Keep encoder simple & fast; limit exposure time for stable fps.
            try:
                frame_us_min = int(1_000_000 / max(1, TARGET_FPS_MAX))
                frame_us_max = int(1_000_000 / max(1, TARGET_FPS_MIN))
                _picam2.set_controls({"FrameDurationLimits": (frame_us_min, frame_us_max)})
            except Exception:
                pass
            enc = _jpeg_encoder(JPEG_QUALITY)
            _picam2.start_recording(enc, FileOutput(_output))
            log.info("Camera started @ %dx%d (buffers=%d)", w, h, int(STREAM_BUFFER_COUNT))
            return
        except OSError as e:
            last_err = e
            if getattr(e, "errno", None) == 12:
                log.warning("DMA OOM at %dx%d: %s; trying next.", w, h, e)
                time.sleep(0.05); continue
            log.warning("OSError at %dx%d: %s; trying next.", w, h, e)
        except Exception as e:
            last_err = e
            log.warning("Configure failed at %dx%d: %s; trying next.", w, h, e)
    raise RuntimeError(f"Camera could not start. Tried {tried}. Last error: {last_err}")

def _restart_camera() -> None:
    try:
        if _picam2:
            try: _picam2.stop_recording()
            except Exception: pass
            try: _picam2.close()
            except Exception: pass
    except Exception:
        pass
    globals()["_picam2"] = None
    _configure_camera()


def _ensure_dir(path: str) -> None:
    try:
        os.makedirs(path, exist_ok=True)
    except Exception as e:
        raise RuntimeError(f"Failed to ensure directory {path}: {e}")

def _capture_fullres_jpeg(save_path: Optional[str] = None) -> bytes:
    """Capture a full-resolution JPEG, temporarily pausing the stream if needed.
    Returns the JPEG bytes. If save_path is provided, saves to that path.
    """
    global _picam2
    with _cam_lock:
        # Ensure snapshots dir exists
        if save_path:
            _ensure_dir(os.path.dirname(save_path))
        # Gracefully stop current recording if running
        try:
            if _picam2 is not None:
                try:
                    _picam2.stop_recording()
                except Exception:
                    pass
        except Exception:
            pass
        # Initialize camera if needed
        if _picam2 is None:
            _picam2 = Picamera2()
        # Create a still configuration at maximum sensor resolution
        try:
            still_cfg = _picam2.create_still_configuration(transform=_transform())
        except Exception as e:
            raise RuntimeError(f"Failed to create still configuration: {e}")
        # Capture to memory (and optionally to disk)
        buf = io.BytesIO()
        try:
            # Prefer a single-shot mode switch that handles start/stop internally
            try:
                _picam2.switch_mode_and_capture_file(still_cfg, buf, format='jpeg')
            except Exception:
                # Fallback: configure, start, capture, stop
                _picam2.configure(still_cfg)
                _picam2.start()
                try:
                    _picam2.capture_file(buf, format='jpeg')
                finally:
                    try:
                        _picam2.stop()
                    except Exception:
                        pass
        except Exception as e:
            raise RuntimeError(f"Snapshot capture failed: {e}")
        data = buf.getvalue()
        if not data:
            raise RuntimeError("Snapshot capture produced no data")
        # Persist to disk if requested
        if save_path:
            try:
                with open(save_path, 'wb') as fh:
                    fh.write(data)
            except Exception as e:
                raise RuntimeError(f"Failed to save snapshot to {save_path}: {e}")
        # Resume video stream at low-latency configuration
        try:
            _restart_camera()
        except Exception:
            # Allow watchdog to restart if this fails
            pass
        return data

# LED detect
def _detect_led() -> Optional[Tuple[str, str]]:
    base = "/sys/class/leds"
    if not os.path.isdir(base): return None
    for path in glob.glob(os.path.join(base, "*")):
        name = os.path.basename(path).lower()
        if any(k in name for k in ("cam", "camera", "flash", "torch", "illuminator")):
            b = os.path.join(path, "brightness")
            m = os.path.join(path, "max_brightness")
            if os.path.isfile(b) and os.path.isfile(m) and os.access(b, os.W_OK):
                return (b, m)
    return None

_LED_PATHS = _detect_led()

def _led_read() -> Optional[bool]:
    if not _LED_PATHS: return None
    b, _ = _LED_PATHS
    try:
        with open(b, "r", encoding="utf-8") as fh:
            return int((fh.read().strip() or "0")) > 0
    except Exception:
        return None

def _led_write(on: bool) -> bool:
    if not _LED_PATHS: return False
    b, m = _LED_PATHS
    try:
        with open(m, "r", encoding="utf-8") as fh:
            maxv = int((fh.read().strip() or "1"))
        with open(b, "w", encoding="utf-8") as fh:
            fh.write(str(maxv if on else 0))
        return True
    except Exception:
        return False

# -----------------------------------------------------------------------------
# UI (embed-optimized, toolbar on top; D-pad auto-hides on non-mobile)
# -----------------------------------------------------------------------------
_INDEX = f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Pi Camera + PanTilt</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root {{ color-scheme: dark; }}
  body {{ font-family: system-ui, Verdana, sans-serif; margin:0; background:#111; color:#eee; }}
  header {{
    position: sticky; top:0; z-index:10;
    padding:.45rem .75rem; background:#1b1b1b; display:flex; gap:.5rem; align-items:center; flex-wrap:wrap;
    border-bottom:1px solid #2a2a2a;
  }}
  header .spacer {{ flex:1 1 auto; }}
  header .toolbar {{
    display:flex; gap:.4rem; align-items:center; flex-wrap:wrap;
  }}
  header .toolbar button, header .toolbar input[type=number] {{
    font-size:1rem; padding:.35rem .6rem; border-radius:8px; border:1px solid #333; background:#222; color:#eee;
  }}
  main {{ display:flex; flex-direction:column; align-items:center; gap:.6rem; padding:.6rem; }}
  img {{ max-width:100%; height:auto; border:1px solid #333; border-radius:6px; }}
  .hidden {{ display:none !important; }}
  /* D-pad shows on coarse pointers (touch). Hidden on desktops by default. */
  .dpad {{
    display:grid; grid-template-columns: minmax(52px,1fr) minmax(52px,1fr) minmax(52px,1fr);
    grid-template-rows: minmax(52px,1fr) minmax(52px,1fr) minmax(52px,1fr);
    gap:.5rem; width:100%; max-width:640px; margin-top:.25rem;
  }}
  .dpad > button {{
    font-size: clamp(1.0rem, 3.2vw, 1.4rem);
    padding:.75rem .85rem; min-height:52px; min-width:52px;
    border-radius:12px; border:1px solid #333; background:#222; color:#eee; touch-action: manipulation;
  }}
  .dpad > button:active {{ background:#2a2a2a; }}
  .btn-up    {{ grid-area: 1 / 2; }}
  .btn-left  {{ grid-area: 2 / 1; }}
  .btn-home  {{ grid-area: 2 / 2; font-weight:700; }}
  .btn-right {{ grid-area: 2 / 3; }}
  .btn-down  {{ grid-area: 3 / 2; }}

  @media (pointer: fine) {{
    .dpad {{ display:none; }}
  }}
  /* Embed mode trims padding & borders */
  .compact header {{ padding:.35rem .6rem; }}
  .compact main {{ padding:.3rem; gap:.35rem; }}
  .compact img {{ border:none; border-radius:0; }}
</style>
</head>
<body>
<header>
  <strong>OBSTENET</strong>
  <span class="spacer"></span>
  <div class="toolbar">
    <label for="step">Step (°)</label>
    <input id="step" type="number" min="1" max="30" value="{DEFAULT_STEP_DEG}" title="Degrees per nudge">
    <button id="center" title="Center (home)">Center</button>
    <button id="snap" title="Open snapshot in new tab">Snapshot</button>
    <button id="release" title="Power off servos (freewheel)">Release</button>
    <button id="led" class="hidden" aria-pressed="false" title="Toggle camera LED">LED: Off</button>
  </div>
</header>
<main>
  <img id="stream" src="/stream.mjpg" alt="camera stream" referrerpolicy="no-referrer" loading="eager" decoding="async">
  <div class="dpad" aria-label="directional pad">
    <button id="up"    class="btn-up"    aria-label="up">↑</button>
    <button id="left"  class="btn-left"  aria-label="left">←</button>
    <button id="home"  class="btn-home"  aria-label="center">Center</button>
    <button id="right" class="btn-right" aria-label="right">→</button>
    <button id="down"  class="btn-down"  aria-label="down">↓</button>
  </div>
</main>
<script>
(function() {{
  const qs = new URLSearchParams(location.search);
  if (qs.get('embed') === '1') document.documentElement.classList.add('compact');

  const clamp = (v,min,max)=>Math.max(min,Math.min(max,v));
  function getStep() {{
    const n = parseInt(document.getElementById('step').value, 10);
    return clamp(isNaN(n)?{DEFAULT_STEP_DEG}:n, 1, 30);
  }}
  async function postJSON(url, body) {{
    const r = await fetch(url, {{method:'POST', headers:{{'Content-Type':'application/json'}}, body:JSON.stringify(body||{{}})}});
    const t = await r.text();
    if (!r.ok) throw new Error(t||r.statusText);
    try {{ return t ? JSON.parse(t) : {{}}; }} catch(_) {{ return {{}}; }}
  }}
  // LED capability discovery
  (async () => {{
    try {{
      const capR = await fetch('/api/capabilities', {{cache:'no-store'}});
      if (!capR.ok) return;
      const cap = await capR.json();
      if (cap.led && cap.led.present && cap.led.writable) {{
        const b = document.getElementById('led'); b.classList.remove('hidden');
        b.textContent = 'LED: ' + (cap.led.on ? 'On':'Off'); b.setAttribute('aria-pressed', cap.led.on?'true':'false');
        b.onclick = async () => {{
          try {{
            const next = b.getAttribute('aria-pressed')!=='true';
            await postJSON('/api/led', {{ on: next }});
            b.textContent = 'LED: ' + (next?'On':'Off'); b.setAttribute('aria-pressed', next?'true':'false');
          }} catch (e) {{ alert('LED error: '+e.message); }}
        }};
      }}
    }} catch {{ /* non-fatal */ }}
  }})();

  // Toolbar actions
  document.getElementById('center').onclick  = () => postJSON('/api/home', {{}}).catch(()=>{{}});
  document.getElementById('snap').onclick    = () => window.open('/snapshot.jpg', '_blank', 'noopener,noreferrer') || alert('Allow pop-ups');
  document.getElementById('release').onclick = () => postJSON('/api/servo/release', {{}}).catch(()=>{{}});

  // D-pad actions
  const step = () => getStep();
  document.getElementById('up').onclick    = () => postJSON('/api/move', {{dp:0, dt:-step()}}).catch(()=>{{}});
  document.getElementById('down').onclick  = () => postJSON('/api/move', {{dp:0, dt: step()}}).catch(()=>{{}});
  document.getElementById('left').onclick  = () => postJSON('/api/move', {{dp:-step(), dt:0}}).catch(()=>{{}});
  document.getElementById('right').onclick = () => postJSON('/api/move', {{dp: step(), dt:0}}).catch(()=>{{}});
  document.getElementById('home').onclick  = () => postJSON('/api/home', {{}}).catch(()=>{{}});

  // Keyboard shortcuts (desktop-only to avoid stealing focus in embeds)
  if (!window.matchMedia('(pointer: coarse)').matches && qs.get('embed') !== '1') {{
    window.addEventListener('keydown', (ev) => {{
      const s = step(), k = ev.key.toLowerCase();
      if (k==='arrowup')    postJSON('/api/move', {{dp:0, dt:-s}}).catch(()=>{{}});
      if (k==='arrowdown')  postJSON('/api/move', {{dp:0, dt: s}}).catch(()=>{{}});
      if (k==='arrowleft')  postJSON('/api/move', {{dp:-s, dt:0}}).catch(()=>{{}});
      if (k==='arrowright') postJSON('/api/move', {{dp: s, dt:0}}).catch(()=>{{}});
      if (k==='c') postJSON('/api/home', {{}}).catch(()=>{{}});
      if (k==='r') postJSON('/api/servo/release', {{}}).catch(()=>{{}});
      if (k==='s') window.open('/snapshot.jpg?ts=' + Date.now(),'_blank','noopener,noreferrer');
    }});
  }}
}})();
</script>
</body>
</html>
"""

# -----------------------------------------------------------------------------
# Net helpers
# -----------------------------------------------------------------------------
def _iface_operstate(ifname: str) -> Optional[str]:
    try:
        with open(f"/sys/class/net/{ifname}/operstate", "r", encoding="utf-8") as fh:
            return fh.read().strip().lower()
    except Exception:
        return None

def _iface_ipv4(ifname: str) -> Optional[str]:
    try:
        import fcntl  # only on Unix
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(0.5)
        ifreq = struct.pack("256s", ifname[:15].encode("utf-8"))
        res = fcntl.ioctl(s.fileno(), 0x8915, ifreq)  # SIOCGIFADDR
        ip = socket.inet_ntoa(res[20:24])
        return ip
    except Exception:
        return None

def _tailscale_ipv4(wait_s: float = 0.0) -> Optional[str]:
    deadline = time.monotonic() + max(0.0, wait_s)
    while True:
        st = _iface_operstate("tailscale0")
        if st == "up":
            ip = _iface_ipv4("tailscale0")
            if ip: return ip
        if time.monotonic() >= deadline:
            return None
        time.sleep(0.25)

def _pick_host() -> str:
    mode = os.environ.get("BIND_MODE", "all").strip().lower()
    if mode == "tailscale":
        wait_s = float(os.environ.get("TAILSCALE_BIND_WAIT_S", "5.0"))
        ip = _tailscale_ipv4(wait_s=wait_s)
        if ip:
            log.info("Binding to Tailscale IPv4 %s (tailscale0).", ip)
            return ip
        log.warning("tailscale0 not ready; falling back to loopback.")
        return "127.0.0.1"
    if mode == "loopback":
        return "127.0.0.1"
    if mode == "custom":
        h = os.environ.get("BIND_HOST", "").strip()
        if h:
            log.info("Binding to custom host '%s'.", h)
            return h
        log.warning("BIND_MODE=custom but BIND_HOST missing; using default HOST.")
        return HOST
    return HOST

# -----------------------------------------------------------------------------
# Flask: headers, JSON errors, request shaping
# -----------------------------------------------------------------------------
_allowed_origins = [o.strip() for o in os.environ.get("CORS_ALLOW_ORIGINS", "").split(",") if o.strip()]
_frame_ancestors = os.environ.get("FRAME_ANCESTORS", "").strip()
_client_last_move_ts: Dict[str, float] = defaultdict(lambda: 0.0)

@app.before_request
def _shape_requests():
    if request.path == "/api/move":
        now = time.monotonic()
        key = request.remote_addr or "0.0.0.0"
        last = _client_last_move_ts[key]
        if now - last < MOVE_MIN_INTERVAL_S:
            return make_response(jsonify({"status": "dropped"}), 202)
        _client_last_move_ts[key] = now

@app.after_request
def _headers(resp):
    # CORS
    if _allowed_origins:
        origin = request.headers.get("Origin", "")
        if "*" in _allowed_origins:
            resp.headers["Access-Control-Allow-Origin"] = "*"
        elif origin in _allowed_origins:
            resp.headers["Access-Control-Allow-Origin"] = origin
            resp.headers["Vary"] = "Origin"
        resp.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"] = "Content-Type"

    # CSP (allow embedding via FRAME_ANCESTORS if provided)
    csp = [
        "default-src 'self'",
        "img-src 'self' data:",
        "style-src 'self' 'unsafe-inline'",
        "script-src 'self' 'unsafe-inline'",
    ]
    if _frame_ancestors:
        csp.append(f"frame-ancestors {_frame_ancestors}")
    resp.headers["Content-Security-Policy"] = "; ".join(csp)

    if request.path.startswith("/api/") or request.path.endswith(".mjpg"):
        resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp

@app.errorhandler(400)
@app.errorhandler(404)
@app.errorhandler(503)
@app.errorhandler(500)
def _json_errors(err):
    code = getattr(err, "code", 500)
    desc = getattr(err, "description", "internal error")
    return jsonify({"error": str(desc), "status": code, "path": request.path}), code

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.get("/")
def index():
    return Response(_INDEX, mimetype="text/html; charset=utf-8")

@app.get("/stream.mjpg")
def stream_mjpg():
    def generate():
        try:
            while True:
                with _output.condition:
                    _output.condition.wait()
                    frame = _output.frame
                if not frame:
                    continue
                yield (b"--FRAME\r\n"
                       b"Content-Type: image/jpeg\r\n"
                       b"Content-Length: " + str(len(frame)).encode() + b"\r\n\r\n" +
                       frame + b"\r\n")
        except GeneratorExit:
            log.info("Client disconnected from MJPEG stream.")
        except Exception as e:
            log.warning("MJPEG stream error: %s", e)
    resp = Response(generate(), mimetype="multipart/x-mixed-replace; boundary=FRAME")
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp

@app.get("/snapshot.jpg")
def snapshot():
    """Capture a full-resolution snapshot, return it, and save to ~/snapshots."""
    ts = time.strftime("%Y%m%d-%H%M%S")
    fname = f"snapshot_{ts}.jpg"
    dest_dir = SNAPSHOT_DIR
    # Validate and expand home dir to avoid surprises
    dest_dir = os.path.expanduser(dest_dir)
    save_path = os.path.join(dest_dir, fname)
    with _snap_lock:
        try:
            data = _capture_fullres_jpeg(save_path)
        except Exception as e:
            abort(503, description=f"Snapshot failed: {e}")
    resp = Response(data, mimetype="image/jpeg")
    resp.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    return resp

# -----------------------------------------------------------------------------
# motionEye compatibility (remote motionEye camera expects these endpoints)
# -----------------------------------------------------------------------------
def _motioneye_camera_descriptor() -> dict:
    base = request.url_root.rstrip("/")
    stream_url = f"{base}/movie/1/stream/"
    snapshot_url = f"{base}/picture/1/current/"
    return {
        "id": 1,
        "name": "obstenet",
        "proto": request.scheme,
        "hostname": request.host.split(":")[0],
        "port": int(request.host.split(":", 1)[1]) if ":" in request.host else 80,
        "base_url": base,
        "stream_url": stream_url,
        "snapshot_url": snapshot_url,
    }


@app.get("/config/list")
def motioneye_config_list():
    """Expose a minimal camera list so motionEye can treat us as a peer instance."""
    return jsonify({"cameras": [_motioneye_camera_descriptor()]})


@app.get("/picture/<int:cid>/current/")
def motioneye_picture_current(cid: int):
    """Alias to /snapshot.jpg for motionEye remote camera compatibility."""
    if cid != 1:
        abort(404, description="Unknown camera id")
    return snapshot()


@app.get("/movie/<int:cid>/stream/")
def motioneye_movie_stream(cid: int):
    """Alias to /stream.mjpg so motionEye can consume our live feed."""
    if cid != 1:
        abort(404, description="Unknown camera id")
    return stream_mjpg()

@app.get("/api/capabilities")
def api_capabilities():
    led_present = _LED_PATHS is not None
    led_state = _led_read() if led_present else None
    return jsonify({
        "led": {"present": bool(led_present), "writable": bool(led_present),
                "on": bool(led_state) if led_state is not None else False},
        "servo_release_after_action": bool(SERVO_RELEASE_AFTER_ACTION),
        "servo_idle_release_s": float(SERVO_IDLE_RELEASE_S),
        "slew_deg_s": {"pan": float(PAN_SLEW_DEG_S), "tilt": float(TILT_SLEW_DEG_S)},
        "soft_limit_margin_deg": int(SOFT_LIMIT_MARGIN_DEG)
    })

_camera_restart_times: deque[float] = deque(maxlen=128)
_cam_backoff_s: float = CAM_RESTART_BACKOFF_S
_cam_last_start_ts: float = 0.0

@app.get("/api/health")
def api_health():
    try:
        servo_ok = (time.monotonic() - _servo._last_hb) <= (HEARTBEAT_GRACE_S * 0.9)
    except Exception:
        servo_ok = False
    cam_age = _output.last_frame_age()
    cam_ok = cam_age is not None and cam_age < 5.0
    try:
        rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except Exception:
        rss_kb = -1
    restarts_last_hour = len([t for t in _camera_restart_times if (time.monotonic() - t) <= 3600.0])
    return jsonify({
        "servo": {"ok": bool(servo_ok), "restarts": _servo._restart_count},
        "camera": {"ok": bool(cam_ok), "last_frame_age_s": cam_age, "restarts_last_hour": restarts_last_hour, "backoff_s": _cam_backoff_s},
        "process": {"rss_kb": rss_kb}
    })

@app.get("/api/net")
def api_net():
    mode = os.environ.get("BIND_MODE", "all").strip().lower()
    ts_ip = _iface_ipv4("tailscale0")
    return jsonify({
        "bind_mode": mode,
        "tailscale0": {"ipv4": ts_ip, "operstate": _iface_operstate("tailscale0")},
        "port": PORT
    })

@app.get("/api/state")
def api_state():
    try:
        resp = _servo.state()
        if not resp or not resp.ok:
            abort(503, description=getattr(resp, "error", "servo unavailable"))
        return jsonify(resp.state or {"pan": 0, "tilt": 0})
    except Exception as e:
        abort(503, description=f"Servo state unavailable: {e}")

@app.post("/api/set")
def api_set():
    d = request.get_json(silent=True) or {}
    if "pan" not in d and "tilt" not in d:
        abort(400, description="Provide 'pan' and/or 'tilt' (degrees).")
    try:
        pan = float(d["pan"]) if "pan" in d else None
        tilt = float(d["tilt"]) if "tilt" in d else None
    except Exception:
        abort(400, description="'pan'/'tilt' must be numeric.")
    if pan is not None and not (PAN_RANGE_DEG[0] <= pan <= PAN_RANGE_DEG[1]):
        abort(400, description=f"'pan' must be in {PAN_RANGE_DEG}.")
    if tilt is not None and not (TILT_RANGE_DEG[0] <= tilt <= TILT_RANGE_DEG[1]):
        abort(400, description=f"'tilt' must be in {TILT_RANGE_DEG}.")
    resp = _servo.set(pan, tilt)
    if resp is None: return jsonify({"status": "queued"}), 202
    if not resp.ok: abort(503, description=resp.error or "servo error")
    return jsonify(resp.state or {})

@app.post("/api/move")
def api_move():
    d = request.get_json(silent=True) or {}
    try:
        dp = float(d.get("dp", 0)); dt = float(d.get("dt", 0))
    except Exception:
        abort(400, description="Invalid 'dp'/'dt'. Must be numeric.")
    if abs(dp) > 45 or abs(dt) > 45:
        abort(400, description="Single move too large; limit |dp|,|dt| <= 45.")
    hdp, hdt = _view_deltas_to_hw(dp, dt)
    resp = _servo.move(hdp, hdt)
    if resp is None: return jsonify({"status": "queued"}), 202
    if not resp.ok: abort(503, description=resp.error or "servo error")
    return jsonify(resp.state or {})

@app.post("/api/home")
def api_home():
    """Home, then sweep tilt to the farthest extreme from current tilt."""
    try:
        r1 = _servo.home()
        if r1 is None: return jsonify({"status": "queued"}), 202
        if not r1.ok:  abort(503, description=r1.error or "servo error during home")
        state = r1.state or {}
        cur_tilt = float(state.get("tilt", TILT_HOME_DEG))
        tlo, thi = float(TILT_RANGE_DEG[0]), float(TILT_RANGE_DEG[1])
        target_tilt = thi if abs(cur_tilt - tlo) < abs(thi - cur_tilt) else tlo
        r2 = _servo.set(None, target_tilt)
        if r2 is None: return jsonify({"status": "queued"}), 202
        if not r2.ok:  abort(503, description=r2.error or "servo error during tilt sweep")
        return jsonify(r2.state or {})
    except Exception as e:
        abort(503, description=f"Servo home/sweep failed: {e}")

@app.post("/api/servo/release")
def api_servo_release():
    """Explicitly power off both servos."""
    try:
        r = _servo.release()
        if r is None: return jsonify({"status": "queued"}), 202
        if not r.ok:  abort(503, description=r.error or "servo release failed")
        return jsonify({"released": True, "state": r.state or {}})
    except Exception as e:
        abort(503, description=f"Servo release failed: {e}")

@app.post("/api/gesture")
def api_gesture():
    """Perform 'nod' or 'shake' with bounded amplitude and cycles."""
    d = request.get_json(silent=True) or {}
    action = str(d.get("action", "")).strip().lower()
    if action not in ("nod", "shake"):
        abort(400, description="Provide 'action' as 'nod' or 'shake'.")

    try:
        resp = _servo.state()
        if not resp or not resp.ok:
            abort(503, description=getattr(resp, "error", "servo state unavailable"))
        st = resp.state or {}
        base_pan  = float(st.get("pan", PAN_HOME_DEG))
        base_tilt = float(st.get("tilt", TILT_HOME_DEG))
    except Exception as e:
        abort(503, description=f"Unable to read servo state: {e}")

    try:
        amp = int(d.get("amplitude_deg", GESTURE_AMPLITUDE_DEG))
        cyc = int(d.get("cycles", GESTURE_CYCLES))
        step_delay = float(d.get("step_delay_s", GESTURE_STEP_DELAY_S))
    except Exception:
        abort(400, description="Invalid amplitude/cycles/step_delay types.")

    amp = max(5, min(25, amp))
    cyc = max(1, min(3, cyc))
    step_delay = max(0.10, min(0.5, step_delay))

    try:
        if action == "nod":
            for _ in range(cyc):
                for tgt in (
                    _clamp(base_tilt + amp, *TILT_RANGE_DEG),
                    _clamp(base_tilt - amp, *TILT_RANGE_DEG),
                    _clamp(base_tilt + amp, *TILT_RANGE_DEG),
                ):
                    r = _servo.set(None, tgt)
                    if not r or not r.ok: abort(503, description=(r.error if r else "servo busy"))
                    time.sleep(step_delay)
            r = _servo.set(None, base_tilt)
            if not r or not r.ok: abort(503, description=(r.error if r else "servo busy"))
            return jsonify({"ok": True, "action": "nod"})
        else:
            for _ in range(cyc):
                for tgt in (
                    _clamp(base_pan - amp, *PAN_RANGE_DEG),
                    _clamp(base_pan + amp, *PAN_RANGE_DEG),
                    _clamp(base_pan - amp, *PAN_RANGE_DEG),
                ):
                    r = _servo.set(tgt, None)
                    if not r or not r.ok: abort(503, description=(r.error if r else "servo busy"))
                    time.sleep(step_delay)
            r = _servo.set(base_pan, None)
            if not r or not r.ok: abort(503, description=(r.error if r else "servo busy"))
            return jsonify({"ok": True, "action": "shake"})
    except Exception as e:
        abort(503, description=f"Gesture failed: {e}")

@app.post("/api/led")
def api_led():
    if not _LED_PATHS:
        abort(404, description="Camera LED not available on this system.")
    data = request.get_json(silent=True) or {}
    if "on" not in data or not isinstance(data["on"], (bool, int)):
        abort(400, description="Provide boolean 'on'.")
    ok = _led_write(bool(data["on"]))
    if not ok:
        abort(503, description="Failed to set LED state (permission or driver).")
    return jsonify({"on": bool(data["on"])})

# -----------------------------------------------------------------------------
# Lifecycle / Init / Watchdogs
# -----------------------------------------------------------------------------
def _cleanup() -> None:
    try: _servo.stop()
    except Exception: pass
    try:
        if _picam2:
            try: _picam2.stop_recording()
            except Exception: pass
            try: _picam2.close()
            except Exception: pass
    except Exception:
        pass

@atexit.register
def _atexit() -> None: _cleanup()

def _sigterm(_signo, _frame):
    _cleanup(); sys.exit(0)

def _sighup(_signo, _frame):
    log.info("SIGHUP received: restarting camera pipeline.")
    try:
        _restart_camera()
        global _cam_last_start_ts
        _cam_last_start_ts = time.monotonic()
    except Exception as e:
        log.error("Camera restart on SIGHUP failed: %s", e)

signal.signal(signal.SIGTERM, _sigterm)
signal.signal(signal.SIGINT, _sigterm)
signal.signal(signal.SIGHUP, _sighup)

def _safe_boot_camera_with_retry() -> None:
    global _cam_last_start_ts
    try:
        _configure_camera()
        _cam_last_start_ts = time.monotonic()
    except Exception as e:
        log.error("Initial camera start failed: %s (server will keep running; watchdog will retry)", e)
        _cam_last_start_ts = 0.0

def _camera_watchdog_loop():
    global _cam_backoff_s, _cam_last_start_ts
    while True:
        time.sleep(0.5)
        age = _output.last_frame_age()
        need_restart = False
        now = time.monotonic()
        if _picam2 is None:
            need_restart = True
        elif age is None or age > CAM_STALL_RESTART_S:
            need_restart = True
        if need_restart:
            recent = [t for t in _camera_restart_times if now - t <= 3600.0]
            if len(recent) >= CAM_RESTARTS_MAX_PER_HOUR:
                time.sleep(min(CAM_RESTART_BACKOFF_MAX_S, _cam_backoff_s * 2))
                continue
            if _cam_last_start_ts > 0 and (now - _cam_last_start_ts) < CAM_MIN_UPTIME_OK_S:
                time.sleep(min(CAM_RESTART_BACKOFF_MAX_S, _cam_backoff_s))
                _cam_backoff_s = min(CAM_RESTART_BACKOFF_MAX_S, max(_cam_backoff_s * 1.6, CAM_RESTART_BACKOFF_S))
            else:
                _cam_backoff_s = CAM_RESTART_BACKOFF_S
            try:
                log.warning("Camera %s; restarting...", "stalled" if _picam2 else "down")
                _restart_camera()
                _camera_restart_times.append(time.monotonic())
                _cam_last_start_ts = time.monotonic()
                log.info("Camera restart successful.")
            except Exception as e:
                log.error("Camera restart failed: %s", e)

def _start_all() -> None:
    _safe_boot_camera_with_retry()
    th = threading.Thread(target=_camera_watchdog_loop, name="camera-wd", daemon=True)
    th.start()

# Boot subsystems
_start_all()

# -----------------------------------------------------------------------------
# View delta mapping (unchanged)
# -----------------------------------------------------------------------------
def _view_deltas_to_hw(dp: float, dt: float) -> Tuple[float, float]:
    vx, vy = float(dp), float(dt)
    if STREAM_HFLIP: vx = -vx
    if STREAM_VFLIP: vy = -vy
    r = int(STREAM_ROTATION) % 360
    if r == 90:   vx, vy =  vy, -vx
    if r == 180:  vx, vy = -vx, -vy
    if r == 270:  vx, vy = -vy,  vx
    if INVERT_UI_LEFTRIGHT: vx = -vx
    if INVERT_UI_UPDOWN:    vy = -vy
    return vx, vy

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    if (STREAM_ROTATION % 360) not in (0, 90, 180, 270):
        sys.stderr.write("STREAM_ROTATION must be 0/90/180/270\n"); sys.exit(3)
    bind_host = _pick_host()
    try:
        app.run(host=bind_host, port=PORT, debug=False, threaded=True)
    except OSError as e:
        sys.stderr.write(f"ERROR: Failed to bind {bind_host}:{PORT} -> {e}\n")
        try:
            app.run(host="127.0.0.1", port=PORT, debug=False, threaded=True)
        except Exception as e2:
            sys.stderr.write(f"ERROR: Fallback bind failed: {e2}\n")
            sys.exit(4)
