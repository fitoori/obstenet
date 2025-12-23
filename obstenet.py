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

# --- Power management / brownout avoidance ---
POWER_MGMT_ENABLED: bool = True
POWER_SAMPLE_RATE_HZ: float = 10.0
POWER_WARN_VOLTS: float = 4.90
POWER_LIMIT_VOLTS: float = 4.75
POWER_CUTOFF_VOLTS: float = 4.65
POWER_SCALE_HORIZONTAL: float = 0.8
POWER_SCALE_VERTICAL_DOWN: float = 0.6
POWER_SCALE_VERTICAL_UP: float = 0.3
POWER_SAMPLE_WINDOW_S: float = 20.0
POWER_TREND_WINDOW_S: float = 60.0

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

# --- ONVIF / discovery ---
ONVIF_ENABLE: bool = True
ONVIF_DEVICE_PATH: str = "/onvif/device_service"
ONVIF_MEDIA_PATH: str = "/onvif/media_service"
ONVIF_PTZ_PATH: str = "/onvif/ptz_service"

# --- Resilience: camera watchdog/backoff ---
CAM_STALL_RESTART_S: float = 2.5        # no frame for this long => restart camera
CAM_MIN_UPTIME_OK_S: float = 20.0
CAM_RESTART_BACKOFF_S: float = 5.0
CAM_RESTART_BACKOFF_MAX_S: float = 60.0
CAM_RESTARTS_MAX_PER_HOUR: int = 120

# --- Resilience: request shaping ---
MOVE_MIN_INTERVAL_S: float = 0.02

import os
from typing import Optional, TYPE_CHECKING

PISUGAR_ENABLED: bool = False
PISUGAR_BASE_URL: str = os.environ.get("PISUGAR_URL", "http://127.0.0.1:8423")
PISUGAR_BATTERY_FULL_VOLTS: float = 4.2
PISUGAR_RAIL_TARGET_VOLTS: float = 5.0
PISUGAR_BATTERY_THRESHOLD_VOLTS: float = 4.5

# --- Home Assistant integration / discovery ---
HA_DISCOVERY_ENABLE: bool = os.environ.get("HA_DISCOVERY", "0").lower() in ("1", "true", "yes", "on")
HA_MQTT_BROKER: Optional[str] = os.environ.get("HA_MQTT_BROKER")
HA_MQTT_PORT: int = int(os.environ.get("HA_MQTT_PORT", "1883"))
HA_MQTT_USERNAME: Optional[str] = os.environ.get("HA_MQTT_USERNAME")
HA_MQTT_PASSWORD: Optional[str] = os.environ.get("HA_MQTT_PASSWORD")
HA_MQTT_BASE_TOPIC: str = os.environ.get("HA_MQTT_BASE_TOPIC", "homeassistant")
HA_BASE_URL: Optional[str] = os.environ.get("HA_BASE_URL")
HA_CAMERA_NAME: str = os.environ.get("HA_CAMERA_NAME", "Obstenet Camera")
HA_CAMERA_ID: str = os.environ.get("HA_CAMERA_ID", "obstenet_cam")

import argparse
import atexit
import glob
import io
import json
import logging
import resource
import shutil
import signal
import socket
import struct
import sys
import threading
import time
import uuid
import subprocess
import xml.etree.ElementTree as ET
import urllib.request
from collections import deque, defaultdict
from dataclasses import dataclass
from typing import Dict, Tuple, List

import faulthandler
from flask import Flask, Response, abort, jsonify, request, make_response
from urllib.parse import urlsplit

from multiprocessing import Process, Queue as MPQueue, Event as MPEvent

if TYPE_CHECKING:  # pragma: no cover - import-only for type checkers
    from picamera2 import Picamera2  # noqa: F401
    from picamera2.encoders import JpegEncoder  # noqa: F401
    from picamera2.outputs import FileOutput  # noqa: F401
    from libcamera import Transform  # noqa: F401

# Hardware libs are imported lazily to allow diagnostics to run without hardware.
Picamera2 = None
JpegEncoder = None
FileOutput = None
Transform = None
pantilthat = None


def _import_dependencies(strict: bool = True) -> dict[str, str]:
    """Import required hardware libraries with helpful diagnostics."""

    missing: dict[str, str] = {}
    global Picamera2, JpegEncoder, FileOutput, Transform, pantilthat

    if Picamera2 is None or JpegEncoder is None or FileOutput is None or Transform is None:
        try:
            from picamera2 import Picamera2 as _Picamera2  # type: ignore
            from picamera2.encoders import JpegEncoder as _JpegEncoder  # type: ignore
            from picamera2.outputs import FileOutput as _FileOutput  # type: ignore
            from libcamera import Transform as _Transform  # type: ignore
            Picamera2, JpegEncoder, FileOutput, Transform = _Picamera2, _JpegEncoder, _FileOutput, _Transform
        except Exception as e:  # pragma: no cover - relies on platform packages
            missing["picamera2/libcamera"] = str(e)

    if pantilthat is None:
        try:
            import pantilthat as _pantilthat  # type: ignore
            pantilthat = _pantilthat
        except Exception as e:  # pragma: no cover - relies on hardware libs
            missing["pantilthat"] = str(e)

    if missing and strict:
        if "picamera2/libcamera" in missing:
            sys.stderr.write(
                "ERROR: Picamera2/libcamera required. Install with:\n"
                "  sudo apt update && sudo apt install -y python3-picamera2\n"
                f"Import failed: {missing['picamera2/libcamera']}\n"
            )
        if "pantilthat" in missing:
            sys.stderr.write(
                "ERROR: pantilthat required. Enable I2C in raspi-config and:\n"
                "  python3 -m pip install --upgrade pantilthat\n"
                f"Import failed: {missing['pantilthat']}\n"
            )
        sys.exit(2)

    return missing

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


class VoltageMonitor:
    """Sample supply voltage periodically with Raspberry Pi vcgencmd backend."""

    def __init__(
        self,
        enabled: bool = True,
        sample_hz: float = POWER_SAMPLE_RATE_HZ,
        pisugar_enabled: bool = PISUGAR_ENABLED,
        pisugar_url: str = PISUGAR_BASE_URL,
    ) -> None:
        self._enabled = bool(enabled)
        self._sample_hz = max(0.1, float(sample_hz))
        self._last_v: Optional[float] = None
        self._battery_percent: Optional[float] = None
        self._current_a: Optional[float] = None
        self._power_w: Optional[float] = None
        self._avg_v: Optional[float] = None
        self._min_v: Optional[float] = None
        self._max_v: Optional[float] = None
        self._trend_v_per_min: Optional[float] = None
        self._avg_current_a: Optional[float] = None
        self._avg_power_w: Optional[float] = None
        self._samples: deque[Tuple[float, Optional[float], Optional[float], Optional[float]]] = deque()
        self._uv_now = False
        self._uv_seen = False
        self._lock = threading.Lock()
        self._stop_evt = threading.Event()
        self._th: Optional[threading.Thread] = None
        self._vcgencmd_supported = shutil.which("vcgencmd") is not None
        self._pisugar_enabled = bool(pisugar_enabled)
        self._pisugar_url = pisugar_url.rstrip("/")
        self._logged_unavail = False

    def start(self) -> None:
        if not self._enabled:
            return
        if not (self._vcgencmd_supported or self._pisugar_enabled):
            if not self._logged_unavail:
                log.warning("Voltage monitor disabled: vcgencmd unavailable and PiSugar disabled.")
                self._logged_unavail = True
            return
        if self._th and self._th.is_alive():
            return
        self._stop_evt.clear()
        self._th = threading.Thread(target=self._run, name="vmon", daemon=True)
        self._th.start()

    def stop(self) -> None:
        self._stop_evt.set()

    def _run(self) -> None:
        while not self._stop_evt.is_set():
            try:
                v, battery_percent, current_a, power_w, uv_now, uv_seen = self._sample_once()
                with self._lock:
                    self._last_v = v
                    self._battery_percent = battery_percent
                    self._current_a = current_a
                    self._power_w = power_w
                    self._update_samples_locked(v, current_a, power_w)
                    self._uv_now = uv_now
                    self._uv_seen = uv_seen
            except Exception:
                pass
            time.sleep(1.0 / self._sample_hz)

    def _sample_once(self) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], bool, bool]:
        pisugar = self._read_pisugar_status()
        v = pisugar.get("voltage")
        battery_percent = pisugar.get("percent")
        current_a = pisugar.get("current_a")
        if v is None:
            v = self._read_voltage()
        power_w = v * current_a if v is not None and current_a is not None else None
        uv_now, uv_seen = self._read_throttled()
        return v, battery_percent, current_a, power_w, uv_now, uv_seen

    def set_pisugar_enabled(self, enabled: bool) -> None:
        self._pisugar_enabled = bool(enabled)

    def _extract_numeric(self, payload: object, keys: Tuple[str, ...]) -> Tuple[Optional[float], Optional[str]]:
        if isinstance(payload, (int, float)):
            return float(payload), None
        if isinstance(payload, dict):
            for key in keys:
                if key in payload:
                    try:
                        return float(payload[key]), key
                    except (TypeError, ValueError):
                        continue
            for key in ("data", "battery", "result", "info"):
                if key in payload:
                    found, found_key = self._extract_numeric(payload[key], keys)
                    if found is not None:
                        return found, found_key
        if isinstance(payload, list):
            for item in payload:
                found, found_key = self._extract_numeric(item, keys)
                if found is not None:
                    return found, found_key
        return None, None

    def _extract_voltage(self, payload: object) -> Optional[float]:
        val, _ = self._extract_numeric(payload, ("voltage", "voltage_v", "battery_voltage", "batteryVoltage"))
        return val

    def _extract_percent(self, payload: object) -> Optional[float]:
        val, _ = self._extract_numeric(payload, ("percent", "percentage", "battery_percent", "batteryPercentage", "batteryLevel"))
        return val

    def _extract_current(self, payload: object) -> Tuple[Optional[float], Optional[str]]:
        return self._extract_numeric(payload, ("current", "current_a", "current_ma", "current_mA", "battery_current"))

    def _read_pisugar_voltage(self) -> Optional[float]:
        if not self._pisugar_enabled:
            return None
        url = f"{self._pisugar_url}/api/v1/getBattery"
        try:
            with urllib.request.urlopen(url, timeout=1.0) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            return self._normalize_pisugar_voltage(self._extract_voltage(data))
        except Exception:
            return None

    def _read_pisugar_status(self) -> Dict[str, Optional[float]]:
        if not self._pisugar_enabled:
            return {"voltage": None, "percent": None, "current_a": None}
        url = f"{self._pisugar_url}/api/v1/getBattery"
        try:
            with urllib.request.urlopen(url, timeout=1.0) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            voltage = self._normalize_pisugar_voltage(self._extract_voltage(data))
            percent = self._normalize_pisugar_percent(self._extract_percent(data))
            current, key = self._extract_current(data)
            current_a = self._normalize_pisugar_current(current, key)
            if percent is None:
                percent = self._read_pisugar_percent_fallback()
            return {"voltage": voltage, "percent": percent, "current_a": current_a}
        except Exception:
            return {"voltage": None, "percent": None, "current_a": None}

    def _read_pisugar_percent_fallback(self) -> Optional[float]:
        url = f"{self._pisugar_url}/api/v1/getBatteryPercentage"
        try:
            with urllib.request.urlopen(url, timeout=1.0) as resp:
                data = json.loads(resp.read().decode("utf-8"))
            return self._normalize_pisugar_percent(self._extract_percent(data))
        except Exception:
            return None

    def _normalize_pisugar_voltage(self, v: Optional[float]) -> Optional[float]:
        if v is None:
            return None
        if v < PISUGAR_BATTERY_THRESHOLD_VOLTS:
            scale = PISUGAR_RAIL_TARGET_VOLTS / max(1e-6, PISUGAR_BATTERY_FULL_VOLTS)
            return v * scale
        return v

    def _normalize_pisugar_percent(self, percent: Optional[float]) -> Optional[float]:
        if percent is None:
            return None
        if 0.0 <= percent <= 1.0:
            percent *= 100.0
        return max(0.0, min(100.0, percent))

    def _normalize_pisugar_current(self, current: Optional[float], key: Optional[str]) -> Optional[float]:
        if current is None:
            return None
        if key and "ma" in key.lower():
            return current / 1000.0
        if abs(current) > 10.0:
            return current / 1000.0
        return current

    def _read_voltage(self) -> Optional[float]:
        """Attempt to read the 5V supply; ignore clearly invalid rails (e.g. core)."""
        if not self._vcgencmd_supported:
            return None
        try:
            # Prefer explicit rail selection if supported; Raspberry Pi's default is SoC core
            # (~1V) which would falsely trigger the motion governor.
            cmd = ["vcgencmd", "measure_volts", "supply"]
            r = subprocess.run(cmd, check=False, capture_output=True, text=True, timeout=1.0)
            if r.returncode == 0:
                out = r.stdout.strip()
                if out.lower().startswith("volt="):
                    val = out.split("=", 1)[-1].lower().replace("v", "").strip()
                    fval = float(val)
                    # Reject implausible rails (e.g., core) so we fall back to throttled flags only.
                    if fval >= 3.0:
                        return fval
        except Exception:
            pass
        try:
            r = subprocess.run(["vcgencmd", "measure_volts"], check=False, capture_output=True, text=True, timeout=1.0)
            if r.returncode != 0:
                return None
            out = r.stdout.strip()
            if not out.lower().startswith("volt="):
                return None
            val = out.split("=", 1)[-1].lower().replace("v", "").strip()
            fval = float(val)
            # Reject implausible rails (e.g., core) so we fall back to throttled flags only.
            return fval if fval >= 3.0 else None
        except Exception:
            return None

    def _read_throttled(self) -> Tuple[bool, bool]:
        if not self._vcgencmd_supported:
            return False, False
        try:
            r = subprocess.run(["vcgencmd", "get_throttled"], check=False, capture_output=True, text=True, timeout=1.0)
            if r.returncode != 0:
                return False, False
            out = r.stdout.strip().lower()
            if "0x" in out:
                val = int(out.split("0x", 1)[-1], 16)
                uv_now = bool(val & 0x1)
                uv_seen = bool(val & 0x10000)
                return uv_now, uv_seen
        except Exception:
            pass
        return False, False

    def voltage(self) -> Optional[float]:
        with self._lock:
            return self._last_v

    def battery_percent(self) -> Optional[float]:
        with self._lock:
            return self._battery_percent

    def power_stats(self) -> Dict[str, Optional[float]]:
        with self._lock:
            return {
                "battery_percent": self._battery_percent,
                "voltage_avg_v": self._avg_v,
                "voltage_min_v": self._min_v,
                "voltage_max_v": self._max_v,
                "voltage_trend_v_per_min": self._trend_v_per_min,
                "current_a": self._current_a,
                "current_avg_a": self._avg_current_a,
                "power_w": self._power_w,
                "power_avg_w": self._avg_power_w,
            }

    def _update_samples_locked(self, v: Optional[float], current_a: Optional[float], power_w: Optional[float]) -> None:
        now = time.monotonic()
        self._samples.append((now, v, current_a, power_w))
        cutoff = now - max(POWER_SAMPLE_WINDOW_S, POWER_TREND_WINDOW_S)
        while self._samples and self._samples[0][0] < cutoff:
            self._samples.popleft()
        volts = [sv for _, sv, _, _ in self._samples if sv is not None]
        currents = [sc for _, _, sc, _ in self._samples if sc is not None]
        powers = [sp for _, _, _, sp in self._samples if sp is not None]
        self._avg_v = sum(volts) / len(volts) if volts else None
        self._min_v = min(volts) if volts else None
        self._max_v = max(volts) if volts else None
        self._avg_current_a = sum(currents) / len(currents) if currents else None
        self._avg_power_w = sum(powers) / len(powers) if powers else None
        trend_samples = [(ts, sv) for ts, sv, _, _ in self._samples if sv is not None and ts >= now - POWER_TREND_WINDOW_S]
        self._trend_v_per_min = self._compute_trend(trend_samples)

    def _compute_trend(self, samples: List[Tuple[float, float]]) -> Optional[float]:
        if len(samples) < 2:
            return None
        t0, v0 = samples[0]
        t1, v1 = samples[-1]
        dt = t1 - t0
        if dt <= 0:
            return None
        return (v1 - v0) / (dt / 60.0)

    def undervoltage_now(self) -> bool:
        with self._lock:
            return bool(self._uv_now)

    def undervoltage_seen(self) -> bool:
        with self._lock:
            return bool(self._uv_seen)

    def available(self) -> bool:
        return self._enabled and (self._vcgencmd_supported or self._pisugar_enabled)


class MotionGovernor:
    """Scale motion commands based on available voltage margin."""

    def __init__(self, monitor: VoltageMonitor, enabled: bool = POWER_MGMT_ENABLED) -> None:
        self._monitor = monitor
        self._enabled = bool(enabled)
        self._last_state = "normal"

    def _axis_floor(self, dp: float, dt: float) -> float:
        if abs(dt) > abs(dp):
            if dt < 0:
                return POWER_SCALE_VERTICAL_UP
            return POWER_SCALE_VERTICAL_DOWN
        return POWER_SCALE_HORIZONTAL

    def _interp(self, v: float, hi: float, lo: float, floor: float) -> float:
        if v >= hi:
            return 1.0
        if v <= lo:
            return floor
        span = max(1e-6, hi - lo)
        return floor + ((v - lo) / span) * (1.0 - floor)

    def apply(self, dp: float, dt: float) -> Tuple[float, float, str, Optional[float], float, bool]:
        if not self._enabled or not self._monitor.available():
            return dp, dt, "bypass", None, 1.0, False
        v = self._monitor.voltage()
        uv = self._monitor.undervoltage_now() or self._monitor.undervoltage_seen()
        action = "normal"
        factor = 1.0
        recovered = False
        if uv or (v is not None and v <= POWER_CUTOFF_VOLTS):
            action = "inhibit"
            dp = 0.0; dt = 0.0
            recovered = _recover_voltage_for_motion()
        elif v is not None:
            floor = self._axis_floor(dp, dt)
            factor = self._interp(v, POWER_WARN_VOLTS, POWER_LIMIT_VOLTS, floor)
            if factor < 0.999:
                action = "scale"
                dp *= factor; dt *= factor

        self._log_transition(action, v, dp, dt, factor, recovered)
        return dp, dt, action, v, factor, recovered

    def _log_transition(self, action: str, v: Optional[float], dp: float, dt: float, factor: float, recovered: bool) -> None:
        state_key = f"{action}-{int(round((v or 0.0)*100))}"
        if state_key == self._last_state:
            return
        self._last_state = state_key
        if action == "inhibit":
            log.warning("POWER CUTOFF v=%.2fV action=inhibit recovered=%s", v if v is not None else -1.0, recovered)
        elif action == "scale":
            axis = "vertical_up" if abs(dt) >= abs(dp) and dt < 0 else "vertical_down" if abs(dt) >= abs(dp) else "horizontal"
            log.warning("POWER WARN v=%.2fV action=scale axis=%s factor=%.2f dp=%.2f dt=%.2f", v if v is not None else -1.0, axis, factor, dp, dt)
        elif action == "normal" and v is not None:
            log.info("POWER RECOVER v=%.2fV action=resume", v)


_voltage_monitor = VoltageMonitor(enabled=POWER_MGMT_ENABLED, sample_hz=POWER_SAMPLE_RATE_HZ)
_motion_gov = MotionGovernor(_voltage_monitor, enabled=POWER_MGMT_ENABLED)

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

_servo = ServoManager()

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


def _recover_voltage_for_motion() -> bool:
    """Temporarily pause the camera to shed load and allow voltage to recover."""
    if not POWER_MGMT_ENABLED:
        return False
    try:
        lock = _cam_lock  # type: ignore[name-defined]
        cam = _picam2     # type: ignore[name-defined]
    except Exception:
        return False
    if lock is None:
        return False
    global _cam_last_start_ts
    with lock:
        if cam is None or _picam2 is None:  # type: ignore[name-defined]
            return False
        log.warning("POWER RECOVERY: pausing camera to free headroom for motion command")
        try:
            _picam2.stop_recording()  # type: ignore[operator]
        except Exception:
            pass
        try:
            _picam2.close()  # type: ignore[operator]
        except Exception:
            pass
        globals()["_picam2"] = None
        _cam_last_start_ts = 0.0

    def _restart_later() -> None:
        time.sleep(0.75)
        try:
            _safe_boot_camera_with_retry()
        except Exception as e:
            log.error("Camera restart after power recovery failed: %s", e)

    threading.Thread(target=_restart_later, name="cam-power-recover", daemon=True).start()
    return True


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
  .status {{ font-size:0.95rem; color:#bbb; }}
  .status.warn {{ color:#ffb347; }}
  .status.cutoff {{ color:#ff6b6b; font-weight:700; }}
  main {{ display:flex; flex-direction:column; align-items:center; gap:.6rem; padding:.6rem; }}
  .stream-wrap {{ position:relative; width:100%; max-width:960px; }}
  .stream-wrap img {{ display:block; width:100%; height:auto; border:1px solid #333; border-radius:6px; }}
  .stream-loading {{
    position:absolute; inset:0; display:flex; flex-direction:column;
    align-items:center; justify-content:center; gap:.5rem;
    background:rgba(17,17,17,0.78); text-transform:uppercase; letter-spacing:.2em;
  }}
  .stream-loading .logo {{
    font-size:clamp(2.4rem, 7vw, 4.2rem);
    font-weight:700; color:#f2f2f2;
  }}
  .stream-loading .sub {{
    font-size:clamp(.8rem, 2.6vw, 1.1rem); color:#aaa; letter-spacing:.25em;
  }}
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
  .compact .stream-wrap img {{ border:none; border-radius:0; }}
</style>
</head>
<body>
<header>
  <strong>OBSTENET</strong>
  <span class="spacer"></span>
  <div class="toolbar">
    <input id="step" type="number" min="1" max="30" value="{DEFAULT_STEP_DEG}">
    <button id="center">Center</button>
    <button id="snap">Snapshot</button>
    <button id="release">Release</button>
    <button id="led" class="hidden" aria-pressed="false">LED: Off</button>
  </div>
  <div id="power-status" class="status" role="status" aria-live="polite">Power: --</div>
  <div id="connection-status" class="status" role="status" aria-live="polite">Stream: connecting…</div>
</header>
<main>
  <div class="stream-wrap">
    <img id="stream" src="/stream.mjpg" alt="camera stream" referrerpolicy="no-referrer" loading="eager" decoding="async">
    <div id="stream-loading" class="stream-loading" aria-hidden="true">
      <div class="logo">Obstenet</div>
      <div class="sub">Loading</div>
    </div>
  </div>
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
  const powerStatus = document.getElementById('power-status');
  const connectionStatus = document.getElementById('connection-status');
  const streamImg = document.getElementById('stream');
  const streamLoading = document.getElementById('stream-loading');
  let reconnectTimer = null;
  let reconnectAttempt = 0;
  let lastConnText = '';

  function setConnectionStatus(text, level) {{
    if (!connectionStatus || text === lastConnText) return;
    connectionStatus.classList.remove('warn', 'cutoff');
    if (level) connectionStatus.classList.add(level);
    connectionStatus.textContent = text;
    lastConnText = text;
  }}

  function showStreamLoading(show) {{
    if (!streamLoading) return;
    streamLoading.classList.toggle('hidden', !show);
    streamLoading.setAttribute('aria-hidden', show ? 'false' : 'true');
  }}

  function reloadStream() {{
    if (!streamImg) return;
    const base = '/stream.mjpg';
    streamImg.src = base + '?ts=' + Date.now();
  }}

  function scheduleReconnect() {{
    if (reconnectTimer) return;
    reconnectAttempt = Math.min(reconnectAttempt + 1, 8);
    const delayMs = Math.min(5000, 400 * Math.pow(1.6, reconnectAttempt));
    setConnectionStatus(`Stream: reconnecting in ${{Math.ceil(delayMs / 1000)}}s…`, 'warn');
    showStreamLoading(true);
    reconnectTimer = setTimeout(() => {{
      reconnectTimer = null;
      reloadStream();
    }}, delayMs);
  }}
  function renderPowerStatus(resp) {{
    if (!powerStatus || !resp || typeof resp !== 'object') return resp;
    const action = resp.power_action || 'bypass';
    const v = typeof resp.voltage_v === 'number' ? resp.voltage_v : null;
    const factor = typeof resp.power_factor === 'number' ? resp.power_factor : null;
    const recovered = !!resp.power_recovered;
    const battery = typeof resp.battery_percent === 'number' ? resp.battery_percent : null;
    let text = 'Power: ' + action;
    powerStatus.classList.remove('warn','cutoff');
    if (factor !== null && factor < 0.999) text += ' (' + Math.round(factor*100) + '%)';
    if (v !== null) text += ' @ ' + v.toFixed(2) + 'V';
    if (recovered) text += ' (camera paused to recover)';
    if (battery !== null && factor === null && v === null && (action === 'bypass' || action === 'normal')) {{
      text = 'Power: ' + Math.round(battery) + '%';
    }}
    if (action === 'scale') powerStatus.classList.add('warn');
    if (action === 'inhibit') powerStatus.classList.add('cutoff');
    powerStatus.textContent = text;
    return resp;
  }}
  if (streamImg) {{
    showStreamLoading(true);
    streamImg.addEventListener('load', () => {{
      reconnectAttempt = 0;
      if (reconnectTimer) {{
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }}
      setConnectionStatus('Stream: live');
      showStreamLoading(false);
    }});
    streamImg.addEventListener('error', () => {{
      scheduleReconnect();
    }});
    setConnectionStatus('Stream: connecting…');
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
  document.getElementById('up').onclick    = () => postJSON('/api/move', {{dp:0, dt:-step()}}).then(renderPowerStatus).catch(()=>{{}});
  document.getElementById('down').onclick  = () => postJSON('/api/move', {{dp:0, dt: step()}}).then(renderPowerStatus).catch(()=>{{}});
  document.getElementById('left').onclick  = () => postJSON('/api/move', {{dp:-step(), dt:0}}).then(renderPowerStatus).catch(()=>{{}});
  document.getElementById('right').onclick = () => postJSON('/api/move', {{dp: step(), dt:0}}).then(renderPowerStatus).catch(()=>{{}});
  document.getElementById('home').onclick  = () => postJSON('/api/home', {{}}).catch(()=>{{}});

  // Keyboard shortcuts (desktop-only to avoid stealing focus in embeds)
  if (!window.matchMedia('(pointer: coarse)').matches && qs.get('embed') !== '1') {{
    window.addEventListener('keydown', (ev) => {{
      const s = step(), k = ev.key.toLowerCase();
        if (k==='arrowup')    postJSON('/api/move', {{dp:0, dt:-s}}).then(renderPowerStatus).catch(()=>{{}});
        if (k==='arrowdown')  postJSON('/api/move', {{dp:0, dt: s}}).then(renderPowerStatus).catch(()=>{{}});
        if (k==='arrowleft')  postJSON('/api/move', {{dp:-s, dt:0}}).then(renderPowerStatus).catch(()=>{{}});
        if (k==='arrowright') postJSON('/api/move', {{dp: s, dt:0}}).then(renderPowerStatus).catch(()=>{{}});
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


def _guess_base_url() -> str:
    """Best effort HTTP base URL for discovery (mDNS/MQTT)."""
    if HA_BASE_URL:
        return HA_BASE_URL.rstrip("/")

    candidates: list[str] = []
    for iface in ("tailscale0", "wlan0", "eth0", "en0"):
        ip = _iface_ipv4(iface)
        if ip:
            candidates.append(ip)
    try:
        hostname_ip = socket.gethostbyname(socket.gethostname())
        if hostname_ip:
            candidates.append(hostname_ip)
    except Exception:
        pass
    host = next((c for c in candidates if c and not c.startswith("127.")), "127.0.0.1")
    return f"http://{host}:{PORT}"


def _onvif_uri(path: str, base_url: Optional[str] = None) -> str:
    base = (base_url or _ha_base_url).rstrip("/") if _ha_base_url else (base_url or _guess_base_url())
    if path.startswith("http://") or path.startswith("https://"):
        return path
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{base}{path}"


def _ha_device_info() -> Dict[str, object]:
    return {
        "identifiers": ["obstenet", HA_CAMERA_ID],
        "manufacturer": "Obstenet",
        "model": "PanTilt Camera",
        "name": HA_CAMERA_NAME,
    }


def _ha_camera_payload(base_url: str) -> Dict[str, object]:
    urls = {
        "mjpeg_url": f"{base_url}/stream.mjpg",
        "still_image_url": f"{base_url}/snapshot.jpg",
        "stream_source": f"{base_url}/stream.mjpg",
        "control_api": f"{base_url}/api",
    }
    availability_topic = f"{HA_MQTT_BASE_TOPIC}/camera/{HA_CAMERA_ID}/status"
    return {
        "name": HA_CAMERA_NAME,
        "unique_id": HA_CAMERA_ID,
        "device": _ha_device_info(),
        # Home Assistant MQTT discovery for cameras expects the structured availability
        # block instead of legacy availability_topic fields.
        "availability": [{
            "topic": availability_topic,
            "payload_available": "online",
            "payload_not_available": "offline",
        }],
        "json_attributes_topic": f"{HA_MQTT_BASE_TOPIC}/camera/{HA_CAMERA_ID}/attributes",
        # Home Assistant Generic camera uses these URLs; stream_source helps the stream component.
        **urls,
    }


_ha_base_url: str = _guess_base_url()


def _onvif_envelope(body: str) -> str:
    return (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://www.w3.org/2003/05/soap-envelope\">"
        "<SOAP-ENV:Body>" + body + "</SOAP-ENV:Body></SOAP-ENV:Envelope>"
    )


def _onvif_service_map(base_url: Optional[str] = None) -> Dict[str, str]:
    return {
        "Device": _onvif_uri(ONVIF_DEVICE_PATH, base_url=base_url),
        "Media": _onvif_uri(ONVIF_MEDIA_PATH, base_url=base_url),
        "PTZ": _onvif_uri(ONVIF_PTZ_PATH, base_url=base_url),
    }


def _onvif_capabilities_payload(base_url: Optional[str] = None) -> str:
    services = _onvif_service_map(base_url=base_url)
    return _onvif_envelope(
        f"<tds:GetCapabilitiesResponse xmlns:tds=\"http://www.onvif.org/ver10/device/wsdl\">"
        f"<tds:Capabilities>"
        f"<tds:Device><tt:XAddr xmlns:tt=\"http://www.onvif.org/ver10/schema\">{services['Device']}</tt:XAddr></tds:Device>"
        f"<tds:Media><tt:XAddr xmlns:tt=\"http://www.onvif.org/ver10/schema\">{services['Media']}</tt:XAddr></tds:Media>"
        f"<tds:PTZ><tt:XAddr xmlns:tt=\"http://www.onvif.org/ver10/schema\">{services['PTZ']}</tt:XAddr></tds:PTZ>"
        f"</tds:Capabilities></tds:GetCapabilitiesResponse>"
    )


def _onvif_get_services_payload(base_url: Optional[str] = None) -> str:
    services = _onvif_service_map(base_url=base_url)
    body = "".join(
        f"<tds:Service><tds:Namespace>http://www.onvif.org/ver10/{name.lower()}</tds:Namespace>"
        f"<tds:XAddr>{uri}</tds:XAddr><tds:Version><tt:Major xmlns:tt=\"http://www.onvif.org/ver10/schema\">2</tt:Major>"
        f"<tt:Minor xmlns:tt=\"http://www.onvif.org/ver10/schema\">0</tt:Minor></tds:Version></tds:Service>"
        for name, uri in services.items()
    )
    return _onvif_envelope(f"<tds:GetServicesResponse xmlns:tds=\"http://www.onvif.org/ver10/device/wsdl\">{body}</tds:GetServicesResponse>")


def _onvif_profile_payload(base_url: Optional[str] = None) -> str:
    return _onvif_envelope(
        "<trt:GetProfilesResponse xmlns:trt=\"http://www.onvif.org/ver10/media/wsdl\">"
        "<trt:Profiles token=\"profile1\" fixed=\"true\" xmlns:tt=\"http://www.onvif.org/ver10/schema\">"
        "<tt:Name>Obstenet</tt:Name>"
        "<tt:PTZConfiguration token=\"ptz1\"><tt:Name>PTZ</tt:Name></tt:PTZConfiguration>"
        "<tt:VideoSourceConfiguration token=\"vsrc1\"><tt:Name>Camera</tt:Name></tt:VideoSourceConfiguration>"
        "<tt:VideoEncoderConfiguration token=\"venc1\"><tt:Name>MJPEG</tt:Name></tt:VideoEncoderConfiguration>"
        "</trt:Profiles></trt:GetProfilesResponse>"
    )


def _onvif_stream_payload(base_url: Optional[str] = None) -> str:
    stream_uri = _onvif_uri("/stream.mjpg", base_url=base_url)
    return _onvif_envelope(
        f"<trt:GetStreamUriResponse xmlns:trt=\"http://www.onvif.org/ver10/media/wsdl\">"
        f"<trt:MediaUri><tt:Uri xmlns:tt=\"http://www.onvif.org/ver10/schema\">{stream_uri}</tt:Uri></trt:MediaUri>"
        f"</trt:GetStreamUriResponse>"
    )


def _onvif_snapshot_payload(base_url: Optional[str] = None) -> str:
    snap_uri = _onvif_uri("/snapshot.jpg", base_url=base_url)
    return _onvif_envelope(
        f"<trt:GetSnapshotUriResponse xmlns:trt=\"http://www.onvif.org/ver10/media/wsdl\">"
        f"<trt:MediaUri><tt:Uri xmlns:tt=\"http://www.onvif.org/ver10/schema\">{snap_uri}</tt:Uri></trt:MediaUri>"
        f"</trt:GetSnapshotUriResponse>"
    )


def _onvif_date_time_payload() -> str:
    now = time.gmtime()
    return _onvif_envelope(
        "<tds:GetSystemDateAndTimeResponse xmlns:tds=\"http://www.onvif.org/ver10/device/wsdl\">"
        "<tds:SystemDateAndTime><tt:UTCDateTime xmlns:tt=\"http://www.onvif.org/ver10/schema\">"
        f"<tt:Time><tt:Hour>{now.tm_hour}</tt:Hour><tt:Minute>{now.tm_min}</tt:Minute><tt:Second>{now.tm_sec}</tt:Second></tt:Time>"
        f"<tt:Date><tt:Year>{now.tm_year}</tt:Year><tt:Month>{now.tm_mon}</tt:Month><tt:Day>{now.tm_mday}</tt:Day></tt:Date>"
        "</tt:UTCDateTime></tds:SystemDateAndTime></tds:GetSystemDateAndTimeResponse>"
    )


def _onvif_device_info_payload() -> str:
    return _onvif_envelope(
        "<tds:GetDeviceInformationResponse xmlns:tds=\"http://www.onvif.org/ver10/device/wsdl\">"
        "<tds:Manufacturer>Obstenet</tds:Manufacturer>"
        "<tds:Model>PanTilt Camera</tds:Model>"
        "<tds:FirmwareVersion>1.0</tds:FirmwareVersion>"
        "<tds:SerialNumber>0001</tds:SerialNumber>"
        "<tds:HardwareId>obstenet</tds:HardwareId>"
        "</tds:GetDeviceInformationResponse>"
    )


def _onvif_scopes_payload() -> str:
    scopes = [
        "onvif://www.onvif.org/type/ptz",
        "onvif://www.onvif.org/Profile/Streaming",
        f"onvif://www.onvif.org/name/{HA_CAMERA_NAME.replace(' ', '_')}",
    ]
    body = "".join(f"<tds:Scopes>{s}</tds:Scopes>" for s in scopes)
    return _onvif_envelope(f"<tds:GetScopesResponse xmlns:tds=\"http://www.onvif.org/ver10/device/wsdl\">{body}</tds:GetScopesResponse>")


def _onvif_probe_response(base_url: Optional[str] = None) -> bytes:
    xaddr = _onvif_uri(ONVIF_DEVICE_PATH, base_url=base_url)
    scopes = " ".join([
        "onvif://www.onvif.org/type/ptz",
        "onvif://www.onvif.org/Profile/Streaming",
        "onvif://www.onvif.org/hardware/obstenet",
    ])
    payload = (
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<SOAP-ENV:Envelope xmlns:SOAP-ENV=\"http://www.w3.org/2003/05/soap-envelope\""
        " xmlns:dn=\"http://www.onvif.org/ver10/network/wsdl\""
        " xmlns:wsdd=\"http://schemas.xmlsoap.org/ws/2005/04/discovery\""
        " xmlns:wsa=\"http://schemas.xmlsoap.org/ws/2004/08/addressing\""
        " xmlns:tds=\"http://www.onvif.org/ver10/device/wsdl\">"
        "<SOAP-ENV:Body><wsdd:ProbeMatches><wsdd:ProbeMatch>"
        f"<wsa:EndpointReference><wsa:Address>urn:uuid:{HA_CAMERA_ID}</wsa:Address></wsa:EndpointReference>"
        "<wsdd:Types>dn:NetworkVideoTransmitter tds:Device</wsdd:Types>"
        f"<wsdd:Scopes>{scopes}</wsdd:Scopes>"
        f"<wsdd:XAddrs>{xaddr}</wsdd:XAddrs>"
        "<wsdd:MetadataVersion>1</wsdd:MetadataVersion>"
        "</wsdd:ProbeMatch></wsdd:ProbeMatches></SOAP-ENV:Body></SOAP-ENV:Envelope>"
    )
    return payload.encode("utf-8")


def _onvif_handle_ptz(body: str) -> Optional[str]:
    try:
        root = ET.fromstring(body)
        ns = {"tt": "http://www.onvif.org/ver10/schema"}
        ptz = root.find(".//tt:PanTilt", ns)
        if ptz is not None:
            x = float(ptz.attrib.get("x", "0"))
            y = float(ptz.attrib.get("y", "0"))
            try:
                _servo.move(dp=float(x) * DEFAULT_STEP_DEG, dt=float(-y) * DEFAULT_STEP_DEG)
            except Exception as e:
                log.warning("PTZ move failed: %s", e)
        return _onvif_envelope("<tptz:ContinuousMoveResponse xmlns:tptz=\"http://www.onvif.org/ver20/ptz/wsdl\" />")
    except Exception as e:
        log.warning("Failed to parse PTZ command: %s", e)
        return None


def _onvif_ws_discovery_loop() -> None:
    """Respond to WS-Discovery probes so Home Assistant finds the device."""
    if not ONVIF_ENABLE:
        return
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("", 3702))
        except OSError:
            sock.bind(("0.0.0.0", 3702))
        mreq = struct.pack("=4sl", socket.inet_aton("239.255.255.250"), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        sock.settimeout(1.0)
    except Exception as e:
        log.warning("ONVIF discovery listener failed to start: %s", e)
        return

    log.info("ONVIF WS-Discovery responder listening on udp/3702")
    while True:
        try:
            data, addr = sock.recvfrom(4096)
        except socket.timeout:
            continue
        except Exception as e:
            log.warning("ONVIF discovery socket error: %s", e)
            break
        if b"Probe" not in data:
            continue
        try:
            resp = _onvif_probe_response()
            sock.sendto(resp, addr)
        except Exception as e:
            log.debug("Failed sending WS-Discovery response: %s", e)
_runtime_started = False


def _publish_home_assistant_mqtt(base_url: str) -> None:
    if not HA_DISCOVERY_ENABLE:
        return
    if not HA_MQTT_BROKER:
        log.info("HA discovery requested but HA_MQTT_BROKER not set; skipping MQTT publish.")
        return
    try:
        import paho.mqtt.client as mqtt
    except Exception as e:
        log.warning("HA discovery enabled but paho-mqtt is missing: %s", e)
        return

    config_topic = f"{HA_MQTT_BASE_TOPIC}/camera/{HA_CAMERA_ID}/config"
    status_topic = f"{HA_MQTT_BASE_TOPIC}/camera/{HA_CAMERA_ID}/status"
    attr_topic = f"{HA_MQTT_BASE_TOPIC}/camera/{HA_CAMERA_ID}/attributes"
    payload = _ha_camera_payload(base_url)

    client = mqtt.Client(client_id=f"{HA_CAMERA_ID}-{uuid.uuid4().hex[:6]}")
    client.will_set(status_topic, payload="offline", retain=True)
    if HA_MQTT_USERNAME:
        client.username_pw_set(HA_MQTT_USERNAME, HA_MQTT_PASSWORD or None)
    try:
        client.connect(HA_MQTT_BROKER, HA_MQTT_PORT, keepalive=30)
        client.loop_start()
        client.publish(config_topic, json.dumps(payload), retain=True)
        client.publish(status_topic, payload="online", retain=True)
        client.publish(attr_topic, json.dumps({
            "mjpeg_url": payload["mjpeg_url"],
            "still_image_url": payload["still_image_url"],
            "control_api": payload["control_api"],
            "base_url": base_url,
        }), retain=True)
        log.info("Published Home Assistant MQTT discovery for %s to %s:%s", HA_CAMERA_ID, HA_MQTT_BROKER, HA_MQTT_PORT)
    except Exception as e:
        log.warning("Failed to publish Home Assistant discovery via MQTT: %s", e)
    finally:
        time.sleep(0.2)
        try:
            client.loop_stop()
            client.disconnect()
        except Exception:
            pass
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


@app.route(ONVIF_DEVICE_PATH, methods=["POST"])
@app.route(ONVIF_MEDIA_PATH, methods=["POST"])
@app.route(ONVIF_PTZ_PATH, methods=["POST"])
def onvif_service():
    if not ONVIF_ENABLE:
        abort(404)
    body = request.data.decode("utf-8", errors="ignore")
    base_url = _ha_base_url or _guess_base_url()
    resp_xml: Optional[str] = None

    if "GetCapabilities" in body:
        resp_xml = _onvif_capabilities_payload(base_url=base_url)
    elif "GetServices" in body:
        resp_xml = _onvif_get_services_payload(base_url=base_url)
    elif "GetProfiles" in body:
        resp_xml = _onvif_profile_payload(base_url=base_url)
    elif "GetStreamUri" in body:
        resp_xml = _onvif_stream_payload(base_url=base_url)
    elif "GetSnapshotUri" in body:
        resp_xml = _onvif_snapshot_payload(base_url=base_url)
    elif "GetSystemDateAndTime" in body:
        resp_xml = _onvif_date_time_payload()
    elif "GetDeviceInformation" in body:
        resp_xml = _onvif_device_info_payload()
    elif "GetScopes" in body:
        resp_xml = _onvif_scopes_payload()
    elif "ContinuousMove" in body:
        resp_xml = _onvif_handle_ptz(body)
    elif "Stop" in body:
        resp_xml = _onvif_envelope("<tptz:StopResponse xmlns:tptz=\"http://www.onvif.org/ver20/ptz/wsdl\" />")

    if resp_xml is None:
        abort(400, description="Unsupported ONVIF request")
    return Response(resp_xml, mimetype="application/soap+xml")


@app.get("/.well-known/homeassistant")
def home_assistant_well_known():
    payload = {
        # Home Assistant companion apps expect this key for discovery. It must be a fully
        # qualified URL reachable by the device performing discovery.
        "base_url": _ha_base_url,
        "uuid": HA_CAMERA_ID,
        "name": HA_CAMERA_NAME,
        "device": _ha_device_info(),
        "services": {
            "camera": {
                "mjpeg_url": f"{_ha_base_url}/stream.mjpg",
                "still_image_url": f"{_ha_base_url}/snapshot.jpg",
                "control_api": f"{_ha_base_url}/api",
            }
        },
    }
    return jsonify(payload)


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
    parsed = urlsplit(base)
    hostname = parsed.hostname or ""
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    return {
        "id": 1,
        "name": "obstenet",
        "proto": request.scheme,
        "hostname": hostname,
        "port": port,
        "base_url": base,
        "stream_url": stream_url,
        "snapshot_url": snapshot_url,
        # motionEye expects remote peers to advertise a netcam_url and whether the feed
        # is enabled. We map these to our MJPEG stream and always expose a single camera.
        "enabled": True,
        "netcam_url": stream_url,
        "netcam_userpass": "",
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
    gdp, gdt, action, v, factor, recovered = _motion_gov.apply(hdp, hdt)
    resp = _servo.move(gdp, gdt)
    if resp is None: return jsonify({"status": "queued"}), 202
    if not resp.ok: abort(503, description=resp.error or "servo error")
    payload = resp.state or {}
    payload["power_action"] = action
    payload["voltage_v"] = v
    payload["power_factor"] = factor
    payload["power_recovered"] = recovered
    payload["battery_percent"] = _voltage_monitor.battery_percent()
    payload["power_stats"] = _voltage_monitor.power_stats()
    payload["requested_dp"] = dp
    payload["requested_dt"] = dt
    payload["applied_dp"] = gdp
    payload["applied_dt"] = gdt
    return jsonify(payload)

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

def _start_all(force: bool = False) -> None:
    global _ha_base_url, _runtime_started
    if _runtime_started and not force:
        return

    _import_dependencies(strict=True)
    _voltage_monitor.start()
    _servo.start()
    _safe_boot_camera_with_retry()
    th = threading.Thread(target=_camera_watchdog_loop, name="camera-wd", daemon=True)
    th.start()
    if ONVIF_ENABLE:
        threading.Thread(target=_onvif_ws_discovery_loop, name="onvif-discovery", daemon=True).start()
    _ha_base_url = _guess_base_url()
    if HA_DISCOVERY_ENABLE:
        try:
            _publish_home_assistant_mqtt(_ha_base_url)
        except Exception as e:
            log.warning("Home Assistant discovery failed: %s", e)
    _runtime_started = True

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


def _run_diagnostics() -> int:
    """Basic environment and dependency checks for production readiness."""

    results: list[tuple[str, bool, str]] = []

    def add(name: str, ok: bool, detail: str) -> None:
        results.append((name, ok, detail))

    py_ok = sys.version_info >= (3, 10)
    add("Python version", py_ok, sys.version.split(" ")[0])

    missing = _import_dependencies(strict=False)
    add("picamera2/libcamera", "picamera2/libcamera" not in missing, missing.get("picamera2/libcamera", "available"))
    add("pantilthat", "pantilthat" not in missing, missing.get("pantilthat", "available"))

    vcmd = shutil.which("vcgencmd")
    add("vcgencmd", bool(vcmd), vcmd or "not found")

    add("I2C bus (/dev/i2c-1)", os.path.exists("/dev/i2c-1"), "present" if os.path.exists("/dev/i2c-1") else "missing")
    add("Camera device (/dev/video0)", os.path.exists("/dev/video0"), "present" if os.path.exists("/dev/video0") else "missing")

    log.info("OBSTENET diagnostics:")
    for name, ok, detail in results:
        log.info("  %-24s : %s (%s)", name, "OK" if ok else "FAIL", detail)

    all_ok = all(ok for _, ok, _ in results)
    if not all_ok:
        log.error("Diagnostics completed with failures. Resolve the issues above before production use.")
    else:
        log.info("Diagnostics completed successfully. System appears production-ready.")
    return 0 if all_ok else 1

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main(argv: Optional[list[str]] = None) -> None:
    parser = argparse.ArgumentParser(description="Obstenet pan/tilt camera controller")
    parser.add_argument("--host", help="Override bind host (defaults to BIND_MODE logic)")
    parser.add_argument("--port", type=int, help="Override bind port (default: 8000)")
    parser.add_argument("--diagnostic", action="store_true", help="Run dependency diagnostics and exit")
    parser.add_argument("--pisugar", "-p", action="store_true", help="Enable PiSugar voltage integration")
    args = parser.parse_args(argv)

    if args.port is not None:
        global PORT
        PORT = args.port
    if args.host:
        global HOST
        HOST = args.host

    if args.diagnostic:
        sys.exit(_run_diagnostics())
    if args.pisugar:
        global PISUGAR_ENABLED
        PISUGAR_ENABLED = True
        _voltage_monitor.set_pisugar_enabled(True)
        log.info("PiSugar integration enabled (base URL: %s).", PISUGAR_BASE_URL)

    if (STREAM_ROTATION % 360) not in (0, 90, 180, 270):
        sys.stderr.write("STREAM_ROTATION must be 0/90/180/270\n"); sys.exit(3)

    _start_all()

    bind_host = args.host or _pick_host()
    try:
        app.run(host=bind_host, port=PORT, debug=False, threaded=True)
    except OSError as e:
        sys.stderr.write(f"ERROR: Failed to bind {bind_host}:{PORT} -> {e}\n")
        try:
            app.run(host="127.0.0.1", port=PORT, debug=False, threaded=True)
        except Exception as e2:
            sys.stderr.write(f"ERROR: Fallback bind failed: {e2}\n")
            sys.exit(4)


if __name__ == "__main__":
    main()
