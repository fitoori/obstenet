# tests/test_api_home.py
#!/usr/bin/env python3
"""Hardware-free regression test: POST /api/home must HOME, not sweep to a stop.

Before this test existed, `/api/home` homed tilt to TILT_HOME_DEG (70) and then
issued a second move to "the farthest extreme from current tilt". Because home()
always lands tilt at 70 first, that second move was a constant, not a sweep, and
always parked tilt at TILT_RANGE_DEG[0] = -45 -- hard against a mechanical stop,
contradicting the TILT_HOME_DEG comment in the same file. Every caller
(HA rest_command.pan_home, HA script.pan_recenter, the web UI Center/home
buttons, the 'c' hotkey) means "recentre".

Stubs `pantilthat` + `smbus2` in *this process only* (the live obstenet.service
is a separate process and is never touched), loads the candidate obstenet file
as a module, swaps a recording fake over the module-global `_servo`, and drives
the route through Flask's test client. No I2C, no servos, no network.

Usage: python3 test_api_home.py /path/to/obstenet.py
Exit 0 = all pass.
"""
import importlib.util
import os
import sys
import types

PATH = sys.argv[1]

# ---- hardware stubs -----------------------------------------------------------
_pt = types.ModuleType("pantilthat")
_pt.calls = []
_pt.frequency = lambda hz: _pt.calls.append(("freq", hz))
_pt.servo_enable = lambda ch, on: _pt.calls.append(("enable", ch, on))
_pt.pan = lambda v: _pt.calls.append(("pan", v))
_pt.tilt = lambda v: _pt.calls.append(("tilt", v))
sys.modules["pantilthat"] = _pt

class _FakeBus:
    def __init__(self, n): pass
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def write_quick(self, addr): return True
    def read_byte(self, addr): return 0
    def i2c_rdwr(self, *a): return None
_sm = types.ModuleType("smbus2")
_sm.SMBus = _FakeBus
class _msg:
    @staticmethod
    def write(addr, data): return (addr, data)
_sm.i2c_msg = _msg
sys.modules["smbus2"] = _sm

os.environ.setdefault("POWER_MGMT_ENABLED", "0")

spec = importlib.util.spec_from_file_location("obstenet_candidate", PATH)
M = importlib.util.module_from_spec(spec)
sys.modules["obstenet_candidate"] = M
spec.loader.exec_module(M)

FAILS = []
def check(name, cond, detail=""):
    print(("PASS  " if cond else "FAIL  ") + name + (("  -- " + str(detail)) if detail else ""))
    if not cond:
        FAILS.append(name)

# ---- helpers ------------------------------------------------------------------
# pantilthat maps -90..90 deg onto a 575..2325 us pulse (pantilt.py servo1/2 min/max).
# Reproduced here so the test asserts the same number a scope or an i2cget of
# regs 0x03/0x04 at addr 0x15 would show, not just the JSON.
PULSE_MIN_US = 575
PULSE_MAX_US = 2325

def deg_to_us(deg):
    """Servo pulse width in whole microseconds for a hardware angle in degrees."""
    assert -90.0 <= float(deg) <= 90.0, "angle out of servo range: %r" % (deg,)
    span = float(PULSE_MAX_US - PULSE_MIN_US)
    return int(PULSE_MIN_US + ((float(deg) + 90.0) / 180.0) * span)

class FakeServo:
    """Records every command the route issues; never touches hardware."""
    def __init__(self, home_tilt):
        self.home_calls = 0
        self.set_calls = []
        self._home_tilt = home_tilt

    def home(self):
        self.home_calls += 1
        return M._Resp(rid="home", ok=True,
                       state={"pan": M.PAN_HOME_DEG, "tilt": self._home_tilt})

    def set(self, pan, tilt):
        self.set_calls.append((pan, tilt))
        return M._Resp(rid="set", ok=True, state={"pan": M.PAN_HOME_DEG, "tilt": tilt})

def post_home():
    """POST /api/home against the loaded module with a recording servo."""
    fake = FakeServo(M.TILT_HOME_DEG)
    real, M._servo = M._servo, fake
    try:
        M.app.config["TESTING"] = True
        resp = M.app.test_client().post("/api/home")
        # Contract: the fake must actually have been installed and exercised,
        # and the test client must yield a response object we can inspect.
        assert M._servo is fake, "post_home: fake servo was not installed on the module"
        assert resp is not None and hasattr(resp, "status_code"), \
            "post_home: test client returned no response"
        return fake, resp
    finally:
        M._servo = real

# ---- 1. home means home -------------------------------------------------------
def t_home_is_home():
    fake, resp = post_home()
    # Contract: post_home yields the recording fake it installed and a response;
    # if the route never reached the servo, home_calls stays 0.
    assert isinstance(fake, FakeServo), "t_home_is_home: post_home must return the FakeServo"
    assert resp is not None and hasattr(resp, "status_code"), \
        "t_home_is_home: post_home must return a response with a status_code"
    check("1 /api/home returns 200", resp.status_code == 200, resp.status_code)

    body = resp.get_json() or {}
    check("1 /api/home reports the pan home angle",
          body.get("pan") == M.PAN_HOME_DEG,
          "pan=%r want %r" % (body.get("pan"), M.PAN_HOME_DEG))
    check("1 /api/home reports the tilt home angle",
          body.get("tilt") == M.TILT_HOME_DEG,
          "tilt=%r want %r" % (body.get("tilt"), M.TILT_HOME_DEG))

    check("1 /api/home homes exactly once", fake.home_calls == 1, fake.home_calls)
    check("1 /api/home issues NO move after homing",
          fake.set_calls == [],
          "post-home moves: %r" % (fake.set_calls,))

# ---- 2. the rig is never left resting on a mechanical stop --------------------
def t_never_rests_on_a_stop():
    _, resp = post_home()
    tilt = (resp.get_json() or {}).get("tilt")
    tlo, thi = float(M.TILT_RANGE_DEG[0]), float(M.TILT_RANGE_DEG[1])
    check("2 homed tilt is not at either end of TILT_RANGE_DEG",
          tilt is not None and tlo < float(tilt) < thi,
          "tilt=%r range=(%s, %s)" % (tilt, tlo, thi))

    # Same claim in the units the hardware actually shows.
    check("2 homed tilt pulse is the home pulse, not the bottom-stop pulse",
          tilt is not None and deg_to_us(tilt) == deg_to_us(M.TILT_HOME_DEG)
          and deg_to_us(tilt) != deg_to_us(tlo),
          "us=%r home_us=%r bottom_stop_us=%r"
          % (None if tilt is None else deg_to_us(tilt),
             deg_to_us(M.TILT_HOME_DEG), deg_to_us(tlo)))

# ---- 3. the docstring no longer promises a sweep ------------------------------
def t_docstring_matches_behaviour():
    # Only the summary line states what the route does; the body is free to
    # explain the removed sweep, so scope the check to the promise itself.
    doc = (M.app.view_functions["api_home"].__doc__ or "").strip()
    summary = doc.splitlines()[0].lower() if doc else ""
    check("3 /api/home summary line does not promise a tilt sweep",
          "sweep" not in summary, repr(summary))

t_home_is_home()
t_never_rests_on_a_stop()
t_docstring_matches_behaviour()

print("\n%d failure(s)" % len(FAILS))
sys.exit(1 if FAILS else 0)
