# OBSTENET

OBSTENET (Opposable Bounded Surveillance & Tracking with Elastic Networking) is a Python application that drives a Raspberry Pi camera and Pan-Tilt HAT through a Flask-based HTTP API. It tunes camera performance for lower latency and includes safeguards for servo reliability.

## Prerequisites

- Raspberry Pi with a compatible camera module configured and tested.
- Pan-Tilt HAT connected over I²C with I²C enabled in `raspi-config`.
- Python 3.10+ on Raspberry Pi OS (Bookworm or later recommended).
- System packages:
  - `python3-picamera2` (provides Picamera2 and libcamera bindings)
  - `python3-libcamera` (usually pulled in with Picamera2)
  - `python3-venv` (for creating a virtual environment)

## Installation

1. **Update packages and install camera dependencies**

   ```bash
   sudo apt update
   sudo apt install -y python3-picamera2 python3-libcamera python3-venv
   ```

2. **Enable I²C and install the Pan-Tilt driver**

   ```bash
   sudo raspi-config nonint do_i2c 0
   python3 -m pip install --upgrade pantilthat smbus2
   ```

3. **Create and activate a virtual environment** (recommended)

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

4. **Install Python dependencies**

   Flask is the only required Python package beyond the hardware libraries:

   ```bash
   python -m pip install --upgrade pip
   python -m pip install Flask
   ```

## Running the server

With dependencies installed and the virtual environment activated, start OBSTENET with:

```bash
python obstenet.py
```

Use `--host` or `--port` to override the defaults, or `--diagnostic` to run production readiness checks without starting the server:

```bash
python obstenet.py --diagnostic
```

By default the HTTP server binds to `0.0.0.0:8000`. Ensure the camera and servos are connected before launching to avoid startup failures.

## Troubleshooting

- If Picamera2 fails to import, ensure the `python3-picamera2` package is installed and the camera interface is enabled in `raspi-config`.
- If servo control fails, confirm I²C is enabled and the Pan-Tilt HAT is seated correctly; reinstall `pantilthat` and `smbus2` if necessary.

