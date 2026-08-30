# Wokwi public MQTT stations

Create three **new** ESP32 MicroPython projects. Each project uses the common `main.py` and `mqtt_simple.py`, plus its station-specific `diagram.json`.

- Austin: `wokwi/austin/diagram.json`
- Tongji: `wokwi/tongji/diagram.json`
- Disclosed digital twin: `wokwi/virtual/diagram.json`

The station-selection pins in each diagram select the correct station identity. The firmware connects through `Wokwi-GUEST` to the public MQTT broker and exchanges telemetry, verified dataset injections, global-weight metadata, and pump commands with the Render service.

The public broker is used only for this non-sensitive trial. A managed TLS MQTT account should replace it for a permanent deployment.
