# RG-AdaFedResidual Wokwi-Linked Federated Laboratory

This branch is the separately deployed, three-station Wokwi-linked edition. It does not replace or modify the standalone `cloud-federated-lab` deployment.

## Live distributed path

- Three distinct Wokwi projects represent the Austin, Tongji, and disclosed digital-twin ESP32 sensor/actuator stations.
- Each station publishes its own telemetry and heartbeat on an isolated topic namespace through the public MQTT broker.
- Three logical Raspberry Pi clients hosted by Render execute private local RG-AdaFedResidual updates.
- The coordinator verifies the three client updates through the PAV HMAC-SHA256 layer, performs relation-guided aggregation, broadcasts the new global version, runs H6 inference, and returns a separate dosing command to each Wokwi station.
- The dashboard advances only after the three-station live quorum is present. A short interruption holds the last validated readings and commands instead of resetting them.

## Isolation from the preserved edition

- Preserved standalone branch: `cloud-federated-lab`
- Linked branch: `wokwi-linked-lab`
- Preserved standalone service: `rg-adafedresidual-cloud-laboratory`
- Linked service: `rg-adafedresidual-wokwi-linked`
- Linked MQTT namespace: `rgaf-sadeem-paper3-linked-20260909-v1`

The separate branch, service name, and MQTT namespace prevent the linked edition from changing or commanding the preserved standalone edition.

## Render

Build command: `pip install -r requirements.txt`

Start command: `python app.py`

Health check: `/api/state`

The service runs on Render's free plan and may sleep after inactivity. Opening the linked dashboard wakes it; start all three linked Wokwi stations after the dashboard reports that the MQTT broker is connected.
