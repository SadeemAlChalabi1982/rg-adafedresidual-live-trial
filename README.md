# RG-AdaFedResidual Wokwi-Linked Federated Laboratory

This branch is the separately deployed, three-station Wokwi-linked edition. It does not replace or modify the standalone `cloud-federated-lab` deployment.

## Live distributed path

- Three distinct Wokwi projects represent the Austin, Tongji, and disclosed digital-twin ESP32 sensor/actuator stations.
- Each station publishes its own telemetry and heartbeat on an isolated topic namespace through the public MQTT broker.
- Three logical Raspberry Pi clients hosted by Render execute private local RG-AdaFedResidual updates.
- The coordinator verifies the three client updates through the PAV HMAC-SHA256 layer, performs relation-guided aggregation, broadcasts the new global version, runs H6 inference, and returns a separate dosing command to each Wokwi station.
- The unified cycle starts only after all three Wokwi stations are online together. A one-second synchronization window avoids a staggered visual start. A missing heartbeat is detected after 12 seconds and confirmed for 5 more seconds before the dashboard enters a coordinated hold state. The last validated readings and commands remain visible instead of resetting to zero.
- Wokwi may reduce a rich three-tab simulation to a small percentage of normal speed. Station telemetry, visual pulses and DS18B20 conversion therefore run without blocking the MQTT loop, while authenticated status snapshots are transmitted every 500 simulated milliseconds. The 12-second freshness window absorbs ordinary browser jitter while still making a deliberately stopped station visible promptly.
- Every station card includes a persistent **Controlled Input Perturbation** control. The operator can select −100%, −75%, −50%, −25%, −10%, 0%, +10%, +25%, +50%, +75%, or +100% for raw-turbidity load, hydraulic-flow load, a bounded pH shift, or chlorine demand. Zero preserves the original dataset row. The original and adjusted rows are evaluated by the same global model, and the interface displays each pump's baseline-to-active command and signed change. A disclosed monotonic turbidity guard ensures that greater raw-turbidity load cannot reduce alum dosing and reduced load cannot increase it without weakening the existing water-quality safety fallback.
- An acknowledged perturbation remains active across subsequent station cycles until the operator presses **Reset**. Hydraulic-flow controls scale both alum and chlorine pump rates to preserve chemical mass per unit water; zero flow invokes a **NO-FLOW INTERLOCK** that stops both dosing pumps. Queuing, acknowledgement, active control and reset events are recorded in the execution log.
- The alum and chlorine commands close the treatment loop through a disclosed first-order plant response: the current command determines the next-cycle filtered-turbidity and residual-chlorine state. The live outcome panel presents both pre-dose measurements and the modeled next-cycle values, including the independent chlorine change and target status.
- **Save Log** downloads the complete current-session execution archive as a UTF-8 CSV file with timestamp, event type, station and description columns; the on-screen panel remains intentionally limited to the most recent events.

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

The service runs on Render's free plan and may sleep after inactivity. Opening the linked dashboard wakes it; start all three linked Wokwi stations after the dashboard reports that the MQTT broker is connected. Control cycles continue without a fixed cycle-count limit while the three-station quorum remains available.
