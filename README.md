# RG-AdaFedResidual Standalone Federated Laboratory

This branch runs the same committee-ready interface and controlled-input tools as the Wokwi-linked edition, while remaining fully independent of Wokwi. The three cloud station runtimes acknowledge every cycle locally, so the laboratory starts automatically and continues when the optional circuit pages are closed.

## Executable cloud path

- Three independent station threads own separate sensor/plant states and actuator command queues.
- Austin and Tongji stream their published-field test partitions; the third station remains explicitly identified as a disclosed digital twin.
- Every operational cycle performs local RG-AdaFedResidual updates at all three Raspberry Pi client processes, uploads model updates, executes relation-guided aggregation, broadcasts a new global version, runs H6 inference, and sends acknowledged dosing commands back to the station plants.
- Only model parameters and operational commands cross the federated boundary; raw station frames remain owned by their station runtimes.
- Wokwi links remain available for inspecting the corresponding ESP32 circuit diagrams, but Wokwi is not required for the cloud engine to continue.
- Each station includes the same persistent controlled-input lever used by the linked edition. The operator can apply and hold a signed change to turbidity, flow, pH, or chlorine demand, inspect the resulting H6 forecast and pump response, and return to the original dataset stream with **Reset**.
- The alum and chlorine commands feed the same disclosed first-order next-cycle treatment response shown in the final linked interface.
- **Save Log** exports the complete current-session execution log as UTF-8 CSV.

## PAV security layer

PAV (Payload Authentication and Verification) is the project name for the message-security profile used between each logical station and the federated coordinator. Every telemetry packet and private model update is signed with a station-specific HMAC-SHA256 key. The coordinator verifies the station identity, payload digest, sequence, timestamp freshness, and single-use nonce before accepting the message. Altered, stale, or replayed messages are rejected before aggregation or actuation.

Keys are never returned by the API or rendered in the dashboard. Render can provision persistent keys through `PAV_KEY_AUSTIN`, `PAV_KEY_TONGJI`, and `PAV_KEY_VIRTUAL`; when those variables are absent, the process provisions fresh 256-bit runtime keys. HTTPS/TLS protects the browser connection separately. PAV provides message authenticity, integrity, freshness, and replay resistance; it is not payload encryption.

## Render

Build command: `pip install -r requirements.txt`

Start command: `python app.py`

Health check: `/api/state`

`LOCAL_REVIEW_MODE=true` activates the internal three-station acknowledgement loop and disables external MQTT publishing. The free Render instance is suitable for execution testing and spins down after inactivity. Opening the public dashboard wakes the service; no Wokwi tab is required.

## Academic disclosure

The deployment is an executable cloud simulation with the same user interface and control logic as the linked edition. Wokwi remains a visual circuit reference in this branch; the continuously hosted station and Raspberry Pi client processes are cloud runtimes rather than claims of installed physical hardware.
