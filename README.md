# RG-AdaFedResidual Cloud Federated Laboratory

This branch runs an executable, reviewer-facing cloud simulation for three water-treatment stations. It is independent of the browser lifetime of the optional Wokwi circuit projects.

## Executable cloud path

- Three independent station threads own separate sensor/plant states and actuator command queues.
- Austin and Tongji stream their published-field test partitions; the third station remains explicitly identified as a disclosed digital twin.
- Every operational cycle performs local RG-AdaFedResidual updates at all three Raspberry Pi client processes, uploads model updates, executes relation-guided aggregation, broadcasts a new global version, runs H6 inference, and sends acknowledged dosing commands back to the station plants.
- Only model parameters and operational commands cross the federated boundary; raw station frames remain owned by their station runtimes.
- Wokwi links remain available for inspecting the corresponding ESP32 circuit diagrams, but Wokwi is not required for the cloud engine to continue.

## PAV security layer

PAV (Payload Authentication and Verification) is the project name for the message-security profile used between each logical station and the federated coordinator. Every telemetry packet and private model update is signed with a station-specific HMAC-SHA256 key. The coordinator verifies the station identity, payload digest, sequence, timestamp freshness, and single-use nonce before accepting the message. Altered, stale, or replayed messages are rejected before aggregation or actuation.

Keys are never returned by the API or rendered in the dashboard. Render can provision persistent keys through `PAV_KEY_AUSTIN`, `PAV_KEY_TONGJI`, and `PAV_KEY_VIRTUAL`; when those variables are absent, the process provisions fresh 256-bit runtime keys. HTTPS/TLS protects the browser connection separately. PAV provides message authenticity, integrity, freshness, and replay resistance; it is not payload encryption.

## Render

Build command: `pip install -r requirements.txt`

Start command: `python cloud_app.py`

Health check: `/api/state`

The free Render instance is suitable for execution testing and spins down after inactivity. Keeping the public dashboard open maintains inbound polling during a review session. Upgrading the same service later removes the free-instance sleep behavior without a code change.

## Academic disclosure

The deployment is an executable cloud simulation. Wokwi represents the ESP32 electrical/firmware layer when opened; the continuously hosted station and Raspberry Pi client processes are cloud runtimes rather than claims of installed physical hardware.
