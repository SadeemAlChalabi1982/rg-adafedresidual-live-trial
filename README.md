# RG-AdaFedResidual Cloud Federated Laboratory

This branch runs an executable, reviewer-facing cloud simulation for three water-treatment stations. It is independent of the browser lifetime of the optional Wokwi circuit projects.

## Executable cloud path

- Three independent station threads own separate sensor/plant states and actuator command queues.
- Austin and Tongji stream their published-field test partitions; the third station remains explicitly identified as a disclosed digital twin.
- Every operational cycle performs local RG-AdaFedResidual updates at all three Raspberry Pi client processes, uploads model updates, executes relation-guided aggregation, broadcasts a new global version, runs H6 inference, and sends acknowledged dosing commands back to the station plants.
- Only model parameters and operational commands cross the federated boundary; raw station frames remain owned by their station runtimes.
- Wokwi links remain available for inspecting the corresponding ESP32 circuit diagrams, but Wokwi is not required for the cloud engine to continue.

## Render

Build command: `pip install -r requirements.txt`

Start command: `python cloud_app.py`

Health check: `/api/state`

The free Render instance is suitable for execution testing and spins down after inactivity. Keeping the public dashboard open maintains inbound polling during a review session. Upgrading the same service later removes the free-instance sleep behavior without a code change.

## Academic disclosure

The deployment is an executable cloud simulation. Wokwi represents the ESP32 electrical/firmware layer when opened; the continuously hosted station and Raspberry Pi client processes are cloud runtimes rather than claims of installed physical hardware.
