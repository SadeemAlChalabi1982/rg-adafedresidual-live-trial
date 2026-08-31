# RG-AdaFedResidual Public Federated Trial

This is a public, reviewer-facing trial deployment for three water-treatment stations.

## Execution modes

- **LIVE MQTT:** all three Wokwi ESP32 simulations are publishing telemetry. Three Python Raspberry Pi logical clients run preprocessing, local RG-AdaFedResidual inference/training, relation-guided federation, and return pump commands.
- **VERIFIED TRACE FALLBACK:** one or more Wokwi simulations are offline. The same model runs against the disclosed external-validation trace, and the interface labels the fallback explicitly.

## Render

Build command: `pip install -r requirements.txt`

Start command: `python app.py`

Health check: `/api/state`

The free Render instance is suitable for trial deployment and spins down after inactivity. Upgrade the same service to Starter after validation.

For browser-based Wokwi review, Render keeps the most recent valid telemetry online for a five-minute grace period. This accommodates browser background-tab throttling; every new telemetry packet replaces the cached reading immediately.

## Academic disclosure

Wokwi simulates the ESP32 electrical/firmware layer. The hosted Python processes execute the same Raspberry Pi client code, but they are cloud-hosted logical Raspberry Pi clients rather than physical Raspberry Pi boards. Physical hardware validation remains a separate future step.
