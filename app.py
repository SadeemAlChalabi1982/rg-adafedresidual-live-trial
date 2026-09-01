from __future__ import annotations

import json
import os
import queue
import threading
import time
import uuid
from http.server import ThreadingHTTPServer
from pathlib import Path

import numpy as np
import paho.mqtt.client as mqtt

import federated_system as fs


ROOT = Path(__file__).resolve().parent
TOPIC_ROOT = os.getenv("MQTT_TOPIC_ROOT", "rgaf-sadeem-paper3-live-20260831-v1")
MQTT_HOST = os.getenv("MQTT_HOST", "broker.hivemq.com")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
FEDERATED_ROUNDS = int(os.getenv("FEDERATED_ROUNDS", "6"))
LIVE_TIMEOUT_SECONDS = float(os.getenv("STATION_HEARTBEAT_TIMEOUT_SECONDS", "75"))
CYCLE_SECONDS = float(os.getenv("CYCLE_SECONDS", "12"))
DEMO_ONLY = os.getenv("DEMO_ONLY", "false").lower() in {"1", "true", "yes"}

WOKWI_URLS = {
    "austin": os.getenv("WOKWI_AUSTIN_URL", "https://wokwi.com/projects/473854149978000385"),
    "tongji": os.getenv("WOKWI_TONGJI_URL", "https://wokwi.com/projects/473855638857098241"),
    "virtual": os.getenv("WOKWI_VIRTUAL_URL", "https://wokwi.com/projects/473855699260353537"),
}


def topic(kind: str, station: str) -> str:
    return f"{TOPIC_ROOT}/{kind}/{station}"


class PublicFederatedEngine:
    def __init__(self):
        _, self.splits, x_center, x_scale, y_center, y_scale = fs.prepare_data()
        self.edges = {
            station: fs.RaspberryPiClient(
                station,
                self.splits[station]["train"],
                self.splits[station]["validation"],
                x_center,
                x_scale,
                y_center,
                y_scale,
            )
            for station in fs.STATIONS
        }
        self.cloud = fs.RelationGuidedCloud(
            fs.initialize_parameters(len(fs.FEATURES), 18, len(fs.TARGETS))
        )
        self.tests = {
            station: self.splits[station]["test"].reset_index(drop=True)
            for station in fs.STATIONS
        }
        self.inbox: queue.Queue[dict] = queue.Queue()
        self.last_seen = {station: 0.0 for station in fs.STATIONS}
        self.last_status_seen = {station: 0.0 for station in fs.STATIONS}
        self.reported_online = {station: False for station in fs.STATIONS}
        self.latest_live: dict[str, dict] = {}
        self.requested_rows: dict[str, dict] = {}
        self.histories = {station: [] for station in fs.STATIONS}
        self.previous_raw = {station: None for station in fs.STATIONS}
        self.mqtt_connected = False
        self.mqtt_client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"rgaf-render-{uuid.uuid4().hex[:10]}",
        )
        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_disconnect = self._on_disconnect
        self.mqtt_client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        self.mqtt_connected = reason_code == 0
        if not self.mqtt_connected:
            fs.STATE.event("mqtt", f"MQTT connection failed: {reason_code}")
            return
        for station in fs.STATIONS:
            client.subscribe(topic("telemetry", station), qos=0)
            client.subscribe(topic("status", station), qos=0)
        fs.STATE.event("mqtt", "Public MQTT broker connected")

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties=None):
        self.mqtt_connected = False
        fs.STATE.event("mqtt", f"MQTT disconnected: {reason_code}")

    def _on_message(self, client, userdata, message):
        if "/status/" in message.topic:
            station = message.topic.rsplit("/", 1)[-1]
            if station in fs.STATIONS:
                status = message.payload.decode("utf-8", errors="ignore").strip().lower()
                if status == "online":
                    self.reported_online[station] = True
                    self.last_status_seen[station] = time.time()
                else:
                    self.reported_online[station] = False
            return
        if "/telemetry/" not in message.topic:
            return
        try:
            payload = json.loads(message.payload.decode("utf-8"))
            station = payload.get("station")
            if station in fs.STATIONS:
                self.last_seen[station] = time.time()
                self.reported_online[station] = True
                self.inbox.put(payload)
        except Exception as error:
            fs.STATE.event("mqtt", f"Malformed station telemetry: {error}")

    def publish(self, kind: str, station: str, payload: dict, retain: bool = False):
        if not self.mqtt_connected:
            return
        self.mqtt_client.publish(
            topic(kind, station),
            json.dumps(payload, separators=(",", ":")),
            qos=0,
            retain=retain,
        )

    def initialize_state(self):
        fs.STATE.update(
            running=True,
            phase="Initializing public Raspberry Pi clients",
            round=0,
            max_rounds=FEDERATED_ROUNDS,
            live_cycle=0,
            deployment={
                "transport": "PUBLIC MQTT",
                "live_mode": "WAITING FOR STATIONS",
                "accuracy_scope": "strict live gate: no readings or pump commands before all three Wokwi stations are online",
                "hardware": "Wokwi ESP32 sensor/actuator node + Python Raspberry Pi 4B logical client",
            },
            broker={"connected": False, "host": MQTT_HOST, "port": MQTT_PORT},
        )
        for station in fs.STATIONS:
            fs.STATE.station(
                station,
                name=fs.DISPLAY_NAMES[station],
                origin=fs.ORIGINS[station],
                controller="Wokwi ESP32 + public Python Raspberry Pi client",
                phase="Preparing federated client",
                local_progress=0,
                sensors={},
                pumps={"alum": 0.0, "chlorine": 0.0},
                forecast=0.0,
                online=False,
                connection_state="OFFLINE",
                stale_seconds=0.0,
                source="initializing",
                wokwi_url=WOKWI_URLS[station],
            )

    def connect_mqtt(self):
        if DEMO_ONLY:
            fs.STATE.event("mqtt", "MQTT disabled; strict zero-output standby remains active")
            return
        try:
            self.mqtt_client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=45)
            self.mqtt_client.loop_start()
        except Exception as error:
            fs.STATE.event("mqtt", f"MQTT startup error: {error}")

    def train_federated_model(self):
        for round_number in range(1, FEDERATED_ROUNDS + 1):
            fs.STATE.update(round=round_number, phase="Raspberry Pi local training")
            updates = []
            for station in fs.STATIONS:
                edge = self.edges[station]
                edge.receive_global(self.cloud.parameters, round_number)
                fs.STATE.station(station, phase="Local RG-AdaFedResidual training", local_progress=25)
                updates.append(edge.train_local(round_number))
                fs.STATE.station(station, local_progress=100)
            fs.STATE.update(phase="Relation-guided aggregation")
            result = self.cloud.aggregate(updates)
            fs.STATE.cloud(
                status="Relation-guided aggregation completed",
                contributors=3,
                weights_hash=result["weights_hash"],
                client_weights=result["client_weights"],
                relation_scores=result["relation_scores"],
            )
            fs.STATE.event("aggregate", f"Federated round {round_number}/{FEDERATED_ROUNDS} completed")

        for station, edge in self.edges.items():
            edge.receive_global(self.cloud.parameters, self.cloud.round)
            edge.calibrate_private_head(include_validation=True)
            self.publish(
                "weights",
                station,
                {"global_round": self.cloud.round, "weights_hash": self.cloud.parameters.digest()},
                retain=True,
            )

    def causal_features(self, station: str, sensors: dict) -> dict:
        values = {key: float(sensors[key]) for key in (
            "raw_turbidity", "filtered_turbidity", "ph", "temperature", "flow", "residual_chlorine"
        )}
        raw = values["raw_turbidity"]
        previous = self.previous_raw[station]
        values["raw_delta"] = 0.0 if previous is None else raw - previous
        self.previous_raw[station] = raw
        history = self.histories[station]
        history.append(raw)
        del history[:-24]
        for window in (3, 6, 12, 24):
            values[f"raw_roll{window}"] = float(np.mean(history[-window:]))
        return values

    def infer_and_update(self, station: str, sensors: dict, live: bool, source: str):
        started = time.perf_counter()
        features = self.causal_features(station, sensors)
        prediction = self.edges[station].infer(features)
        alum, chlorine, mode = fs.regulation(prediction, features)
        latency_ms = 1000.0 * (time.perf_counter() - started)
        if live:
            self.publish(
                "command",
                station,
                {
                    "global_round": self.cloud.round,
                    "alum_percent": alum,
                    "chlorine_percent": chlorine,
                    "mode": mode,
                },
            )
        fs.STATE.station(
            station,
            phase="Live MQTT regulation" if live else "Safety standby — zero output",
            sensors={key: features[key] for key in (
                "raw_turbidity", "filtered_turbidity", "ph", "temperature", "flow", "residual_chlorine"
            )},
            pumps={"alum": alum, "chlorine": chlorine},
            forecast=float(prediction["forecast_h6"]),
            control_mode=mode,
            latency_ms=latency_ms,
            online=live,
            connection_state="LIVE" if live else "OFFLINE",
            stale_seconds=0.0,
            source=source,
            local_progress=100,
            global_round=self.cloud.round,
        )

    def hold_station_at_zero(self, station: str, connected: bool):
        """Expose a safe, truthful standby state until the live quorum exists."""
        fs.STATE.station(
            station,
            phase=(
                "Connected — waiting for all three stations"
                if connected
                else "Waiting for Wokwi telemetry"
            ),
            sensors={},
            pumps={"alum": 0.0, "chlorine": 0.0},
            forecast=None,
            control_mode="SAFETY INTERLOCK — ZERO OUTPUT",
            latency_ms=0.0,
            online=connected,
            connection_state="READY" if connected else "OFFLINE",
            stale_seconds=0.0,
            source="strict_live_standby",
            local_progress=0,
            global_round=self.cloud.round,
        )

    def hold_last_validated_state(self, station: str, connected: bool, age_seconds: float):
        """Freeze the last validated values during a temporary link interruption."""
        fs.STATE.station(
            station,
            phase=(
                "Link ready — waiting for full station quorum"
                if connected
                else "Connection interrupted — holding last validated state"
            ),
            online=connected,
            connection_state="READY" if connected else "HOLDING",
            stale_seconds=max(0.0, age_seconds),
            control_mode="HOLDING LAST VALIDATED COMMAND",
            source="last_validated_live_state",
        )

    def request_station_rows(self, cycle: int):
        for station in fs.STATIONS:
            frame = self.tests[station]
            row = frame.iloc[(cycle - 1) % len(frame)]
            payload = {
                "sequence": int(row.sequence),
                "origin": fs.ORIGINS[station],
                "raw_turbidity": float(row.raw_turbidity),
                "filtered_turbidity": float(row.filtered_turbidity),
                "ph": float(row.ph),
                "temperature": float(row.temperature),
                "flow": float(row.flow),
                "residual_chlorine": float(row.residual_chlorine),
            }
            self.requested_rows[station] = payload
            self.publish(
                "inject",
                station,
                payload,
            )

    def drain_live_telemetry(self):
        while True:
            try:
                payload = self.inbox.get_nowait()
            except queue.Empty:
                return
            station = payload.get("station")
            if station in fs.STATIONS and isinstance(payload.get("sensors"), dict):
                self.latest_live[station] = payload

    def run_forever(self):
        self.initialize_state()
        self.connect_mqtt()
        self.train_federated_model()
        cycle = 0
        control_cycle = 0
        while True:
            cycle += 1
            self.request_station_rows(cycle)
            time.sleep(min(1.0, CYCLE_SECONDS / 3.0))
            self.drain_live_telemetry()
            now = time.time()
            live_stations = [
                station
                for station in fs.STATIONS
                if (
                    self.reported_online[station]
                    and
                    now - self.last_seen[station] <= LIVE_TIMEOUT_SECONDS
                    and station in self.latest_live
                )
            ]
            all_live = len(live_stations) == len(fs.STATIONS)

            if all_live:
                control_cycle += 1
                for station in fs.STATIONS:
                    # Display and regulate the exact current row transmitted to
                    # the Wokwi node.  The returned telemetry is used as the
                    # online/freshness acknowledgement, not as a frozen cache.
                    sensors = self.requested_rows.get(
                        station,
                        self.latest_live[station]["sensors"],
                    )
                    self.infer_and_update(
                        station,
                        sensors,
                        live=True,
                        source="mqtt_transmitted_station_stream",
                    )
            elif control_cycle > 0:
                for station in fs.STATIONS:
                    connected = station in live_stations
                    age_seconds = (
                        now - self.last_seen[station]
                        if station in self.latest_live
                        else 0.0
                    )
                    self.hold_last_validated_state(station, connected, age_seconds)
            else:
                for station in fs.STATIONS:
                    connected = station in live_stations
                    self.hold_station_at_zero(station, connected)
                    if connected:
                        self.publish(
                            "command",
                            station,
                            {
                                "global_round": self.cloud.round,
                                "alum_percent": 0.0,
                                "chlorine_percent": 0.0,
                                "mode": "SAFETY_INTERLOCK_WAITING_ALL_STATIONS",
                            },
                        )

            live_mode = (
                "LIVE MQTT"
                if all_live
                else "HOLDING LAST STATE"
                if control_cycle > 0
                else "WAITING FOR STATIONS"
            )
            fs.STATE.update(
                running=True,
                phase=(
                    "Closed-loop MQTT regulation"
                    if all_live
                    else "Connection interrupted — holding last validated state"
                    if control_cycle > 0
                    else f"Safety standby — {len(live_stations)}/3 stations connected"
                ),
                live_cycle=control_cycle,
                broker={"connected": self.mqtt_connected, "host": MQTT_HOST, "port": MQTT_PORT},
                deployment={
                    "transport": "PUBLIC MQTT",
                    "live_mode": live_mode,
                    "accuracy_scope": "strict live gate with last-validated-state hold during temporary MQTT interruptions",
                    "hardware": "Wokwi ESP32 sensor/actuator node + Python Raspberry Pi 4B logical client",
                },
            )
            fs.STATE.cloud(
                status=(
                    "Live commands returned to all Wokwi pumps"
                    if all_live
                    else f"Holding last validated state — {len(live_stations)}/3 links fresh"
                    if control_cycle > 0
                    else f"Safety standby — waiting for {3 - len(live_stations)} station(s)"
                ),
                contributors=3 if all_live else len(live_stations),
                weights_hash=self.cloud.parameters.digest(),
            )
            fs.STATE.event(
                "live" if all_live else "hold" if control_cycle > 0 else "standby",
                (
                    f"Control cycle {control_cycle}: all three Wokwi stations acknowledged"
                    if all_live
                    else f"Holding cycle {control_cycle}: {len(live_stations)}/3 links fresh"
                    if control_cycle > 0
                    else f"Zero-output interlock: {len(live_stations)}/3 stations connected"
                ),
            )
            time.sleep(max(0.2, CYCLE_SECONDS - min(1.0, CYCLE_SECONDS / 3.0)))


def serve():
    fs.WEB = ROOT / "web"
    port = int(os.getenv("PORT", "10000"))
    engine = PublicFederatedEngine()
    threading.Thread(target=engine.run_forever, daemon=True, name="federated-engine").start()
    handler = lambda *args, **kwargs: fs.DashboardHandler(*args, directory=str(fs.WEB), **kwargs)
    server = ThreadingHTTPServer(("0.0.0.0", port), handler)
    print(f"RG-AdaFedResidual public trial listening on 0.0.0.0:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    serve()

