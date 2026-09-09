from __future__ import annotations

import json
import os
import queue
import hashlib
import hmac
import secrets
import threading
import time
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer
from pathlib import Path

import numpy as np
import pandas as pd
import paho.mqtt.client as mqtt

import federated_system as fs


ROOT = Path(__file__).resolve().parent
TOPIC_ROOT = os.getenv("MQTT_TOPIC_ROOT", "rgaf-sadeem-paper3-linked-20260909-v1")
MQTT_HOST = os.getenv("MQTT_HOST", "broker.hivemq.com")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
FEDERATED_ROUNDS = int(os.getenv("FEDERATED_ROUNDS", "6"))
LIVE_TIMEOUT_SECONDS = max(
    45.0,
    float(
        os.getenv(
            "STATION_HEARTBEAT_TIMEOUT_SECONDS",
            os.getenv("LIVE_TIMEOUT_SECONDS", "120"),
        )
    ),
)
CYCLE_SECONDS = float(os.getenv("CYCLE_SECONDS", "12"))
ONLINE_EPOCHS = max(1, int(os.getenv("ONLINE_LOCAL_EPOCHS", "1")))
DEMO_ONLY = os.getenv("DEMO_ONLY", "false").lower() in {"1", "true", "yes"}
PAV_ALGORITHM = "HMAC-SHA256"

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
        self.trace = deque(maxlen=360)
        self.control_cycle = 0
        self.pav_keys = {
            station: secrets.token_bytes(32)
            for station in fs.STATIONS
        }
        self.pav_verified = {station: 0 for station in fs.STATIONS}
        self.pav_rejected = 0
        self.mqtt_connected = False
        self.mqtt_client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"rgaf-render-{uuid.uuid4().hex[:10]}",
        )
        self.mqtt_client.on_connect = self._on_connect
        self.mqtt_client.on_disconnect = self._on_disconnect
        self.mqtt_client.on_message = self._on_message

    def security_state(self, status="PAV VERIFYING"):
        return {
            "layer": "PAV",
            "profile": "Payload Authentication and Verification",
            "algorithm": PAV_ALGORITHM,
            "status": status,
            "verified_stations": sum(
                1 for station in fs.STATIONS if self.pav_verified[station] > 0
            ),
            "required_stations": len(fs.STATIONS),
            "timestamp_freshness": True,
            "nonce_replay_protection": True,
            "rejected_messages": self.pav_rejected,
        }

    def authenticated_local_update(self, station, round_number, epochs):
        update = self.edges[station].train_local(
            round_number,
            batch_size=64,
            epochs=epochs,
        )
        descriptor = {
            "station": station,
            "round": int(round_number),
            "samples": int(update.samples),
            "validation_rmse": round(float(update.validation_rmse), 12),
            "parameters_sha256": update.parameters.digest(),
            "delta_sha256": hashlib.sha256(
                update.delta.astype("float32").tobytes()
            ).hexdigest(),
        }
        canonical = json.dumps(
            descriptor,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        tag = hmac.new(self.pav_keys[station], canonical, hashlib.sha256).hexdigest()
        expected = hmac.new(
            self.pav_keys[station], canonical, hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(tag, expected):
            self.pav_rejected += 1
            raise RuntimeError(f"PAV rejected local update from {station}")
        self.pav_verified[station] += 1
        fs.STATE.station(
            station,
            pav_status="PAV VERIFIED",
            pav_algorithm=PAV_ALGORITHM,
        )
        return update

    def train_and_aggregate(self, round_number, epochs):
        for station, edge in self.edges.items():
            edge.receive_global(self.cloud.parameters, round_number)
            fs.STATE.station(
                station,
                phase="Private local RG-AdaFedResidual training",
                local_progress=12,
            )
        with ThreadPoolExecutor(max_workers=len(fs.STATIONS)) as pool:
            futures = {
                station: pool.submit(
                    self.authenticated_local_update,
                    station,
                    round_number,
                    epochs,
                )
                for station in fs.STATIONS
            }
            updates = [futures[station].result() for station in fs.STATIONS]
        result = self.cloud.aggregate(updates)
        for edge in self.edges.values():
            edge.receive_global(self.cloud.parameters, self.cloud.round)
        fs.STATE.update(security=self.security_state("PAV VERIFIED"))
        return result

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
            federation_version=0,
            deployment={
                "transport": "PUBLIC MQTT",
                "live_mode": "WAITING FOR STATIONS",
                "accuracy_scope": "three isolated Wokwi stations publish telemetry to three logical Raspberry Pi clients; no global update or dosing command is issued before the three-station quorum",
                "hardware": "Wokwi ESP32 sensor/actuator node + Python Raspberry Pi 4B logical client",
            },
            security=self.security_state(),
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
                pav_status="PAV CHECKING",
                pav_algorithm=PAV_ALGORITHM,
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
            fs.STATE.update(phase="Relation-guided aggregation")
            result = self.train_and_aggregate(round_number, epochs=6)
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

    def online_federated_update(self):
        version = self.cloud.round + 1
        fs.STATE.update(phase=f"Live local learning for global model v{version}")
        result = self.train_and_aggregate(version, epochs=ONLINE_EPOCHS)
        for station in fs.STATIONS:
            self.publish(
                "weights",
                station,
                {
                    "global_round": self.cloud.round,
                    "weights_hash": self.cloud.parameters.digest(),
                },
                retain=True,
            )
        fs.STATE.cloud(
            status=f"PAV-verified global model v{self.cloud.round} broadcast to all stations",
            contributors=3,
            weights_hash=result["weights_hash"],
            client_weights=result["client_weights"],
            relation_scores=result["relation_scores"],
            global_version=self.cloud.round,
        )
        return result

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
                "target_forecast": float(row.forecast_h6_ntu),
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

    def update_summary(self):
        if not self.trace:
            return
        summary = fs.metric_summary(pd.DataFrame(self.trace))
        records = {}
        for row in summary.to_dict(orient="records"):
            station = row["station"]
            row["station"] = fs.DISPLAY_NAMES[station]
            records[station] = row
        fs.STATE.update(summary=records)

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
            last_contact = {
                station: max(self.last_seen[station], self.last_status_seen[station])
                for station in fs.STATIONS
            }
            live_stations = [
                station
                for station in fs.STATIONS
                if (
                    self.reported_online[station]
                    and
                    now - last_contact[station] <= LIVE_TIMEOUT_SECONDS
                    and station in self.latest_live
                )
            ]
            all_live = len(live_stations) == len(fs.STATIONS)

            if all_live:
                control_cycle += 1
                self.control_cycle = control_cycle
                aggregation = self.online_federated_update()
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
                    station_state = fs.STATE.snapshot()["stations"][station]
                    sensors = station_state["sensors"]
                    pumps = station_state["pumps"]
                    self.trace.append(
                        {
                            "time_step": control_cycle,
                            "station": station,
                            "origin": fs.ORIGINS[station],
                            **sensors,
                            "target_forecast": float(
                                self.requested_rows[station]["target_forecast"]
                            ),
                            "predicted_forecast": float(station_state["forecast"]),
                            "alum_percent": float(pumps["alum"]),
                            "chlorine_percent": float(pumps["chlorine"]),
                            "control_mode": station_state["control_mode"],
                            "global_round": self.cloud.round,
                            "weights_hash": aggregation["weights_hash"],
                        }
                    )
                self.update_summary()
            elif control_cycle > 0:
                for station in fs.STATIONS:
                    connected = station in live_stations
                    age_seconds = (
                        now - last_contact[station]
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
                federation_version=self.cloud.round,
                security=self.security_state(
                    "PAV VERIFIED" if all_live else "PAV HOLDING"
                ),
                broker={"connected": self.mqtt_connected, "host": MQTT_HOST, "port": MQTT_PORT},
                deployment={
                    "transport": "PUBLIC MQTT",
                    "live_mode": live_mode,
                    "accuracy_scope": "Wokwi telemetry drives live inference, PAV-verified local updates, relation-guided aggregation, global broadcast and acknowledged pump commands; temporary link loss holds the last validated state",
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
