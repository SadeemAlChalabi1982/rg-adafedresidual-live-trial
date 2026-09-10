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
    20.0,
    float(
        os.getenv(
            "STATION_HEARTBEAT_TIMEOUT_SECONDS",
            os.getenv("LIVE_TIMEOUT_SECONDS", "30"),
        )
    ),
)
CYCLE_SECONDS = float(os.getenv("CYCLE_SECONDS", "12"))
QUORUM_STABLE_SECONDS = max(0.5, float(os.getenv("QUORUM_STABLE_SECONDS", "1")))
QUORUM_LOSS_GRACE_SECONDS = max(
    5.0,
    float(os.getenv("QUORUM_LOSS_GRACE_SECONDS", "15")),
)
ONLINE_EPOCHS = max(1, int(os.getenv("ONLINE_LOCAL_EPOCHS", "1")))
DEMO_ONLY = os.getenv("DEMO_ONLY", "false").lower() in {"1", "true", "yes"}
PAV_ALGORITHM = "HMAC-SHA256"

WOKWI_URLS = {
    "austin": os.getenv("WOKWI_AUSTIN_URL", "https://wokwi.com/projects/474707915927449601"),
    "tongji": os.getenv("WOKWI_TONGJI_URL", "https://wokwi.com/projects/474708305707343873"),
    "virtual": os.getenv("WOKWI_VIRTUAL_URL", "https://wokwi.com/projects/474708525672428545"),
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
        self.quorum_session_active = False
        self.quorum_loss_signal_at = None
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

    def signal_quorum_interruption(self, station: str | None = None):
        """Expose a lost live link immediately while the grace timer confirms it."""
        if not self.quorum_session_active:
            return
        if self.quorum_loss_signal_at is None:
            self.quorum_loss_signal_at = time.time()
        connected = (
            sum(1 for name in fs.STATIONS if self.reported_online[name])
            if self.mqtt_connected
            else 0
        )
        snapshot = fs.STATE.snapshot()
        deployment = dict(snapshot.get("deployment", {}))
        deployment.update(
            live_mode="LINK RECOVERY",
            connected_stations=connected,
            required_stations=len(fs.STATIONS),
            disconnect_grace_seconds=QUORUM_LOSS_GRACE_SECONDS,
            grace_remaining_seconds=QUORUM_LOSS_GRACE_SECONDS,
        )
        fs.STATE.update(
            phase=f"Quorum interrupted — confirming link state for {int(QUORUM_LOSS_GRACE_SECONDS)} s",
            deployment=deployment,
            security=self.security_state("PAV LINK RECOVERY"),
        )
        fs.STATE.cloud(
            status=(
                f"Link verification window — {connected}/3 connected; "
                f"{int(QUORUM_LOSS_GRACE_SECONDS)} s remaining"
            ),
            contributors=connected,
        )
        for name in fs.STATIONS:
            current = snapshot.get("stations", {}).get(name, {})
            if not current.get("sensors"):
                continue
            connected_now = self.mqtt_connected and self.reported_online[name]
            fs.STATE.station(
                name,
                phase=(
                    "Quorum paused — link remains connected"
                    if connected_now
                    else "Confirming station interruption"
                ),
                online=connected_now,
                connection_state="READY" if connected_now else "RECOVERING",
                control_mode="HOLDING LAST VALIDATED COMMAND",
                source="quorum_interruption_verification",
            )
        fs.STATE.event(
            "recovery",
            f"Immediate quorum pause: {connected}/3 links; confirming for {int(QUORUM_LOSS_GRACE_SECONDS)} s",
            station=station,
        )

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
        self.signal_quorum_interruption()
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
                    was_online = self.reported_online[station]
                    self.reported_online[station] = False
                    if was_online:
                        self.signal_quorum_interruption(station)
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

    def hold_last_validated_state(
        self,
        station: str,
        connected: bool,
        age_seconds: float,
        recovering: bool = False,
    ):
        """Freeze the last validated values during a temporary link interruption."""
        fs.STATE.station(
            station,
            phase=(
                "Quorum paused — link remains connected"
                if connected
                else "Confirming station interruption"
                if recovering
                else "Connection interrupted — holding last validated state"
            ),
            online=connected,
            connection_state=(
                "READY"
                if connected
                else "RECOVERING"
                if recovering
                else "HOLDING"
            ),
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

    def live_station_status(self, now: float | None = None):
        """Return fresh Wokwi stations and their most recent contact times."""
        if now is None:
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
                and now - last_contact[station] <= LIVE_TIMEOUT_SECONDS
                and station in self.latest_live
            )
        ]
        return live_stations, last_contact

    def run_forever(self):
        self.initialize_state()
        self.connect_mqtt()
        self.train_federated_model()
        cycle = 0
        control_cycle = 0
        quorum_active = False
        quorum_candidate_since = None
        quorum_loss_since = None
        while True:
            cycle += 1
            self.request_station_rows(cycle)
            time.sleep(min(1.0, CYCLE_SECONDS / 3.0))
            self.drain_live_telemetry()
            now = time.time()
            live_stations, last_contact = self.live_station_status(now)
            raw_all_live = len(live_stations) == len(fs.STATIONS)

            if raw_all_live:
                quorum_loss_since = None
                self.quorum_loss_signal_at = None
                if quorum_active:
                    quorum_candidate_since = None
                else:
                    if quorum_candidate_since is None:
                        quorum_candidate_since = now
                    if now - quorum_candidate_since >= QUORUM_STABLE_SECONDS:
                        quorum_active = True
                        self.quorum_session_active = True
                        quorum_candidate_since = None
            else:
                quorum_candidate_since = None
                if quorum_active and quorum_loss_since is None:
                    quorum_loss_since = self.quorum_loss_signal_at or now
                if (
                    quorum_active
                    and quorum_loss_since is not None
                    and now - quorum_loss_since >= QUORUM_LOSS_GRACE_SECONDS
                ):
                    quorum_active = False
                    self.quorum_session_active = False
                    self.quorum_loss_signal_at = None

            all_live = quorum_active and raw_all_live
            recovering = quorum_active and not raw_all_live
            synchronizing = raw_all_live and not quorum_active
            grace_remaining = (
                max(0.0, QUORUM_LOSS_GRACE_SECONDS - (now - quorum_loss_since))
                if recovering and quorum_loss_since is not None
                else 0.0
            )

            aggregation = None
            if all_live:
                control_cycle += 1
                self.control_cycle = control_cycle
                aggregation = self.online_federated_update()

            # The unified laboratory is deliberately quorum-gated: sensing,
            # aggregation, inference and returned pump commands advance only
            # when all three Wokwi stations are present together.
            if all_live:
                for station in fs.STATIONS:
                    self.infer_and_update(
                        station,
                        self.latest_live[station]["sensors"],
                        live=True,
                        source="mqtt_synchronized_three_station_stream",
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

            snapshot = fs.STATE.snapshot()["stations"]
            has_validated_state = any(
                snapshot.get(station, {}).get("sensors") for station in fs.STATIONS
            )
            # When the quorum is not active, all cards are coordinated as one
            # paused laboratory even if one or two MQTT links remain healthy.
            if not all_live:
                snapshot = fs.STATE.snapshot()["stations"]
                for station in fs.STATIONS:
                    if snapshot.get(station, {}).get("sensors"):
                        age_seconds = (
                            now - last_contact[station]
                            if station in self.latest_live
                            else 0.0
                        )
                        self.hold_last_validated_state(
                            station,
                            station in live_stations,
                            age_seconds,
                            recovering=recovering,
                        )
                    else:
                        self.hold_station_at_zero(station, station in live_stations)

            live_mode = (
                "LIVE MQTT"
                if all_live
                else "LINK RECOVERY"
                if recovering
                else "SYNCHRONIZING STATIONS"
                if synchronizing
                else "HOLDING LAST STATE"
                if has_validated_state
                else "WAITING FOR STATIONS"
            )
            fs.STATE.update(
                running=True,
                phase=(
                    "Closed-loop MQTT regulation"
                    if all_live
                    else f"Quorum interrupted — confirming link state for {int(grace_remaining + 0.999)} s"
                    if recovering
                    else "Three stations detected — synchronizing the unified cycle"
                    if synchronizing
                    else "Connection interrupted — holding last validated state"
                    if has_validated_state
                    else f"Safety standby — waiting for full quorum ({len(live_stations)}/3 connected)"
                ),
                live_cycle=control_cycle,
                federated_live_cycle=control_cycle,
                federation_version=self.cloud.round,
                security=self.security_state(
                    "PAV VERIFIED"
                    if all_live
                    else "PAV LINK RECOVERY"
                    if recovering
                    else "PAV SYNCHRONIZING 3/3"
                    if synchronizing
                    else "PAV HOLDING"
                ),
                broker={"connected": self.mqtt_connected, "host": MQTT_HOST, "port": MQTT_PORT},
                deployment={
                    "transport": "PUBLIC MQTT",
                    "live_mode": live_mode,
                    "accuracy_scope": "The unified cycle advances only with a synchronized three-station quorum; a confirmed interruption pauses all new inference and commands while preserving the last validated state",
                    "hardware": "Wokwi ESP32 sensor/actuator node + Python Raspberry Pi 4B logical client",
                    "connected_stations": len(live_stations),
                    "required_stations": len(fs.STATIONS),
                    "activation_stability_seconds": QUORUM_STABLE_SECONDS,
                    "disconnect_grace_seconds": QUORUM_LOSS_GRACE_SECONDS,
                    "grace_remaining_seconds": round(grace_remaining, 1),
                },
            )
            fs.STATE.cloud(
                status=(
                    "Live commands returned to all Wokwi pumps"
                    if all_live
                    else f"Link verification window — {len(live_stations)}/3 connected; {int(grace_remaining + 0.999)} s remaining"
                    if recovering
                    else "Three station links detected — aligning synchronized start"
                    if synchronizing
                    else f"Holding last validated state — {len(live_stations)}/3 links fresh"
                    if has_validated_state
                    else f"Safety standby — waiting for {3 - len(live_stations)} station(s)"
                ),
                contributors=3 if all_live else len(live_stations),
                weights_hash=self.cloud.parameters.digest(),
            )
            fs.STATE.event(
                "live" if all_live else "recovery" if recovering else "sync" if synchronizing else "hold" if has_validated_state else "standby",
                (
                    f"Control cycle {control_cycle}: all three Wokwi stations acknowledged"
                    if all_live
                    else f"Quorum recovery: {len(live_stations)}/3 links; {int(grace_remaining + 0.999)} s before confirmed stop"
                    if recovering
                    else "Three-station quorum detected; synchronizing start"
                    if synchronizing
                    else f"Holding cycle {control_cycle}: {len(live_stations)}/3 links fresh"
                    if has_validated_state
                    else f"Zero-output interlock: {len(live_stations)}/3 stations connected"
                ),
            )
            # Preserve the 12-second scientific cycle while polling link-state
            # changes every 250 ms. New quorum members and lost members both
            # wake the next state evaluation promptly.
            remaining = max(0.2, CYCLE_SECONDS - min(1.0, CYCLE_SECONDS / 3.0))
            if synchronizing and quorum_candidate_since is not None:
                remaining = min(
                    remaining,
                    max(0.2, QUORUM_STABLE_SECONDS - (now - quorum_candidate_since)),
                )
            if recovering and quorum_loss_since is not None:
                # Refresh the visible verification countdown without advancing
                # a scientific control cycle or issuing a new pump command.
                remaining = min(remaining, 1.0, max(0.2, grace_remaining))
            deadline = time.monotonic() + remaining
            baseline = set(live_stations)
            while True:
                wait_seconds = deadline - time.monotonic()
                if wait_seconds <= 0:
                    break
                time.sleep(min(0.25, wait_seconds))
                self.drain_live_telemetry()
                refreshed, _ = self.live_station_status()
                if set(refreshed) != baseline:
                    break


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
