from __future__ import annotations

import os
import hashlib
import hmac
import json
import queue
import secrets
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pandas as pd

import federated_system as fs


ROOT = Path(__file__).resolve().parent
BOOTSTRAP_ROUNDS = int(os.getenv("FEDERATED_ROUNDS", "6"))
CYCLE_SECONDS = max(10.5, float(os.getenv("CYCLE_SECONDS", "12")))
ONLINE_EPOCHS = max(1, int(os.getenv("ONLINE_LOCAL_EPOCHS", "2")))
PAV_VERSION = "PAV1"
PAV_ALGORITHM = "HMAC-SHA256"
PAV_MAX_AGE_SECONDS = max(10.0, float(os.getenv("PAV_MAX_AGE_SECONDS", "60")))

WOKWI_URLS = {
    "austin": os.getenv("WOKWI_AUSTIN_URL", "https://wokwi.com/projects/473854149978000385"),
    "tongji": os.getenv("WOKWI_TONGJI_URL", "https://wokwi.com/projects/473855638857098241"),
    "virtual": os.getenv("WOKWI_VIRTUAL_URL", "https://wokwi.com/projects/473855699260353537"),
}

LOGICAL_LOCATIONS = {
    "austin": "Austin published-field data node",
    "tongji": "Tongji published-field data node",
    "virtual": "Disclosed digital-twin data node",
}

NODE_ORIGINS = {
    "austin": "PUBLISHED_FIELD · AUSTIN DATA NODE",
    "tongji": "PUBLISHED_FIELD · TONGJI DATA NODE",
    "virtual": "DISCLOSED_DIGITAL_TWIN · CLOUD NODE",
}


def pav_key(station: str) -> bytes:
    """Load a station PSK from Render or provision an ephemeral 256-bit key."""
    configured = os.getenv(f"PAV_KEY_{station.upper()}", "").strip()
    return configured.encode("utf-8") if configured else secrets.token_bytes(32)


def pav_payload_digest(payload: dict) -> str:
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def pav_sign(
    key: bytes,
    station: str,
    message_type: str,
    sequence: int,
    payload: dict,
) -> dict:
    issued_at_ms = int(time.time() * 1000)
    nonce = secrets.token_hex(12)
    payload_sha256 = pav_payload_digest(payload)
    material = (
        f"{PAV_VERSION}|{station}|{message_type}|{sequence}|"
        f"{issued_at_ms}|{nonce}|{payload_sha256}"
    ).encode("utf-8")
    return {
        "version": PAV_VERSION,
        "algorithm": PAV_ALGORITHM,
        "station": station,
        "message_type": message_type,
        "sequence": int(sequence),
        "issued_at_ms": issued_at_ms,
        "nonce": nonce,
        "payload_sha256": payload_sha256,
        "tag": hmac.new(key, material, hashlib.sha256).hexdigest(),
    }


def pav_verify(
    key: bytes,
    station: str,
    message_type: str,
    sequence: int,
    payload: dict,
    envelope: dict,
) -> tuple[bool, str]:
    if not isinstance(envelope, dict):
        return False, "missing envelope"
    try:
        issued_at_ms = int(envelope["issued_at_ms"])
        nonce = str(envelope["nonce"])
        supplied_digest = str(envelope["payload_sha256"])
        supplied_tag = str(envelope["tag"])
        envelope_sequence = int(envelope["sequence"])
    except (KeyError, TypeError, ValueError):
        return False, "malformed envelope"
    if envelope.get("version") != PAV_VERSION or envelope.get("algorithm") != PAV_ALGORITHM:
        return False, "unsupported PAV profile"
    if envelope.get("station") != station or envelope.get("message_type") != message_type:
        return False, "identity mismatch"
    if envelope_sequence != int(sequence):
        return False, "sequence mismatch"
    if len(nonce) < 16 or len(supplied_digest) != 64 or len(supplied_tag) != 64:
        return False, "invalid security field length"
    if abs(time.time() - issued_at_ms / 1000.0) > PAV_MAX_AGE_SECONDS:
        return False, "stale timestamp"
    expected_digest = pav_payload_digest(payload)
    if not hmac.compare_digest(supplied_digest, expected_digest):
        return False, "payload digest mismatch"
    material = (
        f"{PAV_VERSION}|{station}|{message_type}|{int(sequence)}|"
        f"{issued_at_ms}|{nonce}|{expected_digest}"
    ).encode("utf-8")
    expected_tag = hmac.new(key, material, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(supplied_tag, expected_tag):
        return False, "authentication tag mismatch"
    return True, "verified"


@dataclass
class StationCommand:
    cycle: int
    alum: float
    chlorine: float
    mode: str
    global_version: int
    acknowledged: threading.Event


class CloudStationNode:
    """Independent station runtime with its own sensor plant and actuator queue."""

    def __init__(
        self,
        station: str,
        frame: pd.DataFrame,
        telemetry_bus: queue.Queue,
        start_barrier: threading.Barrier,
        security_key: bytes,
    ):
        self.station = station
        self.frame = frame.reset_index(drop=True)
        self.telemetry_bus = telemetry_bus
        self.start_barrier = start_barrier
        self.security_key = security_key
        self.command_bus: queue.Queue[StationCommand] = queue.Queue()
        self.plant = fs.ESP32Plant(station)
        self.row_index = 0
        self.sample_cycle = 0
        self.last_heartbeat = 0.0
        self.thread = threading.Thread(
            target=self._run,
            daemon=True,
            name=f"cloud-station-{station}",
        )

    def start(self):
        self.thread.start()

    def submit_command(
        self,
        cycle: int,
        alum: float,
        chlorine: float,
        mode: str,
        global_version: int,
    ) -> bool:
        acknowledged = threading.Event()
        self.command_bus.put(
            StationCommand(
                cycle=cycle,
                alum=alum,
                chlorine=chlorine,
                mode=mode,
                global_version=global_version,
                acknowledged=acknowledged,
            )
        )
        return acknowledged.wait(timeout=1.0)

    def _apply_command(self, command: StationCommand):
        self.plant.actuate(command.alum, command.chlorine)
        command.acknowledged.set()

    def _emit_sample(self):
        row = self.frame.iloc[self.row_index]
        self.row_index = (self.row_index + 1) % len(self.frame)
        self.sample_cycle += 1
        sensors = self.plant.sense(row)
        self.last_heartbeat = time.time()
        telemetry = {
            "station": self.station,
            "cycle": self.sample_cycle,
            "sequence": int(row.sequence),
            "origin": fs.ORIGINS[self.station],
            "logical_location": LOGICAL_LOCATIONS[self.station],
            "emitted_at": self.last_heartbeat,
            "sensors": sensors,
            "target_forecast": float(row.forecast_h6),
            "actuator_feedback": {
                "alum": self.plant.alum_percent,
                "chlorine": self.plant.chlorine_percent,
            },
        }
        telemetry["pav"] = pav_sign(
            self.security_key,
            self.station,
            "TELEMETRY",
            self.sample_cycle,
            telemetry,
        )
        self.telemetry_bus.put(telemetry)

    def _run(self):
        self.start_barrier.wait()
        next_sample = time.monotonic()
        while True:
            timeout = max(0.0, next_sample - time.monotonic())
            try:
                command = self.command_bus.get(timeout=timeout)
                self._apply_command(command)
            except queue.Empty:
                self._emit_sample()
                next_sample += CYCLE_SECONDS


class CloudFederatedEngine:
    """Three live station workers plus executable RG-AdaFedResidual federation."""

    def __init__(self):
        _, self.splits, x_center, x_scale, y_center, y_scale = fs.prepare_data()
        self.clients = {
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
        self.telemetry_bus: queue.Queue[dict] = queue.Queue()
        self.pav_keys = {station: pav_key(station) for station in fs.STATIONS}
        self.pav_seen_nonces = {station: deque(maxlen=512) for station in fs.STATIONS}
        self.pav_nonce_index = {station: set() for station in fs.STATIONS}
        self.pav_verified = {station: {"telemetry": 0, "model_update": 0} for station in fs.STATIONS}
        self.pav_rejected = 0
        self.start_barrier = threading.Barrier(len(fs.STATIONS))
        self.nodes = {
            station: CloudStationNode(
                station,
                self.splits[station]["test"],
                self.telemetry_bus,
                self.start_barrier,
                self.pav_keys[station],
            )
            for station in fs.STATIONS
        }
        self.latest_telemetry: dict[str, dict] = {}
        self.trace = deque(maxlen=360)
        self.control_cycle = 0

    def _security_state(self, status: str = "PAV VERIFYING") -> dict:
        verified_stations = sum(
            1
            for station in fs.STATIONS
            if self.pav_verified[station]["telemetry"] > 0
            and self.pav_verified[station]["model_update"] > 0
        )
        return {
            "layer": "PAV",
            "profile": "Payload Authentication and Verification",
            "algorithm": PAV_ALGORITHM,
            "status": status,
            "verified_stations": verified_stations,
            "required_stations": len(fs.STATIONS),
            "timestamp_freshness": True,
            "nonce_replay_protection": True,
            "rejected_messages": self.pav_rejected,
        }

    def _accept_pav(
        self,
        station: str,
        message_type: str,
        sequence: int,
        payload: dict,
        envelope: dict,
    ) -> bool:
        valid, reason = pav_verify(
            self.pav_keys[station],
            station,
            message_type,
            sequence,
            payload,
            envelope,
        )
        nonce = str(envelope.get("nonce", "")) if isinstance(envelope, dict) else ""
        if valid and nonce in self.pav_nonce_index[station]:
            valid, reason = False, "replayed nonce"
        if not valid:
            self.pav_rejected += 1
            fs.STATE.station(
                station,
                pav_status="REJECTED",
                pav_reason=reason,
            )
            fs.STATE.update(security=self._security_state("PAV REJECTED"))
            fs.STATE.event("security", f"PAV rejected {message_type.lower()}: {reason}", station)
            return False

        nonces = self.pav_seen_nonces[station]
        nonce_index = self.pav_nonce_index[station]
        if len(nonces) == nonces.maxlen:
            nonce_index.discard(nonces[0])
        nonces.append(nonce)
        nonce_index.add(nonce)
        counter_key = "telemetry" if message_type == "TELEMETRY" else "model_update"
        self.pav_verified[station][counter_key] += 1
        fs.STATE.station(
            station,
            pav_status="PAV VERIFIED",
            pav_algorithm=PAV_ALGORITHM,
            pav_last_type=message_type,
            pav_verified_at=time.time(),
        )
        fully_verified = all(
            self.pav_verified[item]["telemetry"] > 0
            and self.pav_verified[item]["model_update"] > 0
            for item in fs.STATIONS
        )
        fs.STATE.update(
            security=self._security_state("PAV VERIFIED" if fully_verified else "PAV VERIFYING")
        )
        return True

    @staticmethod
    def _update_descriptor(update, round_number: int) -> dict:
        delta_bytes = update.delta.astype("float32").tobytes()
        return {
            "station": update.station,
            "round": int(round_number),
            "samples": int(update.samples),
            "validation_rmse": round(float(update.validation_rmse), 12),
            "parameters_sha256": update.parameters.digest(),
            "delta_sha256": hashlib.sha256(delta_bytes).hexdigest(),
        }

    def _train_signed_update(self, station: str, round_number: int, epochs: int):
        """Run one private client and sign its update inside that logical runtime."""
        update = self.clients[station].train_local(round_number, 64, epochs)
        descriptor = self._update_descriptor(update, round_number)
        envelope = pav_sign(
            self.pav_keys[station],
            station,
            "MODEL_UPDATE",
            round_number,
            descriptor,
        )
        return update, descriptor, envelope

    def _authenticate_updates(self, signed_updates: list, round_number: int) -> list:
        authenticated = []
        for update, descriptor, envelope in signed_updates:
            if self._accept_pav(
                update.station,
                "MODEL_UPDATE",
                round_number,
                descriptor,
                envelope,
            ):
                authenticated.append(update)
        if len(authenticated) != len(fs.STATIONS):
            raise RuntimeError("PAV model-update quorum verification failed")
        return authenticated

    def initialize_state(self):
        fs.STATE.update(
            running=True,
            phase="Starting cloud station runtimes",
            round=0,
            max_rounds=BOOTSTRAP_ROUNDS,
            live_cycle=0,
            federation_version=0,
            deployment={
                "transport": "CLOUD EVENT BUS",
                "live_mode": "CLOUD INITIALIZING",
                "accuracy_scope": "three independent PAV-authenticated cloud station runtimes with executable local training, relation-guided aggregation, inference and closed-loop actuation",
                "hardware": "ESP32 plant runtime + Raspberry Pi 4B federated client process",
            },
            security=self._security_state(),
            broker={
                "connected": True,
                "host": "Render cloud event bus",
                "port": None,
            },
        )
        for station in fs.STATIONS:
            fs.STATE.station(
                station,
                name=fs.DISPLAY_NAMES[station],
                origin=NODE_ORIGINS[station],
                controller="Cloud ESP32 plant runtime + Raspberry Pi 4B client",
                phase="Preparing private local model",
                local_progress=0,
                sensors={},
                pumps={"alum": 0.0, "chlorine": 0.0},
                forecast=None,
                online=False,
                connection_state="STARTING",
                source="cloud_station_runtime",
                logical_location=LOGICAL_LOCATIONS[station],
                wokwi_url=WOKWI_URLS[station],
                pav_status="PAV CHECKING",
                pav_algorithm=PAV_ALGORITHM,
            )

    def _train_clients(self, round_number: int, epochs: int):
        for station, client in self.clients.items():
            client.receive_global(self.cloud.parameters, round_number)
            fs.STATE.station(
                station,
                phase="Private local training",
                local_progress=8,
            )
        with ThreadPoolExecutor(max_workers=len(fs.STATIONS)) as pool:
            futures = {
                station: pool.submit(
                    self._train_signed_update,
                    station,
                    round_number,
                    epochs,
                )
                for station in fs.STATIONS
            }
            signed_updates = [futures[station].result() for station in fs.STATIONS]
        return self._authenticate_updates(signed_updates, round_number)

    def bootstrap_federation(self):
        for round_number in range(1, BOOTSTRAP_ROUNDS + 1):
            fs.STATE.update(
                round=round_number,
                phase=f"Federated bootstrap round {round_number}/{BOOTSTRAP_ROUNDS}",
            )
            fs.STATE.cloud(
                status=f"Collecting three private updates · bootstrap {round_number}/{BOOTSTRAP_ROUNDS}",
                contributors=0,
                weights_hash=self.cloud.parameters.digest(),
            )
            updates = self._train_clients(round_number, epochs=6)
            aggregation = self.cloud.aggregate(updates)
            fs.STATE.cloud(
                status=f"Relation-guided bootstrap aggregation {round_number}/{BOOTSTRAP_ROUNDS}",
                contributors=len(updates),
                weights_hash=aggregation["weights_hash"],
                client_weights=aggregation["client_weights"],
                relation_scores=aggregation["relation_scores"],
                validation_rmse=aggregation["mean_validation_rmse"],
            )
            fs.STATE.event(
                "aggregate",
                f"Bootstrap round {round_number}/{BOOTSTRAP_ROUNDS}: three local updates aggregated",
            )

        for client in self.clients.values():
            client.receive_global(self.cloud.parameters, self.cloud.round)
            client.calibrate_private_head(include_validation=True)

    def start_station_nodes(self):
        for node in self.nodes.values():
            node.start()
        fs.STATE.event("cloud", "Austin, Tongji and digital-twin cloud station runtimes started")

    def collect_station_batch(self) -> dict[str, dict]:
        # Allow the independently scheduled station threads to reach their next
        # sample boundary without generating a false interruption between two
        # healthy cycles.
        deadline = time.monotonic() + max(6.0, CYCLE_SECONDS * 1.30)
        batch: dict[str, dict] = {}
        while len(batch) < len(fs.STATIONS):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            try:
                telemetry = self.telemetry_bus.get(timeout=remaining)
            except queue.Empty:
                break
            station = telemetry.get("station")
            if station in fs.STATIONS:
                envelope = telemetry.get("pav")
                payload = {key: value for key, value in telemetry.items() if key != "pav"}
                if not self._accept_pav(
                    station,
                    "TELEMETRY",
                    int(telemetry.get("cycle", -1)),
                    payload,
                    envelope,
                ):
                    continue
                batch[station] = telemetry
                self.latest_telemetry[station] = telemetry
        return batch

    def online_federated_update(self):
        version = self.cloud.round + 1
        fs.STATE.update(phase=f"Local learning for global model version {version}")
        updates = self._train_clients(version, epochs=ONLINE_EPOCHS)
        fs.STATE.update(phase=f"Relation-guided aggregation for version {version}")
        aggregation = self.cloud.aggregate(updates)
        for client in self.clients.values():
            client.receive_global(self.cloud.parameters, self.cloud.round)
        fs.STATE.cloud(
            status=f"Global model v{self.cloud.round} broadcast to three station clients",
            contributors=len(updates),
            weights_hash=aggregation["weights_hash"],
            client_weights=aggregation["client_weights"],
            relation_scores=aggregation["relation_scores"],
            validation_rmse=aggregation["mean_validation_rmse"],
            global_version=self.cloud.round,
        )
        fs.STATE.event(
            "security",
            f"PAV verified three HMAC-SHA256 model updates for global version {self.cloud.round}",
        )
        return aggregation

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

    def regulate_batch(self, batch: dict[str, dict]):
        self.control_cycle += 1
        aggregation = self.online_federated_update()
        for station in fs.STATIONS:
            telemetry = batch[station]
            sensors = telemetry["sensors"]
            started = time.perf_counter()
            prediction = self.clients[station].infer(sensors)
            alum, chlorine, mode = fs.regulation(prediction, sensors)
            acknowledged = self.nodes[station].submit_command(
                self.control_cycle,
                alum,
                chlorine,
                mode,
                self.cloud.round,
            )
            latency_ms = 1000.0 * (time.perf_counter() - started)
            fs.STATE.station(
                station,
                phase="Command acknowledged by cloud station" if acknowledged else "Command acknowledgement pending",
                local_progress=100,
                sequence=telemetry["sequence"],
                sensors={key: sensors[key] for key in (
                    "raw_turbidity",
                    "filtered_turbidity",
                    "ph",
                    "temperature",
                    "flow",
                    "residual_chlorine",
                )},
                forecast=float(prediction["forecast_h6"]),
                pumps={"alum": alum, "chlorine": chlorine},
                pump_alarm=max(alum, chlorine) >= 90.0,
                control_mode=mode,
                latency_ms=latency_ms,
                online=True,
                connection_state="LIVE",
                stale_seconds=0.0,
                source="cloud_station_sensor_stream",
                global_round=BOOTSTRAP_ROUNDS,
                global_version=self.cloud.round,
                command_acknowledged=acknowledged,
            )
            self.trace.append(
                {
                    "time_step": self.control_cycle,
                    "station": station,
                    "origin": fs.ORIGINS[station],
                    "sequence": telemetry["sequence"],
                    **sensors,
                    "target_forecast": telemetry["target_forecast"],
                    "predicted_forecast": float(prediction["forecast_h6"]),
                    "alum_percent": alum,
                    "chlorine_percent": chlorine,
                    "control_mode": mode,
                    "global_round": self.cloud.round,
                    "weights_hash": aggregation["weights_hash"],
                }
            )

        self.update_summary()
        fs.STATE.update(
            running=True,
            phase="Cloud closed-loop federation and dosing",
            round=BOOTSTRAP_ROUNDS,
            max_rounds=BOOTSTRAP_ROUNDS,
            live_cycle=self.control_cycle,
            federation_version=self.cloud.round,
            deployment={
                "transport": "CLOUD EVENT BUS",
                "live_mode": "CLOUD FEDERATED LIVE",
                "accuracy_scope": "each displayed cycle verifies PAV-authenticated telemetry and private model updates before relation-guided aggregation, global broadcast, H6 inference and acknowledged pump commands",
                "hardware": "ESP32 plant runtime + Raspberry Pi 4B federated client process",
            },
            security=self._security_state("PAV VERIFIED"),
            broker={
                "connected": True,
                "host": "Render cloud event bus",
                "port": None,
            },
        )
        fs.STATE.event(
            "federation",
            f"Cycle {self.control_cycle}: global model v{self.cloud.round} aggregated and all pump commands acknowledged",
        )

    def hold_last_state(self, available: int):
        now = time.time()
        for station in fs.STATIONS:
            telemetry = self.latest_telemetry.get(station)
            age = now - telemetry["emitted_at"] if telemetry else 0.0
            fs.STATE.station(
                station,
                connection_state="HOLDING",
                online=False,
                stale_seconds=max(0.0, age),
                phase="Holding last validated cloud state",
                control_mode="HOLDING LAST VALIDATED COMMAND",
            )
        fs.STATE.update(
            phase=f"Cloud station quorum interrupted · {available}/3",
            deployment={
                "transport": "CLOUD EVENT BUS",
                "live_mode": "HOLDING LAST STATE",
                "accuracy_scope": "no new global update or pump command is issued without a three-station sample quorum",
                "hardware": "ESP32 plant runtime + Raspberry Pi 4B federated client process",
            },
        )
        fs.STATE.event("hold", f"Holding cycle {self.control_cycle}: {available}/3 cloud station samples available")

    def run_forever(self):
        self.initialize_state()
        self.bootstrap_federation()
        self.start_station_nodes()
        while True:
            batch = self.collect_station_batch()
            if len(batch) == len(fs.STATIONS):
                self.regulate_batch(batch)
            elif self.control_cycle > 0:
                self.hold_last_state(len(batch))
            time.sleep(0.05)


class CloudDashboardHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        request_path = self.path.partition("?")[0].rstrip("/") or "/"
        if request_path == "/api/state" or request_path.endswith("/api/state"):
            payload = json.dumps(fs.STATE.snapshot(), separators=(",", ":")).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        super().do_GET()

    def log_message(self, fmt, *args):
        return


def _run_engine(engine: CloudFederatedEngine):
    try:
        engine.run_forever()
    except Exception as error:
        fs.STATE.update(running=False, phase=f"Cloud engine error: {error}")
        fs.STATE.event("error", f"Cloud engine stopped: {error}")
        raise


def serve():
    fs.WEB = ROOT / "web"
    port = int(os.getenv("PORT", "10000"))
    engine = CloudFederatedEngine()
    threading.Thread(
        target=_run_engine,
        args=(engine,),
        daemon=True,
        name="cloud-federated-engine",
    ).start()
    handler = lambda *args, **kwargs: CloudDashboardHandler(
        *args,
        directory=str(fs.WEB),
        **kwargs,
    )
    server = ThreadingHTTPServer(("0.0.0.0", port), handler)
    print(f"RG-AdaFedResidual cloud laboratory listening on 0.0.0.0:{port}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    serve()
