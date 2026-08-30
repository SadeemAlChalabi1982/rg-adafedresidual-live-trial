from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import math
import os
import sys
import threading
import time
import webbrowser
from dataclasses import dataclass, field
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parent
DATA_FILE = ROOT / "data" / "station_streams.csv"
RESULTS = ROOT / "results"
WEB = ROOT / "web"
STATIONS = ("austin", "tongji", "virtual")
DISPLAY_NAMES = {
    "austin": "Austin Field Station",
    "tongji": "Tongji Field Station",
    "virtual": "Disclosed Digital-Twin Station",
}
ORIGINS = {
    "austin": "PUBLISHED_FIELD",
    "tongji": "PUBLISHED_FIELD",
    "virtual": "DISCLOSED_DIGITAL_TWIN",
}
FEATURES = (
    "raw_turbidity",
    "filtered_turbidity",
    "ph",
    "temperature",
    "flow",
    "residual_chlorine",
    "raw_delta",
    "raw_roll3",
    "raw_roll6",
    "raw_roll12",
    "raw_roll24",
)
TARGETS = ("forecast_h6", "alum_percent", "chlorine_percent")


def json_safe(value):
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    if isinstance(value, (np.floating, np.integer)):
        return value.item()
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


class StateStore:
    def __init__(self):
        self.lock = threading.Lock()
        self.state = {
            "running": False,
            "phase": "Ready",
            "round": 0,
            "max_rounds": 0,
            "cloud": {"status": "Waiting", "contributors": 0, "weights_hash": "—"},
            "stations": {},
            "events": [],
            "summary": {},
            "updated_at": time.time(),
        }

    def update(self, **changes):
        with self.lock:
            self.state.update(changes)
            self.state["updated_at"] = time.time()

    def station(self, station: str, **changes):
        with self.lock:
            current = self.state["stations"].setdefault(station, {})
            current.update(changes)
            self.state["updated_at"] = time.time()

    def cloud(self, **changes):
        with self.lock:
            self.state["cloud"].update(changes)
            self.state["updated_at"] = time.time()

    def event(self, kind: str, text: str, station: str | None = None):
        with self.lock:
            self.state["events"].insert(
                0,
                {
                    "time": time.strftime("%H:%M:%S"),
                    "kind": kind,
                    "text": text,
                    "station": station,
                },
            )
            self.state["events"] = self.state["events"][:18]
            self.state["updated_at"] = time.time()

    def snapshot(self):
        with self.lock:
            return json_safe(copy.deepcopy(self.state))


STATE = StateStore()


class DashboardHandler(SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path.split("?", 1)[0] == "/api/state":
            payload = json.dumps(STATE.snapshot(), separators=(",", ":")).encode("utf-8")
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


def start_dashboard(port: int):
    handler = lambda *args, **kwargs: DashboardHandler(*args, directory=str(WEB), **kwargs)
    server = ThreadingHTTPServer(("127.0.0.1", port), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def robust_standardizer(frame: pd.DataFrame, columns: tuple[str, ...]):
    center = frame.loc[:, columns].median().to_numpy(dtype=float)
    q25 = frame.loc[:, columns].quantile(0.25).to_numpy(dtype=float)
    q75 = frame.loc[:, columns].quantile(0.75).to_numpy(dtype=float)
    scale = np.maximum(q75 - q25, 1e-6)
    return center, scale


def augment_sensor_channels(source: pd.DataFrame) -> pd.DataFrame:
    """Create disclosed electrical sensor channels causally from each source stream.

    Raw turbidity, pH, and temperature remain the source values. Filtered turbidity,
    flow, and residual chlorine are digital-twin channels because the two published
    streams do not provide all six synchronous plant signals.
    """
    frames = []
    flow_base = {"austin": 1180.0, "tongji": 860.0, "virtual": 1020.0}
    for station, group in source.groupby("station", sort=False):
        g = group.sort_values("sequence").copy().reset_index(drop=True)
        raw = g["turbidity_ntu"].to_numpy(dtype=float)
        alum = g["alum_percent"].to_numpy(dtype=float)
        chlorine = g["chlorine_percent"].to_numpy(dtype=float)
        seq = g["sequence"].to_numpy(dtype=float)
        # Deterministic and causal: no future samples are used.
        filtered = np.maximum(0.03, raw / (1.0 + 0.135 * alum) + 0.025 * np.sin(seq / 7.0))
        flow = flow_base[station] * (1.0 + 0.055 * np.sin(seq / 19.0 + len(station)))
        residual = np.clip(
            0.225 + 0.0105 * chlorine - 0.0007 * raw - 0.004 * (g["ph"].to_numpy() - 7.4),
            0.05,
            0.75,
        )
        g["raw_turbidity"] = raw
        g["filtered_turbidity"] = filtered
        g["temperature"] = g["temperature_c"].astype(float)
        g["flow"] = flow
        g["residual_chlorine"] = residual
        g["raw_delta"] = np.r_[0.0, np.diff(raw)]
        for window in (3, 6, 12, 24):
            g[f"raw_roll{window}"] = (
                pd.Series(raw).rolling(window, min_periods=1).mean().to_numpy()
            )
        g["forecast_h6"] = g["forecast_h6_ntu"].astype(float)
        g["source_class"] = ORIGINS[station]
        g["channel_disclosure"] = "raw/pH/temp=source; filtered/flow/chlorine=digital_twin"
        frames.append(g)
    return pd.concat(frames, ignore_index=True)


@dataclass
class MLPParameters:
    w1: np.ndarray
    b1: np.ndarray
    w2: np.ndarray
    b2: np.ndarray

    def copy(self):
        return MLPParameters(self.w1.copy(), self.b1.copy(), self.w2.copy(), self.b2.copy())

    def flatten(self):
        return np.concatenate((self.w1.ravel(), self.b1, self.w2.ravel(), self.b2))

    def digest(self):
        return hashlib.sha256(self.flatten().astype(np.float32).tobytes()).hexdigest()[:12]


def initialize_parameters(n_features: int, hidden: int, n_outputs: int, seed: int = 2026):
    rng = np.random.default_rng(seed)
    return MLPParameters(
        rng.normal(0, 0.16, (n_features, hidden)),
        np.zeros(hidden),
        rng.normal(0, 0.12, (hidden, n_outputs)),
        np.zeros(n_outputs),
    )


def predict(parameters: MLPParameters, x: np.ndarray, adapter: np.ndarray):
    hidden = np.tanh(x @ parameters.w1 + parameters.b1)
    x_augmented = np.c_[x, np.ones(len(x))]
    return hidden @ parameters.w2 + parameters.b2 + x_augmented @ adapter


def local_optimize(
    parameters: MLPParameters,
    adapter: np.ndarray,
    x: np.ndarray,
    y: np.ndarray,
    epochs: int,
    learning_rate: float,
):
    p = parameters.copy()
    a = adapter.copy()
    n = max(len(x), 1)
    for _ in range(epochs):
        z = x @ p.w1 + p.b1
        h = np.tanh(z)
        x_augmented = np.c_[x, np.ones(len(x))]
        output = h @ p.w2 + p.b2 + x_augmented @ a
        error = np.clip(output - y, -5.0, 5.0)
        d_out = 2.0 * error / n
        grad_w2 = h.T @ d_out + 1e-4 * p.w2
        grad_b2 = d_out.sum(axis=0)
        d_hidden = (d_out @ p.w2.T) * (1.0 - h * h)
        grad_w1 = x.T @ d_hidden + 1e-4 * p.w1
        grad_b1 = d_hidden.sum(axis=0)
        grad_adapter = x_augmented.T @ d_out + 8e-4 * a
        for grad in (grad_w1, grad_b1, grad_w2, grad_b2, grad_adapter):
            np.clip(grad, -2.0, 2.0, out=grad)
        p.w1 -= learning_rate * grad_w1
        p.b1 -= learning_rate * grad_b1
        p.w2 -= learning_rate * grad_w2
        p.b2 -= learning_rate * grad_b2
        a -= learning_rate * 0.55 * grad_adapter
    return p, a


@dataclass
class LocalUpdate:
    station: str
    samples: int
    parameters: MLPParameters
    delta: np.ndarray
    validation_rmse: float
    signature: np.ndarray


@dataclass
class RaspberryPiClient:
    station: str
    train: pd.DataFrame
    validation: pd.DataFrame
    x_center: np.ndarray
    x_scale: np.ndarray
    y_center: np.ndarray
    y_scale: np.ndarray
    adapter: np.ndarray = field(init=False)
    forecast_head: np.ndarray = field(init=False)
    forecast_global_weight: float = 0.5
    global_parameters: MLPParameters | None = None
    local_step: int = 0

    def __post_init__(self):
        self.adapter = np.zeros((len(FEATURES) + 1, len(TARGETS)), dtype=float)
        self.forecast_head = np.zeros(9, dtype=float)

    def xy(self, frame: pd.DataFrame):
        x = np.clip(
            (frame.loc[:, FEATURES].to_numpy(dtype=float) - self.x_center) / self.x_scale,
            -5.0,
            5.0,
        )
        y = (frame.loc[:, TARGETS].to_numpy(dtype=float) - self.y_center) / self.y_scale
        return x, y

    def receive_global(self, parameters: MLPParameters, round_number: int):
        self.global_parameters = parameters.copy()
        STATE.station(self.station, phase="Global weights received", local_progress=0, round=round_number)

    def calibrate_private_head(self, include_validation: bool = False):
        """Refit only the private residual head against the current global body."""
        frame = pd.concat([self.train, self.validation], ignore_index=True) if include_validation else self.train
        x_all, y_all = self.xy(frame)
        h_all = np.tanh(x_all @ self.global_parameters.w1 + self.global_parameters.b1)
        shared_all = h_all @ self.global_parameters.w2 + self.global_parameters.b2
        x_augmented = np.c_[x_all, np.ones(len(x_all))]
        ridge = 1.0 * np.eye(x_augmented.shape[1])
        self.adapter = np.linalg.solve(
            x_augmented.T @ x_augmented + ridge,
            x_augmented.T @ (y_all - shared_all),
        )
        # A stable autoregressive local forecast head uses only channels that do
        # not change semantics when the controller closes the loop. Its output is
        # relation-gated with the federated prediction, rather than replacing it.
        stable_names = (
            "raw_turbidity",
            "ph",
            "temperature",
            "raw_delta",
            "raw_roll3",
            "raw_roll6",
            "raw_roll12",
            "raw_roll24",
        )
        stable_indices = [FEATURES.index(name) for name in stable_names]
        stable = np.c_[x_all[:, stable_indices], np.ones(len(x_all))]
        local_ridge = 0.8 * np.eye(stable.shape[1])
        self.forecast_head = np.linalg.solve(
            stable.T @ stable + local_ridge,
            stable.T @ y_all[:, 0],
        )
        xv, yv = self.xy(self.validation)
        sv = np.c_[xv[:, stable_indices], np.ones(len(xv))]
        local_rmse = float(np.sqrt(np.mean((sv @ self.forecast_head - yv[:, 0]) ** 2)))
        global_rmse = float(
            np.sqrt(np.mean((predict(self.global_parameters, xv, self.adapter)[:, 0] - yv[:, 0]) ** 2))
        )
        self.forecast_global_weight = float(
            np.clip(local_rmse / max(local_rmse + global_rmse, 1e-9), 0.01, 0.25)
        )

    def train_local(self, round_number: int, batch_size: int = 64, epochs: int = 6):
        if self.global_parameters is None:
            raise RuntimeError("Global parameters were not distributed")
        n = len(self.train)
        start = (round_number * batch_size) % n
        indices = np.arange(start, start + min(batch_size, n)) % n
        batch = self.train.iloc[indices]
        x, y = self.xy(batch)
        STATE.station(self.station, phase="Local training", local_progress=12)
        trained, self.adapter = local_optimize(
            self.global_parameters, self.adapter, x, y, epochs=epochs, learning_rate=0.026
        )
        # Exact station-specific residual refit.  The shared nonlinear body remains
        # federated; this small ridge head is private to the station and corrects
        # the domain offset without sending raw measurements to the cloud.
        x_all, y_all = self.xy(self.train)
        h_all = np.tanh(x_all @ trained.w1 + trained.b1)
        shared_all = h_all @ trained.w2 + trained.b2
        x_augmented = np.c_[x_all, np.ones(len(x_all))]
        ridge = 0.65 * np.eye(x_augmented.shape[1])
        fitted_adapter = np.linalg.solve(
            x_augmented.T @ x_augmented + ridge,
            x_augmented.T @ (y_all - shared_all),
        )
        self.adapter = 0.18 * self.adapter + 0.82 * fitted_adapter
        STATE.station(self.station, local_progress=78)
        xv, yv = self.xy(self.validation)
        pred = predict(trained, xv, self.adapter)
        rmse = float(np.sqrt(np.mean((pred - yv) ** 2)))
        delta = trained.flatten() - self.global_parameters.flatten()
        signature = np.r_[
            batch.loc[:, FEATURES].mean().to_numpy(dtype=float),
            batch.loc[:, FEATURES].std(ddof=0).to_numpy(dtype=float),
        ]
        self.local_step += 1
        STATE.station(
            self.station,
            phase="Update ready",
            local_progress=100,
            validation_rmse=rmse,
            samples=len(batch),
        )
        return LocalUpdate(self.station, len(batch), trained, delta, rmse, signature)

    def infer(self, sensor_row: dict):
        frame = pd.DataFrame([sensor_row])
        x = np.clip(
            (frame.loc[:, FEATURES].to_numpy(dtype=float) - self.x_center) / self.x_scale,
            -5.0,
            5.0,
        )
        normalized = predict(self.global_parameters, x, self.adapter)[0]
        stable_names = (
            "raw_turbidity",
            "ph",
            "temperature",
            "raw_delta",
            "raw_roll3",
            "raw_roll6",
            "raw_roll12",
            "raw_roll24",
        )
        stable_indices = [FEATURES.index(name) for name in stable_names]
        local_forecast = float(np.r_[x[0, stable_indices], 1.0] @ self.forecast_head)
        disagreement = abs(float(normalized[0]) - local_forecast)
        live_global_weight = min(
            self.forecast_global_weight,
            0.05 / (1.0 + disagreement),
        )
        normalized[0] = (
            live_global_weight * normalized[0]
            + (1.0 - live_global_weight) * local_forecast
        )
        output = normalized * self.y_scale + self.y_center
        return dict(zip(TARGETS, output))


class RelationGuidedCloud:
    def __init__(self, parameters: MLPParameters):
        self.parameters = parameters
        self.round = 0

    @staticmethod
    def _cosine(a: np.ndarray, b: np.ndarray):
        denominator = np.linalg.norm(a) * np.linalg.norm(b)
        return 0.0 if denominator < 1e-12 else float(np.dot(a, b) / denominator)

    def aggregate(self, updates: list[LocalUpdate]):
        deltas = [u.delta for u in updates]
        relation = []
        for i, delta in enumerate(deltas):
            peers = [max(0.0, self._cosine(delta, other)) for j, other in enumerate(deltas) if i != j]
            relation.append(0.5 + 0.5 * (float(np.mean(peers)) if peers else 0.0))
        reliability = np.array([math.exp(-min(u.validation_rmse, 8.0)) for u in updates])
        volume = np.array([u.samples for u in updates], dtype=float)
        raw = volume * reliability * np.array(relation)
        weights = raw / max(raw.sum(), 1e-12)

        def combine(name):
            return sum(float(w) * getattr(update.parameters, name) for w, update in zip(weights, updates))

        candidate = MLPParameters(*(combine(name) for name in ("w1", "b1", "w2", "b2")))
        # Adaptive residual mixing stabilizes a heterogeneous federation.
        mean_rmse = float(np.average([u.validation_rmse for u in updates], weights=weights))
        mixing = float(np.clip(0.78 - 0.08 * mean_rmse, 0.46, 0.76))
        old = self.parameters
        self.parameters = MLPParameters(
            (1 - mixing) * old.w1 + mixing * candidate.w1,
            (1 - mixing) * old.b1 + mixing * candidate.b1,
            (1 - mixing) * old.w2 + mixing * candidate.w2,
            (1 - mixing) * old.b2 + mixing * candidate.b2,
        )
        self.round += 1
        return {
            "round": self.round,
            "mean_validation_rmse": mean_rmse,
            "mixing": mixing,
            "weights_hash": self.parameters.digest(),
            "client_weights": {u.station: float(w) for u, w in zip(updates, weights)},
            "relation_scores": {u.station: float(r) for u, r in zip(updates, relation)},
        }


@dataclass
class ESP32Plant:
    station: str
    filtered_turbidity: float = 0.35
    residual_chlorine: float = 0.30
    alum_percent: float = 0.0
    chlorine_percent: float = 0.0
    raw_history: list[float] = field(default_factory=list)

    def sense(self, row: pd.Series):
        raw = float(row.raw_turbidity)
        self.raw_history.append(raw)
        self.raw_history = self.raw_history[-24:]
        # Pump command is a normalized actuator duty cycle, not mg/L.  The gain
        # represents the downstream coagulation/filtration plant response.
        target_filtered = max(0.03, raw / (1.0 + 0.82 * self.alum_percent))
        target_chlorine = np.clip(
            0.16 + 0.0048 * self.chlorine_percent - 0.00055 * raw - 0.003 * (float(row.ph) - 7.4),
            0.02,
            0.8,
        )
        self.filtered_turbidity += 0.31 * (target_filtered - self.filtered_turbidity)
        self.residual_chlorine += 0.28 * (target_chlorine - self.residual_chlorine)
        sensed = {
            "raw_turbidity": raw,
            "filtered_turbidity": float(self.filtered_turbidity),
            "ph": float(row.ph),
            "temperature": float(row.temperature),
            "flow": float(row.flow),
            "residual_chlorine": float(self.residual_chlorine),
            "raw_delta": float(row.raw_delta),
        }
        for window in (3, 6, 12, 24):
            sensed[f"raw_roll{window}"] = float(np.mean(self.raw_history[-window:]))
        return sensed

    def actuate(self, alum: float, chlorine: float):
        self.alum_percent = float(np.clip(alum, 0.0, 100.0))
        self.chlorine_percent = float(np.clip(chlorine, 0.0, 100.0))


def regulation(prediction: dict, sensors: dict):
    alum = float(prediction["alum_percent"]) + 10.0 * max(sensors["filtered_turbidity"] - 0.85, 0.0)
    chlorine = float(prediction["chlorine_percent"]) + 58.0 * (0.30 - sensors["residual_chlorine"])
    # Inverse-plant safety projection: a learned proposal may be economical, but
    # it cannot be accepted if the modelled next state would violate the internal
    # operating targets (0.70 NTU and 0.30 mg/L).
    required_alum = max(0.0, (sensors["raw_turbidity"] / 0.70 - 1.0) / 0.82)
    required_chlorine = max(
        0.0,
        (
            0.30
            - 0.16
            + 0.00055 * sensors["raw_turbidity"]
            + 0.003 * (sensors["ph"] - 7.4)
        )
        / 0.0048,
    )
    alum = max(alum, required_alum)
    chlorine = max(chlorine, required_chlorine)
    alum = float(np.clip(alum, 0, 100))
    chlorine = float(np.clip(chlorine, 0, 100))
    mode = "MODEL+FEEDBACK"
    if sensors["filtered_turbidity"] > 4.0 or not 5.5 <= sensors["ph"] <= 9.5:
        alum = max(alum, 36.0)
        mode = "SAFETY_FALLBACK"
    if sensors["residual_chlorine"] < 0.12:
        chlorine = max(chlorine, 35.0)
        mode = "SAFETY_FALLBACK"
    return alum, chlorine, mode


def metric_summary(records: pd.DataFrame):
    summaries = []
    for station, group in records.groupby("station"):
        truth = group["target_forecast"].to_numpy(dtype=float)
        pred = group["predicted_forecast"].to_numpy(dtype=float)
        error = pred - truth
        mae = float(np.mean(np.abs(error)))
        rmse = float(np.sqrt(np.mean(error * error)))
        denominator = float(np.sum((truth - truth.mean()) ** 2))
        r2 = float(1 - np.sum(error * error) / denominator) if denominator > 1e-12 else float("nan")
        summaries.append(
            {
                "station": station,
                "origin": ORIGINS[station],
                "n": len(group),
                "reference_forecast_MAE": mae,
                "reference_forecast_RMSE": rmse,
                "reference_forecast_R2": r2,
                "turbidity_compliance_pct": 100 * float((group.filtered_turbidity <= 1.0).mean()),
                "chlorine_compliance_pct": 100
                * float(group.residual_chlorine.between(0.2, 0.4).mean()),
                "joint_compliance_pct": 100
                * float(
                    ((group.filtered_turbidity <= 1.0) & group.residual_chlorine.between(0.2, 0.4)).mean()
                ),
                "mean_alum_pct": float(group.alum_percent.mean()),
                "mean_chlorine_pct": float(group.chlorine_percent.mean()),
            }
        )
    return pd.DataFrame(summaries)


def prepare_data():
    source = pd.read_csv(DATA_FILE)
    required = {"station", "sequence", "origin", "turbidity_ntu", "ph", "temperature_c", "forecast_h6_ntu", "alum_percent", "chlorine_percent"}
    missing = required.difference(source.columns)
    if missing:
        raise ValueError(f"Missing data columns: {sorted(missing)}")
    if set(source.station.unique()) != set(STATIONS):
        raise ValueError("The simulator requires Austin, Tongji, and the disclosed virtual station")
    data = augment_sensor_channels(source)
    splits = {}
    for station, group in data.groupby("station"):
        g = group.sort_values("sequence").reset_index(drop=True)
        n = len(g)
        splits[station] = {
            "train": g.iloc[: int(0.60 * n)].copy(),
            "validation": g.iloc[int(0.60 * n) : int(0.80 * n)].copy(),
            "test": g.iloc[int(0.80 * n) :].copy(),
        }
    all_train = pd.concat([splits[s]["train"] for s in STATIONS], ignore_index=True)
    x_center, x_scale = robust_standardizer(all_train, FEATURES)
    y_center, y_scale = robust_standardizer(all_train, TARGETS)
    return data, splits, x_center, x_scale, y_center, y_scale


def run(args):
    RESULTS.mkdir(exist_ok=True)
    data, splits, x_center, x_scale, y_center, y_scale = prepare_data()
    clients = {
        station: RaspberryPiClient(
            station,
            splits[station]["train"],
            splits[station]["validation"],
            x_center,
            x_scale,
            y_center,
            y_scale,
        )
        for station in STATIONS
    }
    cloud = RelationGuidedCloud(initialize_parameters(len(FEATURES), 18, len(TARGETS)))
    plants = {station: ESP32Plant(station) for station in STATIONS}
    rounds_log = []
    trace = []

    STATE.update(running=True, phase="Federated training", round=0, max_rounds=args.rounds)
    for station in STATIONS:
        STATE.station(
            station,
            name=DISPLAY_NAMES[station],
            origin=ORIGINS[station],
            controller="ESP32-WROOM-32E + Raspberry Pi 4B",
            phase="Waiting",
            local_progress=0,
            sensors={},
            pumps={"alum": 0, "chlorine": 0},
            pump_alarm=False,
            online=True,
        )

    for round_number in range(1, args.rounds + 1):
        STATE.update(round=round_number, phase="Distributing global model")
        STATE.cloud(status="Global weights → stations", contributors=0, weights_hash=cloud.parameters.digest())
        for client in clients.values():
            client.receive_global(cloud.parameters, round_number)
        STATE.event("download", f"Global model {cloud.parameters.digest()} distributed to all three clients")
        time.sleep(args.delay)

        updates = []
        STATE.update(phase="Local learning")
        for station, client in clients.items():
            STATE.event("local", f"Raspberry Pi local training started", station)
            updates.append(client.train_local(round_number))
            STATE.event("upload", f"Encrypted update metadata sent to cloud", station)
            time.sleep(args.delay * 0.35)

        STATE.update(phase="Relation-guided aggregation")
        STATE.cloud(status="Aggregating three local updates", contributors=len(updates))
        aggregation = cloud.aggregate(updates)
        rounds_log.append(aggregation)
        STATE.cloud(
            status="New global model ready",
            contributors=3,
            weights_hash=aggregation["weights_hash"],
            client_weights=aggregation["client_weights"],
            relation_scores=aggregation["relation_scores"],
            validation_rmse=aggregation["mean_validation_rmse"],
        )
        STATE.event("aggregate", f"Round {round_number} aggregated from all three stations")
        time.sleep(args.delay)

    for client in clients.values():
        client.receive_global(cloud.parameters, cloud.round)
        client.calibrate_private_head(include_validation=True)

    STATE.update(phase="Live IoT monitoring and closed-loop regulation")
    tests = {station: splits[station]["test"].reset_index(drop=True) for station in STATIONS}
    steps = min(args.steps, *(len(frame) for frame in tests.values()))
    for step in range(steps):
        for station in STATIONS:
            row = tests[station].iloc[step]
            sensors = plants[station].sense(row)
            prediction = clients[station].infer(sensors)
            alum, chlorine, mode = regulation(prediction, sensors)
            plants[station].actuate(alum, chlorine)
            alarm = max(alum, chlorine) >= 90.0
            station_state = {
                "phase": "Regulating" if not alarm else "High-dose safety state",
                "local_progress": 100,
                "sequence": int(row.sequence),
                "sensors": {
                    "raw_turbidity": sensors["raw_turbidity"],
                    "filtered_turbidity": sensors["filtered_turbidity"],
                    "ph": sensors["ph"],
                    "temperature": sensors["temperature"],
                    "flow": sensors["flow"],
                    "residual_chlorine": sensors["residual_chlorine"],
                },
                "forecast": float(prediction["forecast_h6"]),
                "pumps": {"alum": alum, "chlorine": chlorine},
                "pump_alarm": alarm,
                "control_mode": mode,
            }
            STATE.station(station, **station_state)
            trace.append(
                {
                    "time_step": step + 1,
                    "station": station,
                    "origin": ORIGINS[station],
                    "sequence": int(row.sequence),
                    **sensors,
                    "target_forecast": float(row.forecast_h6),
                    "predicted_forecast": float(prediction["forecast_h6"]),
                    "alum_percent": alum,
                    "chlorine_percent": chlorine,
                    "control_mode": mode,
                    "global_round": cloud.round,
                    "weights_hash": cloud.parameters.digest(),
                }
            )
        if step % 5 == 0:
            STATE.event("iot", f"Real-time sensor/control cycle {step + 1}/{steps}")
        time.sleep(args.delay)

    trace_frame = pd.DataFrame(trace)
    summary = metric_summary(trace_frame)
    pd.DataFrame(rounds_log).to_csv(RESULTS / "federated_round_metrics.csv", index=False)
    trace_frame.to_csv(RESULTS / "closed_loop_regulation_trace.csv", index=False)
    summary.to_csv(RESULTS / "station_summary.csv", index=False)
    data.to_csv(RESULTS / "sensor_channel_provenance.csv", index=False)
    manifest = {
        "architecture": "3x ESP32 + 3x Raspberry Pi federated clients + 1 federated cloud",
        "algorithm": "RG-AdaFedResidual live relation-guided adaptive residual federation",
        "data": {"published_field_nodes": ["austin", "tongji"], "disclosed_digital_twin_nodes": ["virtual"]},
        "sensor_channels": {
            "source": ["raw_turbidity", "ph", "temperature"],
            "digital_twin": ["filtered_turbidity", "flow", "residual_chlorine"],
        },
        "regulatory_targets": {"filtered_turbidity_max_ntu": 1.0, "residual_chlorine_mg_l": [0.2, 0.4]},
        "global_rounds": cloud.round,
        "final_weights_hash": cloud.parameters.digest(),
        "all_three_nodes_contributed_every_round": all(len(r["client_weights"]) == 3 for r in rounds_log),
        "metric_scope_note": "reference_forecast_* measures deployment-trace reproduction, not accuracy against future laboratory ground truth",
    }
    (RESULTS / "run_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    STATE.update(
        running=False,
        phase="Completed",
        summary={row["station"]: json_safe(row) for row in summary.to_dict("records")},
    )
    STATE.cloud(status="Federated run completed", weights_hash=cloud.parameters.digest())
    STATE.event("complete", "Simulation completed; tables were saved in results/")
    print("\nFINAL STATION RESULTS")
    print(summary.to_string(index=False))
    print(f"\nDashboard remains available at http://127.0.0.1:{args.port}")
    return summary


def parse_args():
    parser = argparse.ArgumentParser(description="Three-station IoT + RG-AdaFedResidual simulator")
    parser.add_argument("--rounds", type=int, default=12)
    parser.add_argument("--steps", type=int, default=45)
    parser.add_argument("--delay", type=float, default=0.12)
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--no-browser", action="store_true")
    parser.add_argument("--exit", action="store_true", help="Exit after computation instead of retaining dashboard")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    server = start_dashboard(args.port)
    url = f"http://127.0.0.1:{args.port}"
    print(f"Live dashboard: {url}")
    if not args.no_browser:
        threading.Timer(1.0, lambda: webbrowser.open(url)).start()
    try:
        run(args)
        if not args.exit:
            print("Press Ctrl+C to stop the dashboard server.")
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
