const order = ['austin', 'tongji', 'virtual'];
const sensorDefs = {
  raw_turbidity: ['Raw turbidity', 'NTU', '◧'],
  filtered_turbidity: ['Filtered turb.', 'NTU', '◨'],
  ph: ['pH', 'pH', '◉'],
  temperature: ['Temperature', '°C', '♨'],
  flow: ['Flow', 'm³/h', '↝'],
  residual_chlorine: ['Residual Cl₂', 'mg/L', '◌'],
};
const stages = [
  { key: 'sensor', label: 'Sensor sampling', color: '#1787ff', duration: 1600 },
  { key: 'edge', label: 'ESP32 acquisition', color: '#1787ff', duration: 850 },
  { key: 'local', label: 'Raspberry Pi local learning', color: '#8954ff', duration: 1250 },
  { key: 'upload', label: 'Uploading local weights', color: '#8954ff', duration: 1300 },
  { key: 'verify', label: 'PAV authenticating three signed updates', color: '#0aa873', duration: 1150 },
  { key: 'aggregate', label: 'Relation-guided aggregation', color: '#8954ff', duration: 1200 },
  { key: 'broadcast', label: 'Broadcasting global update', color: '#10ad72', duration: 1300 },
  { key: 'actuate', label: 'Applying dosing commands', color: '#ed4658', duration: 2600 },
];

let stageIndex = 0;
let motionPaused = false;
let stageTimer = null;
let stageTimers = [];
let latestState = null;
let queuedState = null;
let currentCycleState = null;
let activeCycle = null;
let cycleRunning = false;
let cycleToken = 0;
let standbyApplied = false;

const fmt = (value, key) => value == null ? '—' : (key === 'flow' ? Number(value).toFixed(0) : Number(value).toFixed(2));
const esc = value => String(value ?? '').replace(/[&<>"']/g, char => ({
  '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
})[char]);

function sensorMotionMarkup(id) {
  const xs = [25, 75, 125, 175, 225, 275];
  const paths = xs.map(x => `M${x} 3 Q${x} 27 82 39`);
  return `<svg class="sensor-flow" viewBox="0 0 300 43" preserveAspectRatio="none" aria-hidden="true">
    ${paths.map(path => `<path class="sensor-wire" d="${path}"/>`).join('')}
    ${paths.map((path, index) => `<circle class="sensor-ball" r="4"><animateMotion id="sensorMotion-${id}-${index}" begin="indefinite" dur=".92s" fill="freeze" path="${path}"/></circle>`).join('')}
  </svg>`;
}

function commandMarkup(id) {
  return `<div class="command-caption"><span>ESP32 PWM OUT</span><span>commands to dosing pumps</span></div>
    <svg class="command-svg" viewBox="0 0 320 41" preserveAspectRatio="none" aria-hidden="true">
      <path class="command-wire" d="M54 2 V14 H80 V41"/>
      <path class="command-wire" d="M54 14 H240 V41"/>
      <circle class="command-ball" r="4"><animateMotion id="commandMotion-${id}-0" begin="indefinite" dur="1.15s" fill="freeze" path="M54 2 V14 H80 V41"/></circle>
      <circle class="command-ball" r="4"><animateMotion id="commandMotion-${id}-1" begin="indefinite" dur="1.35s" fill="freeze" path="M54 14 H240 V41"/></circle>
    </svg>`;
}

function treatmentOutcomeMarkup(id) {
  return `<section class="treatment-outcome" aria-label="Live treatment outcome">
    <div class="treatment-title"><span>Live treatment outcome</span><strong id="${id}-quality" class="quality-status">Awaiting cycle</strong></div>
    <div class="treatment-path">
      <div class="water-state before"><small>BEFORE TREATMENT</small><b id="${id}-before-ntu">— NTU</b><em>Raw turbidity</em></div>
      <div class="dose-state"><small>DOSING</small><b><span id="${id}-dose-alum">0.0%</span> Alum</b><b><span id="${id}-dose-chlorine">0.0%</span> Cl₂</b></div>
      <div class="water-state after"><small>AFTER TREATMENT</small><b id="${id}-after-ntu">— NTU</b><em id="${id}-removal">Removal —</em><span id="${id}-after-chlorine">Residual Cl₂ — mg/L</span></div>
    </div>
  </section>`;
}

function setChangingText(element, text) {
  if (!element || element.textContent === text) return;
  const previous = element.textContent;
  element.textContent = text;
  if (previous && previous !== '—' && previous !== '0.0%') {
    element.classList.remove('value-flash');
    void element.offsetWidth;
    element.classList.add('value-flash');
  }
}

function stationShell(id, station) {
  const sensors = Object.entries(sensorDefs).map(([key, definition]) => `
    <div class="sensor" data-sensor="${key}">
      <span class="sensor-led"></span><span class="sensor-glyph">${definition[2]}</span>
      <b id="${id}-${key}">—</b><small>${definition[0]} · ${definition[1]}</small>
      <em>${['filtered_turbidity', 'flow', 'residual_chlorine'].includes(key) ? 'digital twin' : 'sensor channel'}</em>
    </div>`).join('');
  return `<article id="station-${id}" class="station">
    <div class="station-head"><div><h2 id="${id}-name">${esc(station.name || id)}</h2><div id="${id}-origin" class="origin">${esc(station.origin || '—')}</div></div><div class="station-badges"><span id="${id}-online" class="online">OFFLINE</span><span id="${id}-pav" class="pav-badge">PAV CHECKING</span></div></div>
    <div class="sensor-grid">${sensors}</div>
    ${sensorMotionMarkup(id)}
    <div class="hardware">
      <div id="${id}-esp32" class="device esp32"><span class="device-leds"><i class="red"></i><i class="green"></i></span><b>ESP32 station</b><small>sense · event bus · PWM</small></div>
      <div class="data-lane"><i class="lane-ball edge"></i><i class="lane-ball return"></i></div>
      <div id="${id}-pi" class="device pi"><span class="device-leds"><i class="red"></i><i class="green"></i></span><b>Raspberry Pi 4B</b><small>private RG-AdaFedResidual client</small></div>
    </div>
    <div class="local-box"><div class="local-row"><span id="${id}-phase">Waiting</span><span id="${id}-progressText">0%</span></div><div class="track"><span id="${id}-progress"></span></div></div>
    ${commandMarkup(id)}
    <div class="pumps">
      <div id="${id}-pump-alum" class="pump" style="--power:0"><span class="pump-icon"></span><span><b id="${id}-alum">0.0%</b><small>Alum pump</small></span></div>
      <div id="${id}-pump-chlorine" class="pump" style="--power:0"><span class="pump-icon"></span><span><b id="${id}-chlorine">0.0%</b><small>Cl₂ pump</small></span></div>
    </div>
    ${treatmentOutcomeMarkup(id)}
    <div class="station-foot"><span id="${id}-mode" class="control-mode">Awaiting control command</span><a id="${id}-link" class="open-node pending" target="_blank" rel="noopener">Inspect circuit ↗</a></div>
  </article>`;
}

function ensureStations(stations) {
  const host = document.querySelector('#stations');
  if (host.children.length) return;
  host.innerHTML = order.map(id => stationShell(id, stations[id] || {})).join('');
}

function updateStationStatus(id, station) {
  const card = document.querySelector(`#station-${id}`);
  if (!card) return;
  const connectionState = station.connection_state || (station.online ? 'LIVE' : 'OFFLINE');
  card.classList.toggle('active', connectionState === 'LIVE');
  card.classList.toggle('holding', connectionState === 'HOLDING' || connectionState === 'READY');
  document.querySelector(`#${id}-online`).textContent = connectionState;
  document.querySelector(`#${id}-name`).textContent = station.name || id;
  document.querySelector(`#${id}-origin`).textContent = station.origin || '—';
  const pavBadge = document.querySelector(`#${id}-pav`);
  const pavStatus = station.pav_status || 'PAV CHECKING';
  pavBadge.textContent = pavStatus;
  pavBadge.classList.toggle('verified', pavStatus === 'PAV VERIFIED');
  pavBadge.classList.toggle('rejected', pavStatus === 'REJECTED');
  const link = document.querySelector(`#${id}-link`);
  if (station.wokwi_url) {
    link.href = station.wokwi_url;
    link.classList.remove('pending');
  } else {
    link.removeAttribute('href');
    link.classList.add('pending');
  }
}

function updateSecurity(state) {
  const security = state.security || {};
  const layer = document.querySelector('#pavLayer');
  const status = security.status || 'PAV CHECKING';
  layer.dataset.status = status.includes('REJECTED') ? 'rejected' : status.includes('VERIFIED') ? 'verified' : 'checking';
  document.querySelector('#pavStatus').textContent = status;
  document.querySelector('#pavAlgorithm').textContent = security.algorithm || 'HMAC-SHA256';
  document.querySelector('#pavVerified').textContent = Number(security.verified_stations || 0);
  document.querySelector('#pavRequired').textContent = Number(security.required_stations || order.length);
  document.querySelector('#pavRejected').textContent = Number(security.rejected_messages || 0);
}

function enterStrictStandby(state) {
  const isCloud = String(state.deployment?.transport || '').includes('CLOUD');
  if (!standbyApplied) {
    clearTimeout(stageTimer);
    clearStageTimers();
    cycleToken += 1;
    cycleRunning = false;
    activeCycle = 0;
    currentCycleState = null;
    queuedState = null;
    motionPaused = false;
    document.body.classList.remove('motion-paused');
    document.querySelector('#motionToggle').textContent = 'Pause motion';
    document.querySelectorAll('.global-ball,.sensor-ball,.command-ball').forEach(ball => ball.classList.remove('visible'));
    document.querySelectorAll('.lane-ball').forEach(ball => ball.classList.remove('go'));
  }
  standbyApplied = true;
  const architecture = document.querySelector('#architecture');
  document.querySelector('#pavLayer').classList.remove('active');
  architecture.dataset.stage = 'standby';
  architecture.style.setProperty('--stage-color', '#94a3b8');
  document.querySelector('#visualStage').textContent = isCloud
    ? 'Starting all three cloud station runtimes'
    : 'Waiting for all three Wokwi stations';
  document.querySelector('#cycle').textContent = '0';
  document.querySelector('#cloudStatus').textContent = state.cloud?.status || 'Safety standby';
  document.querySelector('#hash').textContent = state.cloud?.weights_hash || '—';
  document.querySelectorAll('.step').forEach(element => element.classList.remove('active'));
  document.querySelector('#motionToggle').disabled = true;
  document.querySelector('#replay').disabled = true;

  order.forEach(id => {
    const station = state.stations?.[id] || {};
    Object.keys(sensorDefs).forEach(key => {
      const value = document.querySelector(`#${id}-${key}`);
      value.textContent = '—';
      value.classList.remove('value-flash');
    });
    document.querySelector(`#${id}-phase`).textContent = station.phase || (station.online
      ? 'Connected — waiting for all stations'
      : isCloud ? 'Starting cloud station runtime' : 'Waiting for Wokwi telemetry');
    document.querySelector(`#${id}-progressText`).textContent = '0%';
    document.querySelector(`#${id}-progress`).style.width = '0%';
    ['alum', 'chlorine'].forEach(key => {
      const pump = document.querySelector(`#${id}-pump-${key}`);
      pump.style.setProperty('--power', 0);
      pump.classList.remove('high', 'received');
      document.querySelector(`#${id}-${key}`).textContent = '0.0%';
      document.querySelector(`#${id}-dose-${key}`).textContent = '0.0%';
    });
    resetTreatmentOutcome(id);
    document.querySelector(`#${id}-mode`).textContent = 'Safety interlock · zero output';
  });
}

function enterHoldState(state) {
  clearTimeout(stageTimer);
  clearStageTimers();
  cycleToken += 1;
  cycleRunning = false;
  queuedState = null;
  currentCycleState = state;
  activeCycle = Number(state.live_cycle || activeCycle || 0);
  standbyApplied = false;
  motionPaused = false;
  document.body.classList.remove('motion-paused');
  document.querySelectorAll('.global-ball,.sensor-ball,.command-ball').forEach(ball => ball.classList.remove('visible'));
  document.querySelectorAll('.lane-ball').forEach(ball => ball.classList.remove('go'));
  const architecture = document.querySelector('#architecture');
  document.querySelector('#pavLayer').classList.remove('active');
  architecture.dataset.stage = 'hold';
  architecture.style.setProperty('--stage-color', '#f3a21c');
  document.querySelector('#visualStage').textContent = 'Holding last validated state';
  document.querySelector('#cycle').textContent = activeCycle;
  document.querySelector('#cloudStatus').textContent = state.cloud?.status || 'Holding last validated state';
  document.querySelector('#hash').textContent = state.cloud?.weights_hash || '—';
  document.querySelectorAll('.step').forEach(element => element.classList.remove('active'));
  document.querySelector('#motionToggle').disabled = true;
  document.querySelector('#replay').disabled = true;

  order.forEach(id => {
    const station = state.stations?.[id] || {};
    Object.keys(sensorDefs).forEach(key => {
      const value = station.sensors?.[key];
      document.querySelector(`#${id}-${key}`).textContent = value == null ? '—' : fmt(value, key);
    });
    updateStationProcess(id, station);
    ['alum', 'chlorine'].forEach(key => {
      const value = Number(station.pumps?.[key] || 0);
      const pump = document.querySelector(`#${id}-pump-${key}`);
      pump.style.setProperty('--power', Math.max(0, value / 100));
      pump.classList.toggle('high', value >= 90);
      pump.classList.remove('received');
      document.querySelector(`#${id}-${key}`).textContent = `${value.toFixed(1)}%`;
      document.querySelector(`#${id}-dose-${key}`).textContent = `${value.toFixed(1)}%`;
    });
    updateTreatmentOutcome(id, station);
    const age = Number(station.stale_seconds || 0);
    document.querySelector(`#${id}-mode`).textContent = station.connection_state === 'HOLDING'
      ? `Holding last validated command · ${Math.round(age)} s`
      : 'Link ready · waiting for full station quorum';
  });
}

function commitSensor(id, station, key) {
  setChangingText(document.querySelector(`#${id}-${key}`), fmt(station.sensors?.[key], key));
  if (key === 'residual_chlorine') updateTreatmentOutcome(id, station);
}

function resetTreatmentOutcome(id) {
  document.querySelector(`#${id}-before-ntu`).textContent = '— NTU';
  document.querySelector(`#${id}-after-ntu`).textContent = '— NTU';
  document.querySelector(`#${id}-removal`).textContent = 'Removal —';
  document.querySelector(`#${id}-after-chlorine`).textContent = 'Residual Cl₂ — mg/L';
  const quality = document.querySelector(`#${id}-quality`);
  quality.textContent = 'Awaiting cycle';
  quality.classList.remove('within', 'outside');
}

function updateTreatmentOutcome(id, station) {
  const raw = station.sensors?.raw_turbidity;
  const filtered = station.sensors?.filtered_turbidity;
  const chlorine = station.sensors?.residual_chlorine;
  if (![raw, filtered, chlorine].every(value => value != null && Number.isFinite(Number(value)))) {
    resetTreatmentOutcome(id);
    return;
  }
  const rawValue = Number(raw);
  const filteredValue = Number(filtered);
  const chlorineValue = Number(chlorine);
  const removal = rawValue > 0 ? 100 * (rawValue - filteredValue) / rawValue : 0;
  const jointWithin = filteredValue <= 1.0 && chlorineValue >= 0.2 && chlorineValue <= 0.4;
  setChangingText(document.querySelector(`#${id}-before-ntu`), `${rawValue.toFixed(2)} NTU`);
  setChangingText(document.querySelector(`#${id}-after-ntu`), `${filteredValue.toFixed(2)} NTU`);
  setChangingText(document.querySelector(`#${id}-removal`), `Removal ${Math.max(0, removal).toFixed(1)}%`);
  setChangingText(document.querySelector(`#${id}-after-chlorine`), `Residual Cl₂ ${chlorineValue.toFixed(2)} mg/L`);
  const quality = document.querySelector(`#${id}-quality`);
  quality.textContent = jointWithin ? 'WATER WITHIN TARGET' : 'ATTENTION REQUIRED';
  quality.classList.toggle('within', jointWithin);
  quality.classList.toggle('outside', !jointWithin);
}

function updateStationProcess(id, station) {
  const progress = Math.max(0, Math.min(100, Number(station.local_progress || 0)));
  document.querySelector(`#${id}-phase`).textContent = station.phase || 'Waiting';
  document.querySelector(`#${id}-progressText`).textContent = `${Math.round(progress)}%`;
  document.querySelector(`#${id}-progress`).style.width = `${progress}%`;
}

function commitPump(id, station, key) {
  const value = Number(station.pumps?.[key] || 0);
  const pump = document.querySelector(`#${id}-pump-${key}`);
  pump.style.setProperty('--power', Math.max(0, value / 100));
  pump.classList.toggle('high', value >= 90);
  pump.classList.add('received');
  setChangingText(document.querySelector(`#${id}-${key}`), `${value.toFixed(1)}%`);
  setChangingText(document.querySelector(`#${id}-dose-${key}`), `${value.toFixed(1)}%`);
}

function commitControlResult(id, station) {
  document.querySelector(`#${id}-mode`).textContent = `${station.control_mode || 'Awaiting command'} · H6 ${fmt(station.forecast, 'raw_turbidity')} NTU · ${Number(station.latency_ms || 0).toFixed(1)} ms`;
}

function showSvgMotion(animationId, delay = 0, visibleFor = 1500) {
  const token = setTimeout(() => {
    const motion = document.getElementById(animationId);
    if (!motion || motionPaused) return;
    const ball = motion.parentElement;
    ball.classList.add('visible');
    try { motion.beginElement(); } catch (error) { /* SVG motion unsupported: path remains visible */ }
    setTimeout(() => ball.classList.remove('visible'), visibleFor);
  }, delay);
  stageTimers.push(token);
}

function restartLane(selector, delay = 0) {
  const token = setTimeout(() => {
    if (motionPaused) return;
    document.querySelectorAll(selector).forEach(element => {
      element.classList.remove('go');
      void element.offsetWidth;
      element.classList.add('go');
    });
  }, delay);
  stageTimers.push(token);
}

function clearStageTimers() {
  stageTimers.forEach(clearTimeout);
  stageTimers = [];
}

function runStage(index, state, token) {
  if (!state || motionPaused || token !== cycleToken) return;
  clearTimeout(stageTimer);
  clearStageTimers();
  const stage = stages[index];
  stageIndex = index;
  const architecture = document.querySelector('#architecture');
  architecture.dataset.stage = stage.key;
  architecture.style.setProperty('--stage-color', stage.color);
  document.querySelector('#visualStage').textContent = stage.label;
  document.querySelectorAll('.step').forEach(element => element.classList.toggle('active', element.dataset.key === stage.key));
  document.querySelector('#pavLayer').classList.toggle('active', stage.key === 'verify');
  if (!motionPaused) {
    if (stage.key === 'sensor') {
      document.querySelectorAll('.pump.received').forEach(pump => pump.classList.remove('received'));
      order.forEach(id => {
        Object.keys(sensorDefs).forEach((key, index) => {
          showSvgMotion(`sensorMotion-${id}-${index}`, index * 85, 1050);
          const arrival = setTimeout(() => {
            if (token === cycleToken && !motionPaused) commitSensor(id, state.stations?.[id] || {}, key);
          }, 920 + index * 85);
          stageTimers.push(arrival);
        });
      });
    }
    if (stage.key === 'edge') restartLane('.lane-ball.edge');
    if (stage.key === 'local') order.forEach(id => updateStationProcess(id, state.stations?.[id] || {}));
    if (stage.key === 'upload') for (let i = 0; i < 3; i += 1) showSvgMotion(`uploadMotion-${i}`, i * 100, 1400);
    if (stage.key === 'verify') {
      updateSecurity(state);
      for (let i = 0; i < 3; i += 1) showSvgMotion(`verifyMotion-${i}`, 340 + i * 85, 1050);
    }
    if (stage.key === 'aggregate') {
      document.querySelector('#cloudStatus').textContent = state.cloud?.status || 'Relation-guided aggregation';
      document.querySelector('#hash').textContent = state.cloud?.weights_hash || '—';
    }
    if (stage.key === 'broadcast') {
      for (let i = 0; i < 3; i += 1) showSvgMotion(`broadcastMotion-${i}`, i * 100, 1400);
      restartLane('.lane-ball.return', 720);
    }
    if (stage.key === 'actuate') order.forEach(id => {
      showSvgMotion(`commandMotion-${id}-0`, 0, 1450);
      showSvgMotion(`commandMotion-${id}-1`, 170, 1550);
    });
    if (stage.key === 'actuate') {
      const alumArrival = setTimeout(() => {
        if (token !== cycleToken || motionPaused) return;
        order.forEach(id => commitPump(id, state.stations?.[id] || {}, 'alum'));
      }, 1150);
      const chlorineArrival = setTimeout(() => {
        if (token !== cycleToken || motionPaused) return;
        order.forEach(id => {
          const station = state.stations?.[id] || {};
          commitPump(id, station, 'chlorine');
          commitControlResult(id, station);
        });
      }, 1530);
      stageTimers.push(alumArrival, chlorineArrival);
    }
  }
  stageTimer = setTimeout(() => {
    if (motionPaused || token !== cycleToken) return;
    if (index < stages.length - 1) {
      runStage(index + 1, state, token);
      return;
    }
    cycleRunning = false;
    if (queuedState && Number(queuedState.live_cycle || 0) !== activeCycle) {
      const next = queuedState;
      queuedState = null;
      startCycle(next, true);
    }
  }, stage.duration);
}

function startCycle(state, force = false) {
  if (!state) return;
  if (motionPaused) {
    queuedState = state;
    return;
  }
  if (cycleRunning && !force) {
    queuedState = state;
    return;
  }
  clearTimeout(stageTimer);
  clearStageTimers();
  standbyApplied = false;
  document.querySelector('#motionToggle').disabled = false;
  document.querySelector('#replay').disabled = false;
  cycleToken += 1;
  activeCycle = Number(state.live_cycle || 0);
  currentCycleState = state;
  cycleRunning = true;
  document.querySelector('#cycle').textContent = activeCycle;
  runStage(0, state, cycleToken);
}

document.querySelector('#motionToggle').addEventListener('click', event => {
  motionPaused = !motionPaused;
  document.body.classList.toggle('motion-paused', motionPaused);
  event.currentTarget.textContent = motionPaused ? 'Resume motion' : 'Pause motion';
  if (motionPaused) {
    clearTimeout(stageTimer);
    clearStageTimers();
    cycleRunning = false;
  } else startCycle(queuedState || latestState || currentCycleState, true);
});

document.querySelector('#replay').addEventListener('click', () => {
  motionPaused = false;
  document.body.classList.remove('motion-paused');
  document.querySelector('#motionToggle').textContent = 'Pause motion';
  startCycle(latestState || currentCycleState, true);
});

function renderSummary(summary) {
  const values = order.map(key => summary[key]).filter(Boolean);
  if (!values.length) return;
  document.querySelector('#summary').innerHTML = `<table><thead><tr><th>Station</th><th>Data origin</th><th>Samples</th><th>Turbidity acceptance</th><th>Chlorine acceptance</th><th>Overall water-quality acceptance</th><th>Average alum dose</th><th>Average chlorine dose</th></tr></thead><tbody>${values.map(row => `<tr><td><b>${esc(row.station)}</b></td><td>${esc(row.origin)}</td><td>${row.n}</td><td>${Number(row.turbidity_compliance_pct).toFixed(1)}%</td><td>${Number(row.chlorine_compliance_pct).toFixed(1)}%</td><td><b class="acceptance-value">${Number(row.joint_compliance_pct).toFixed(1)}%</b></td><td>${Number(row.mean_alum_pct).toFixed(1)}%</td><td>${Number(row.mean_chlorine_pct).toFixed(1)}%</td></tr>`).join('')}</tbody></table><div class="disclosure"><b>Acceptance percentage</b> is the share of completed cycles that met the configured treated-water target. These rolling operational statistics update with the live stream; fixed paper results remain unchanged.</div>`;
}

async function refresh() {
  try {
    const response = await fetch(`/api/state?${Date.now()}`, { cache: 'no-store' });
    const state = await response.json();
    const mode = state.deployment?.live_mode || 'INITIALIZING';
    const isCloud = mode === 'CLOUD FEDERATED LIVE';
    const isLive = mode === 'LIVE MQTT' || isCloud;
    const isHolding = mode === 'HOLDING LAST STATE';
    const dot = document.querySelector('#liveDot');
    dot.classList.toggle('on', isLive);
    dot.classList.toggle('fallback', !isLive && state.running);
    document.querySelector('#runState').textContent = mode;
    const transport = state.deployment?.transport || 'PUBLIC MQTT';
    const transportDetail = state.broker?.connected
      ? (String(transport).includes('CLOUD') ? 'EVENT BUS ACTIVE' : 'BROKER CONNECTED')
      : (String(transport).includes('CLOUD') ? 'EVENT BUS STARTING' : 'BROKER STANDBY');
    document.querySelector('#transportState').textContent = `${transport} · ${transportDetail}`;
    document.querySelector('#round').textContent = state.round;
    document.querySelector('#maxRound').textContent = state.max_rounds;
    ensureStations(state.stations || {});
    order.forEach(id => updateStationStatus(id, state.stations?.[id] || {}));
    updateSecurity(state);
    latestState = state;
    const nextCycle = Number(state.live_cycle || 0);
    if (isLive && nextCycle > 0 && nextCycle !== activeCycle) {
      // A newly published backend cycle is authoritative.  Interrupting an
      // older visual cycle prevents a page opened mid-cycle from remaining
      // one sample behind the MQTT/model execution indefinitely.
      startCycle(state, true);
    } else if (isHolding && nextCycle > 0) {
      enterHoldState(state);
    } else if (!isLive || nextCycle === 0) {
      enterStrictStandby(state);
    }
    document.querySelector('#events').innerHTML = state.events.length ? state.events.map(event => `<div class="event"><time>${esc(event.time)}</time><span class="badge">${esc(event.kind).toUpperCase()}</span><span>${event.station ? `${esc(event.station)}: ` : ''}${esc(event.text)}</span></div>`).join('') : '<div class="empty">Waiting for events…</div>';
    const sampleDetail = isCloud
      ? `The displayed figures are the current samples emitted by three independent cloud station runtimes. Each cycle performs three private local updates, relation-guided aggregation to global model v${state.federation_version || '—'}, H6 inference and acknowledged actuator commands.`
      : isLive
      ? 'The displayed figures are the exact current MQTT samples transmitted to the three acknowledged Wokwi nodes.'
      : isHolding
      ? 'The last validated readings and commands are frozen during the temporary interruption; no new command is generated until all three links recover.'
      : 'No operational readings or pump commands are issued until all three Wokwi stations acknowledge current telemetry.';
    document.querySelector('#deploymentDisclosure').textContent = `Execution mode: ${mode}. ${state.deployment?.accuracy_scope || '—'}. Transport: ${transport}. ${sampleDetail} Austin and Tongji use published-field streams; Virtual is explicitly disclosed as a digital twin.`;
    renderSummary(state.summary || {});
  } catch (error) {
    document.querySelector('#runState').textContent = 'OFFLINE';
  }
}

refresh();
setInterval(refresh, 450);
