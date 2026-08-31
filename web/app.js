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
  { key: 'sensor', label: 'Sensor sampling', color: '#1787ff', duration: 1450 },
  { key: 'edge', label: 'ESP32 acquisition', color: '#1787ff', duration: 1200 },
  { key: 'local', label: 'Raspberry Pi local learning', color: '#8954ff', duration: 1450 },
  { key: 'upload', label: 'Uploading local weights', color: '#8954ff', duration: 1350 },
  { key: 'aggregate', label: 'Relation-guided aggregation', color: '#8954ff', duration: 1550 },
  { key: 'broadcast', label: 'Broadcasting global update', color: '#10ad72', duration: 1450 },
  { key: 'actuate', label: 'Applying dosing commands', color: '#ed4658', duration: 1900 },
];

let stageIndex = 0;
let motionPaused = false;
let stageTimer = null;
let stageTimers = [];

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
      <circle class="command-node source" cx="54" cy="2" r="5"/>
      <circle class="command-node target" cx="80" cy="39" r="4"/>
      <circle class="command-node target" cx="240" cy="39" r="4"/>
      <circle class="command-ball" r="5"><animateMotion id="commandMotion-${id}-0" begin="indefinite" dur="1.15s" fill="freeze" path="M54 2 V14 H80 V41"/></circle>
      <circle class="command-ball" r="5"><animateMotion id="commandMotion-${id}-1" begin="indefinite" dur="1.35s" fill="freeze" path="M54 14 H240 V41"/></circle>
    </svg>`;
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
    <div class="station-head"><div><h2 id="${id}-name">${esc(station.name || id)}</h2><div id="${id}-origin" class="origin">${esc(station.origin || '—')}</div></div><span id="${id}-online" class="online">OFFLINE</span></div>
    <div class="sensor-grid">${sensors}</div>
    ${sensorMotionMarkup(id)}
    <div class="hardware">
      <div id="${id}-esp32" class="device esp32"><span class="device-leds"><i class="red"></i><i class="green"></i></span><b>ESP32</b><small>ADC · MQTT · PWM</small></div>
      <div class="data-lane"><i class="lane-ball edge"></i><i class="lane-ball return"></i></div>
      <div id="${id}-pi" class="device pi"><span class="device-leds"><i class="red"></i><i class="green"></i></span><b>Raspberry Pi 4B</b><small>RG-AdaFedResidual client</small></div>
    </div>
    <div class="local-box"><div class="local-row"><span id="${id}-phase">Waiting</span><span id="${id}-progressText">0%</span></div><div class="track"><span id="${id}-progress"></span></div></div>
    ${commandMarkup(id)}
    <div class="pumps">
      <div id="${id}-pump-alum" class="pump"><span class="pump-icon"></span><span><b id="${id}-alum">0.0%</b><small>Alum pump</small></span></div>
      <div id="${id}-pump-chlorine" class="pump"><span class="pump-icon"></span><span><b id="${id}-chlorine">0.0%</b><small>Cl₂ pump</small></span></div>
    </div>
    <div class="station-foot"><span id="${id}-mode" class="control-mode">Awaiting control command</span><a id="${id}-link" class="open-node pending" target="_blank" rel="noopener">Open Wokwi ↗</a></div>
  </article>`;
}

function ensureStations(stations) {
  const host = document.querySelector('#stations');
  if (host.children.length) return;
  host.innerHTML = order.map(id => stationShell(id, stations[id] || {})).join('');
}

function updateStation(id, station) {
  const card = document.querySelector(`#station-${id}`);
  if (!card) return;
  card.classList.toggle('active', Boolean(station.online));
  document.querySelector(`#${id}-online`).textContent = station.online ? 'ONLINE' : 'OFFLINE';
  document.querySelector(`#${id}-name`).textContent = station.name || id;
  document.querySelector(`#${id}-origin`).textContent = station.origin || '—';
  Object.keys(sensorDefs).forEach(key => {
    setChangingText(document.querySelector(`#${id}-${key}`), fmt(station.sensors?.[key], key));
  });
  const progress = Math.max(0, Math.min(100, Number(station.local_progress || 0)));
  document.querySelector(`#${id}-phase`).textContent = station.phase || 'Waiting';
  document.querySelector(`#${id}-progressText`).textContent = `${Math.round(progress)}%`;
  document.querySelector(`#${id}-progress`).style.width = `${progress}%`;
  [['alum', 'alum'], ['chlorine', 'chlorine']].forEach(([key, label]) => {
    const value = Number(station.pumps?.[key] || 0);
    const pump = document.querySelector(`#${id}-pump-${key}`);
    pump.style.setProperty('--power', Math.max(.12, value / 100));
    pump.classList.toggle('high', value >= 90);
    setChangingText(document.querySelector(`#${id}-${label}`), `${value.toFixed(1)}%`);
  });
  document.querySelector(`#${id}-mode`).textContent = `${station.control_mode || 'Awaiting command'} · H6 ${fmt(station.forecast, 'raw_turbidity')} NTU · ${Number(station.latency_ms || 0).toFixed(1)} ms`;
  const link = document.querySelector(`#${id}-link`);
  if (station.wokwi_url) {
    link.href = station.wokwi_url;
    link.classList.remove('pending');
  } else {
    link.removeAttribute('href');
    link.classList.add('pending');
  }
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

function runStage(index = stageIndex) {
  clearTimeout(stageTimer);
  clearStageTimers();
  const stage = stages[index];
  stageIndex = index;
  const architecture = document.querySelector('#architecture');
  architecture.dataset.stage = stage.key;
  architecture.style.setProperty('--stage-color', stage.color);
  document.querySelector('#visualStage').textContent = stage.label;
  document.querySelectorAll('.step').forEach(element => element.classList.toggle('active', element.dataset.key === stage.key));
  if (!motionPaused) {
    if (stage.key === 'sensor') order.forEach(id => { for (let i = 0; i < 6; i += 1) showSvgMotion(`sensorMotion-${id}-${i}`, i * 85, 1050); });
    if (stage.key === 'edge') restartLane('.lane-ball.edge');
    if (stage.key === 'upload') for (let i = 0; i < 3; i += 1) showSvgMotion(`uploadMotion-${i}`, i * 100, 1400);
    if (stage.key === 'broadcast') {
      for (let i = 0; i < 3; i += 1) showSvgMotion(`broadcastMotion-${i}`, i * 100, 1400);
      restartLane('.lane-ball.return', 720);
    }
    if (stage.key === 'actuate') order.forEach(id => {
      showSvgMotion(`commandMotion-${id}-0`, 0, 1450);
      showSvgMotion(`commandMotion-${id}-1`, 170, 1550);
    });
  }
  stageTimer = setTimeout(() => { if (!motionPaused) runStage((index + 1) % stages.length); }, stage.duration);
}

document.querySelector('#motionToggle').addEventListener('click', event => {
  motionPaused = !motionPaused;
  document.body.classList.toggle('motion-paused', motionPaused);
  event.currentTarget.textContent = motionPaused ? 'Resume motion' : 'Pause motion';
  if (motionPaused) {
    clearTimeout(stageTimer);
    clearStageTimers();
  } else runStage(stageIndex);
});

document.querySelector('#replay').addEventListener('click', () => {
  motionPaused = false;
  document.body.classList.remove('motion-paused');
  document.querySelector('#motionToggle').textContent = 'Pause motion';
  runStage(0);
});

function renderSummary(summary) {
  const values = order.map(key => summary[key]).filter(Boolean);
  if (!values.length) return;
  document.querySelector('#summary').innerHTML = `<table><thead><tr><th>Station</th><th>Origin</th><th>Cycles</th><th>Turbidity compliant</th><th>Chlorine compliant</th><th>Joint compliant</th><th>Mean alum</th><th>Mean chlorine</th></tr></thead><tbody>${values.map(row => `<tr><td><b>${esc(row.station)}</b></td><td>${esc(row.origin)}</td><td>${row.n}</td><td>${Number(row.turbidity_compliance_pct).toFixed(1)}%</td><td>${Number(row.chlorine_compliance_pct).toFixed(1)}%</td><td>${Number(row.joint_compliance_pct).toFixed(1)}%</td><td>${Number(row.mean_alum_pct).toFixed(1)}%</td><td>${Number(row.mean_chlorine_pct).toFixed(1)}%</td></tr>`).join('')}</tbody></table><div class="disclosure">Rolling operational diagnostics are recalculated from the latest stream; fixed paper results remain unchanged.</div>`;
}

async function refresh() {
  try {
    const response = await fetch(`/api/state?${Date.now()}`, { cache: 'no-store' });
    const state = await response.json();
    const mode = state.deployment?.live_mode || 'INITIALIZING';
    const isLive = mode === 'LIVE MQTT';
    const dot = document.querySelector('#liveDot');
    dot.classList.toggle('on', isLive);
    dot.classList.toggle('fallback', !isLive && state.running);
    document.querySelector('#runState').textContent = mode;
    const transport = state.deployment?.transport || 'PUBLIC MQTT';
    const transportDetail = state.broker?.connected ? 'BROKER CONNECTED' : 'BROKER STANDBY';
    document.querySelector('#transportState').textContent = `${transport} · ${transportDetail}`;
    document.querySelector('#round').textContent = state.round;
    document.querySelector('#maxRound').textContent = state.max_rounds;
    document.querySelector('#cycle').textContent = state.live_cycle || 0;
    document.querySelector('#cloudStatus').textContent = state.cloud.status;
    document.querySelector('#hash').textContent = state.cloud.weights_hash || '—';
    ensureStations(state.stations || {});
    order.forEach(id => updateStation(id, state.stations?.[id] || {}));
    document.querySelector('#events').innerHTML = state.events.length ? state.events.map(event => `<div class="event"><time>${esc(event.time)}</time><span class="badge">${esc(event.kind).toUpperCase()}</span><span>${event.station ? `${esc(event.station)}: ` : ''}${esc(event.text)}</span></div>`).join('') : '<div class="empty">Waiting for events…</div>';
    const sampleDetail = isLive
      ? 'The displayed figures are the exact current MQTT samples transmitted to the three acknowledged Wokwi nodes.'
      : 'The displayed figures advance through the explicitly disclosed verified external-validation trace while Wokwi is unavailable.';
    document.querySelector('#deploymentDisclosure').textContent = `Execution mode: ${mode}. ${state.deployment?.accuracy_scope || '—'}. Transport: ${transport}. ${sampleDetail} Austin and Tongji use published-field streams; Virtual is explicitly disclosed as a digital twin.`;
    renderSummary(state.summary || {});
  } catch (error) {
    document.querySelector('#runState').textContent = 'OFFLINE';
  }
}

refresh().then(() => runStage(0));
setInterval(refresh, 700);

