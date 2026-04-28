'use strict';

// data field values → display labels
const TIER_DISPLAY = {
  CRITICAL: 'Prime',
  HIGH:     'Strong',
  MODERATE: 'Moderate',
  LOW:      'Low',
};

const TIER_COLORS = {
  CRITICAL: '#16a34a',  // green-700
  HIGH:     '#4ade80',  // green-400
  MODERATE: '#bbf7d0',  // green-100
  LOW:      '#374151',  // gray-700
};

const SCORE_LABELS = [
  { key: 'score_transmission',  label: 'Transmission lines',   type: 'infra',   max: 20 },
  { key: 'score_substation',    label: 'Substation proximity',  type: 'infra',   max: 15 },
  { key: 'score_water',         label: 'Water availability',   type: 'infra',   max: 20 },
  { key: 'score_aquifer',       label: 'Aquifer access',       type: 'infra',   max: 10 },
  { key: 'score_land_area',     label: 'Land area',            type: 'infra',   max: 15 },
  { key: 'score_terrain',       label: 'Terrain flatness',     type: 'infra',   max: 10 },
  { key: 'score_opp_zone',      label: 'Opportunity zone',     type: 'infra',   max: 5  },
  { key: 'score_flood_penalty', label: 'Flood risk',           type: 'penalty', max: 10 },
];

// ── Icon generation for market-signal symbol layers ────────────────────────────

function makeIconImage(shape, color, size = 40) {
  const c = document.createElement('canvas');
  c.width = c.height = size;
  const ctx = c.getContext('2d');
  const p = 5;
  const s = size - p * 2;
  ctx.fillStyle = color;

  if (shape === 'square') {
    ctx.fillRect(p, p, s, s);
    ctx.strokeStyle = 'rgba(255,255,255,0.95)';
    ctx.lineWidth = 2.5;
    ctx.strokeRect(p + 1.25, p + 1.25, s - 2.5, s - 2.5);
  } else if (shape === 'triangle') {
    ctx.beginPath();
    ctx.moveTo(size / 2, p);
    ctx.lineTo(size - p, size - p);
    ctx.lineTo(p, size - p);
    ctx.closePath();
    ctx.fill();
    ctx.strokeStyle = 'rgba(255,255,255,0.95)';
    ctx.lineWidth = 2.5;
    ctx.stroke();
  } else if (shape === 'diamond') {
    ctx.beginPath();
    ctx.moveTo(size / 2, p);
    ctx.lineTo(size - p, size / 2);
    ctx.lineTo(size / 2, size - p);
    ctx.lineTo(p, size / 2);
    ctx.closePath();
    ctx.fill();
    ctx.strokeStyle = 'rgba(255,255,255,0.95)';
    ctx.lineWidth = 2.5;
    ctx.stroke();
  }

  const d = ctx.getImageData(0, 0, size, size);
  return { width: size, height: size, data: new Uint8Array(d.data.buffer) };
}

// ── Overlay layer configuration ────────────────────────────────────────────────

const OVERLAY_CONFIG = {
  transmission_lines: {
    label: 'Transmission Lines',
    belowTribal: true,
    type: 'line',
    paint: {
      'line-color': '#f0c040',
      'line-width': ['interpolate', ['linear'], ['zoom'], 3, 0.5, 8, 2],
      'line-opacity': 0.7,
    },
    popupLabel: (p) => `${p.VOLTAGE || '?'}kV — ${p.TYPE || ''} ${p.STATUS || ''}`.trim(),
  },
  substations: {
    label: 'Substations',
    belowTribal: true,
    type: 'circle',
    paint: {
      'circle-color': '#ffa500',
      'circle-radius': ['interpolate', ['linear'], ['zoom'], 4, 2.5, 10, 6],
      'circle-opacity': 0.85,
      'circle-stroke-color': '#000',
      'circle-stroke-width': 0.5,
    },
    popupLabel: (p) => `${p.NAME || 'Substation'} — ${p.MAX_VOLT || '?'}kV`,
  },
  power_plants: {
    label: 'Power Plants',
    belowTribal: true,
    type: 'circle',
    paint: {
      'circle-color': [
        'match', ['get', 'TYPE'],
        'NATURAL GAS', '#c084fc',
        'NUCLEAR',     '#f43f5e',
        'COAL',        '#78716c',
        'HYDRO',       '#38bdf8',
        'WIND',        '#86efac',
        'SOLAR',       '#fde68a',
        '#c084fc',
      ],
      'circle-radius': [
        'interpolate', ['linear'], ['coalesce', ['get', 'TOTAL_MW'], 100],
        50, 3, 500, 6, 2000, 10, 10000, 16,
      ],
      'circle-opacity': 0.85,
      'circle-stroke-color': '#000',
      'circle-stroke-width': 0.5,
    },
    popupLabel: (p) => `${p.NAME || 'Plant'} — ${p.TYPE || '?'} — ${p.TOTAL_MW ? Math.round(p.TOTAL_MW) + ' MW' : ''}`,
  },
  wind_turbines: {
    label: 'Wind Turbines',
    belowTribal: true,
    type: 'circle',
    paint: {
      'circle-color': '#6ee7b7',
      'circle-radius': ['interpolate', ['linear'], ['zoom'], 4, 1.5, 10, 4],
      'circle-opacity': 0.8,
      'circle-stroke-color': '#fff',
      'circle-stroke-width': 0.5,
    },
    popupLabel: (p) => `${p.p_name || 'Wind Turbine'}${p.t_cap ? ' — ' + p.t_cap + ' kW' : ''}${p.p_year ? ' (' + p.p_year + ')' : ''}`,
  },
  gas_pipelines: {
    label: 'Natural Gas Pipelines',
    belowTribal: true,
    type: 'line',
    paint: {
      'line-color': '#22d3ee',
      'line-width': ['interpolate', ['linear'], ['zoom'], 3, 0.5, 8, 1.8],
      'line-opacity': 0.6,
      'line-dasharray': [4, 2],
    },
    popupLabel: (p) => `${p.Operator || 'Gas Pipeline'} — ${p.Type || ''}`.trim(),
  },

  // ── Market signal layers — rendered above tribal fill ──────────────────────
  known_sites: {
    label: 'Existing Data Centers',
    aboveTribal: true,
    type: 'symbol',
    layout: {
      'icon-image': 'icon-dc',
      'icon-size': ['interpolate', ['linear'], ['zoom'], 3, 0.45, 8, 0.85, 12, 1.1],
      'icon-allow-overlap': true,
      'icon-ignore-placement': true,
    },
    paint: {},
    popupLabel: (p) => `${p.company || p.name || 'Data Center'}${p.status ? ' — ' + p.status : ''}`,
  },
  land_acquisitions: {
    label: 'Developer Activity',
    aboveTribal: true,
    type: 'symbol',
    layout: {
      'icon-image': 'icon-acq',
      'icon-size': ['interpolate', ['linear'], ['zoom'], 3, 0.4, 8, 0.75, 12, 1.0],
      'icon-allow-overlap': true,
      'icon-ignore-placement': true,
    },
    paint: {},
    popupLabel: (p) => `${p.buyer || 'Unknown'} — ${p.state || ''} (${p.confidence || '?'}% confidence, ${p.source || ''})`,
  },
  ferc_flags: {
    label: 'Grid Investment',
    aboveTribal: true,
    type: 'symbol',
    layout: {
      'icon-image': 'icon-grid',
      'icon-size': [
        'interpolate', ['linear'],
        ['coalesce', ['get', 'mw'], 100],
        50, 0.35, 300, 0.55, 1000, 0.75, 3000, 0.95,
      ],
      'icon-allow-overlap': true,
      'icon-ignore-placement': true,
    },
    paint: {},
    popupLabel: (p) => `${p.project_name || p.applicant || 'Grid project'} — ${p.mw ? Math.round(p.mw) + ' MW' : '?'} · ${p.status || ''} · ${p.county || ''} ${p.state || ''}`.trim().replace(/·\s*$/, ''),
  },
};

const INTEL_LAYERS = ['known_sites', 'land_acquisitions', 'ferc_flags'];

let map, popup, overlayPopup;
let allFeatures = [];
let activeTiers = new Set(['CRITICAL', 'HIGH', 'MODERATE', 'LOW']);
let colorMode = 'tier';
const overlayState = {};

// ── Data loading ───────────────────────────────────────────────────────────────

async function loadData() {
  const [riskResp, statsResp] = await Promise.all([
    fetch('data/tribal_datacenter_risk.geojson'),
    fetch('data/stats.json'),
  ]);
  if (!riskResp.ok) throw new Error('Failed to load tribal_datacenter_risk.geojson');
  const geojson = await riskResp.json();
  const stats   = statsResp.ok ? await statsResp.json() : null;
  return { geojson, stats };
}

async function fetchOverlay(name) {
  const paths = [
    `data/overlays/${name}.geojson`,
    `data/${name}.geojson`,
  ];
  for (const path of paths) {
    try {
      const resp = await fetch(path);
      if (resp.ok) return await resp.json();
    } catch (_) { /* try next */ }
  }
  return null;
}

// ── Tribal layer color + filter ────────────────────────────────────────────────

function tierFillColor() {
  return ['match', ['get', 'risk_tier'],
    'CRITICAL', TIER_COLORS.CRITICAL,
    'HIGH',     TIER_COLORS.HIGH,
    'MODERATE', TIER_COLORS.MODERATE,
    /* default */ TIER_COLORS.LOW,
  ];
}

function scoreFillColor(field) {
  return ['interpolate', ['linear'], ['coalesce', ['get', field], 0],
    0,    '#111827',
    0.25, '#14532d',
    0.5,  '#16a34a',
    0.7,  '#4ade80',
    0.85, '#86efac',
    1,    '#d9f99d',
  ];
}

function buildFillColor() {
  if (colorMode === 'tier')  return tierFillColor();
  if (colorMode === 'corp')  return scoreFillColor('corp_score');
  return tierFillColor();
}

function buildFilter() {
  const tiers = [...activeTiers];
  if (tiers.length === 4) return null;
  if (tiers.length === 0) return ['==', ['get', 'risk_tier'], '__none__'];
  return ['in', ['get', 'risk_tier'], ['literal', tiers]];
}

function applyFilter() {
  if (!map.getLayer('tribal-fill')) return;
  const f = buildFilter();
  map.setFilter('tribal-fill',    f);
  map.setFilter('tribal-outline', f);
}

function applyColor() {
  if (!map.getLayer('tribal-fill')) return;
  map.setPaintProperty('tribal-fill', 'fill-color', buildFillColor());
  renderLegend();
}

// ── Overlay layer management ───────────────────────────────────────────────────

async function toggleOverlay(name, visible) {
  const cfg = OVERLAY_CONFIG[name];
  if (!cfg) return;

  const sourceId = `overlay-${name}`;
  const layerId  = `overlay-${name}-layer`;
  const statusEl = document.getElementById(`status-${name}`);

  if (!visible) {
    if (map.getLayer(layerId)) map.setLayoutProperty(layerId, 'visibility', 'none');
    return;
  }

  if (!map.getSource(sourceId)) {
    if (statusEl) statusEl.textContent = '…';
    const geojson = await fetchOverlay(name);

    if (!geojson || !geojson.features || geojson.features.length === 0) {
      if (statusEl) statusEl.textContent = 'N/A';
      const cb = document.querySelector(`.overlay-cb[value="${name}"]`);
      if (cb) cb.checked = false;
      return;
    }

    map.addSource(sourceId, { type: 'geojson', data: geojson });

    const layerDef = {
      id: layerId,
      type: cfg.type,
      source: sourceId,
      paint: cfg.paint || {},
    };
    if (cfg.layout) layerDef.layout = cfg.layout;

    map.addLayer(layerDef, cfg.belowTribal ? 'tribal-fill' : undefined);

    map.on('mouseenter', layerId, (e) => {
      if (!e.features.length) return;
      map.getCanvas().style.cursor = 'pointer';
      const p = e.features[0].properties;
      const label = cfg.popupLabel ? cfg.popupLabel(p) : name;
      overlayPopup.setLngLat(e.lngLat)
        .setHTML(`<div class="overlay-popup-label">${label}</div>`)
        .addTo(map);
    });
    map.on('mouseleave', layerId, () => {
      map.getCanvas().style.cursor = '';
      overlayPopup.remove();
    });

    if (cfg.aboveTribal) {
      map.on('click', layerId, (e) => {
        if (!e.features.length) return;
        e.originalEvent._overlayHandled = true;
        overlayPopup.remove();
        showOverlayDetail(name, e.features[0].properties);
      });
    }

    const n = geojson.features.length;
    if (statusEl) statusEl.textContent = n.toLocaleString();
    overlayState[name] = { loaded: true, count: n };
  } else {
    map.setLayoutProperty(layerId, 'visibility', 'visible');
  }
}

// ── UI rendering ───────────────────────────────────────────────────────────────

function renderLegend() {
  const el = document.getElementById('legend');
  if (colorMode === 'tier') {
    el.innerHTML = Object.entries(TIER_COLORS).map(([tier, color]) => `
      <div class="legend-row">
        <span class="legend-swatch" style="background:${color}"></span>
        <span class="legend-label">${TIER_DISPLAY[tier]}</span>
      </div>`).join('');
  } else {
    el.innerHTML = `
      <div class="gradient-bar" style="background:linear-gradient(to right,#111827,#14532d,#16a34a,#4ade80,#d9f99d)"></div>
      <div class="gradient-labels"><span>0</span><span>Siting Score</span><span>1</span></div>`;
  }
}

function renderStats(stats) {
  const el = document.getElementById('stats-panel');
  if (!stats) { el.innerHTML = ''; return; }
  el.innerHTML = `
    <div class="stat-row"><span class="stat-label">Total tribal lands</span><span class="stat-val">${stats.total_tribal_lands}</span></div>
    <div class="stat-row"><span class="stat-label">Prime opportunity</span><span class="stat-val critical">${stats.critical_count}</span></div>
    <div class="stat-row"><span class="stat-label">Strong opportunity</span><span class="stat-val high">${stats.high_count}</span></div>
    <div class="stat-row"><span class="stat-label">Moderate opportunity</span><span class="stat-val moderate">${stats.moderate_count}</span></div>
    <div class="stat-row"><span class="stat-label">Total area (km²)</span><span class="stat-val">${Math.round(stats.total_area_km2).toLocaleString()}</span></div>
    <div class="stat-row"><span class="stat-label">Existing data centers</span><span class="stat-val">${stats.known_sites}</span></div>`;
  const headerEl = document.getElementById('header-stats');
  headerEl.textContent = `${stats.critical_count} PRIME · ${stats.high_count} STRONG · ${stats.total_tribal_lands} total`;
}

function renderTierCounts(features) {
  const counts = { CRITICAL: 0, HIGH: 0, MODERATE: 0, LOW: 0 };
  features.forEach(f => { const t = f.properties.risk_tier; if (counts[t] !== undefined) counts[t]++; });
  Object.entries(counts).forEach(([tier, n]) => {
    const el = document.getElementById(`count-${tier}`);
    if (el) el.textContent = n;
  });
}

// ── Overlay detail panel ───────────────────────────────────────────────────────

function showOverlayDetail(layerName, props) {
  const panel   = document.getElementById('detail-panel');
  const content = document.getElementById('detail-content');

  let html = '';

  if (layerName === 'known_sites') {
    const name    = props.name || '';
    const company = props.company || '';
    const display = company || name || 'Unknown';
    const subtitle = (company && name && name !== company) ? name : '';
    const status  = props.status || 'unknown';
    const statusLabel = {
      operational:        'Operational',
      under_construction: 'Under construction',
      planned:            'Planned',
      closed:             'Closed / decommissioned',
    }[status] || status;
    const statusColor = {
      operational:        '#34d399',
      under_construction: '#fbbf24',
      planned:            '#60a5fa',
      closed:             '#6b7280',
    }[status] || '#94a3b8';

    html = `
      <div class="od-icon od-icon-dc">▪</div>
      <div class="od-title">${display}</div>
      ${subtitle ? `<div class="od-subtitle">${subtitle}</div>` : ''}
      <div class="od-badge" style="color:${statusColor};border-color:${statusColor}20;background:${statusColor}18">${statusLabel}</div>
      <div class="od-section">
        ${props.mw ? `<div class="od-row"><span class="od-key">Capacity</span><span class="od-val">${Math.round(props.mw)} MW</span></div>` : ''}
        <div class="od-row"><span class="od-key">Source</span><span class="od-val">OpenStreetMap</span></div>
      </div>
      <div class="od-blurb">
        An existing data center near or on tribal lands — proof that the area already supports this type of development.
        ${company ? `Operated by <strong>${company}</strong>.` : ''}
      </div>`;

  } else if (layerName === 'land_acquisitions') {
    const buyer   = props.buyer  || 'Unknown';
    const parent  = props.resolved_parent || buyer;
    const conf    = props.confidence || '?';
    const state   = props.state || '';
    const source  = props.source || '';
    const date    = props.file_date || '';
    const confColor = conf >= 65 ? '#4ade80' : conf >= 50 ? '#fbbf24' : '#94a3b8';

    html = `
      <div class="od-icon od-icon-acq">▲</div>
      <div class="od-title">${buyer}</div>
      ${parent !== buyer ? `<div class="od-subtitle">Parent: ${parent}</div>` : ''}
      <div class="od-badge" style="color:${confColor};border-color:${confColor}20;background:${confColor}18">${conf}% confidence</div>
      <div class="od-section">
        <div class="od-row"><span class="od-key">State</span><span class="od-val">${state}</span></div>
        <div class="od-row"><span class="od-key">Signal</span><span class="od-val">${source}</span></div>
        ${date ? `<div class="od-row"><span class="od-key">Filed</span><span class="od-val">${date}</span></div>` : ''}
      </div>
      <div class="od-blurb">
        Active developer interest in this region — a positive demand signal that hyperscalers are seeking sites near tribal lands here.
      </div>`;

  } else if (layerName === 'ferc_flags') {
    const projName = props.project_name || props.applicant || 'Grid project';
    const entity   = props.applicant || '';
    const mw       = props.mw ? Math.round(props.mw) : null;
    const status   = props.status || 'Proposed';
    const tech     = props.technology || '';
    const county   = props.county || '';
    const state    = props.state || '';
    const location = [county, state].filter(Boolean).join(', ');
    const statusColor = status.toLowerCase().includes('construct') ? '#fbbf24' : '#60a5fa';

    html = `
      <div class="od-icon od-icon-grid">◆</div>
      <div class="od-title">${projName}</div>
      ${entity && entity !== projName ? `<div class="od-subtitle">${entity}</div>` : ''}
      <div class="od-badge" style="color:${statusColor};border-color:${statusColor}20;background:${statusColor}18">${status}</div>
      <div class="od-section">
        ${mw ? `<div class="od-row"><span class="od-key">Capacity</span><span class="od-val">${mw.toLocaleString()} MW</span></div>` : ''}
        ${tech ? `<div class="od-row"><span class="od-key">Technology</span><span class="od-val">${tech}</span></div>` : ''}
        ${location ? `<div class="od-row"><span class="od-key">Location</span><span class="od-val">${location}</span></div>` : ''}
        <div class="od-row"><span class="od-key">Source</span><span class="od-val">EIA Form 860M</span></div>
      </div>
      <div class="od-blurb">
        A large planned generator (${mw ? mw.toLocaleString() + ' MW' : 'significant capacity'}) near tribal lands — grid investment that directly improves siting potential for data center development.
      </div>`;
  }

  content.innerHTML = html;
  panel.classList.remove('hidden');
}

// ── Detail panel ───────────────────────────────────────────────────────────────

function fmtScore(v) {
  if (v == null) return '—';
  return Number(v).toFixed(3);
}

function fmtRaw(v, max) {
  if (v == null) return '—';
  const n = Number(v);
  const pct = Math.abs(n) / max * 100;
  return { val: n.toFixed(1), pct: Math.min(pct, 100) };
}

function showDetail(props) {
  const panel = document.getElementById('detail-panel');
  const content = document.getElementById('detail-content');
  const tierDisplay = TIER_DISPLAY[props.risk_tier] || props.risk_tier;

  let existingDcHtml = '';
  if (props.known_datacenter) {
    existingDcHtml = `<div class="known-dc-badge">
      <span class="known-dc-icon">✓</span>
      <div class="known-dc-text">
        <div class="known-dc-company">${props.known_dc_company || 'Data center present'}</div>
        <div>Existing facility — ${props.known_dc_status || 'status unknown'}</div>
      </div>
    </div>`;
  }

  function barRow(item, value) {
    const r = fmtRaw(value, item.max);
    if (r === '—') return '';
    const isPenalty = item.type === 'penalty';
    const fillClass = isPenalty ? 'penalty' : 'infra';
    return `<div class="score-bar-row">
      <span class="score-bar-label">${item.label}</span>
      <div class="score-bar-track">
        <div class="score-bar-fill ${fillClass}" style="width:${r.pct}%"></div>
      </div>
      <span class="score-bar-num">${r.val}</span>
    </div>`;
  }

  const infraRows = SCORE_LABELS.map(it => barRow(it, props[it.key])).join('');

  content.innerHTML = `
    ${existingDcHtml}
    <div class="detail-name">${props.tribe_name || '—'}</div>
    <div class="detail-fullname">${props.tribe_name_full || ''}</div>
    <span class="tier-badge ${props.risk_tier}">${tierDisplay}</span>

    <div class="score-pair">
      <div class="score-card accent">
        <div class="score-card-label">Siting Score</div>
        <div class="score-card-val">${fmtScore(props.corp_score)}</div>
      </div>
      <div class="score-card">
        <div class="score-card-label">Area (km²)</div>
        <div class="score-card-val" style="font-size:15px">${props.area_km2 ? Math.round(props.area_km2).toLocaleString() : '—'}</div>
      </div>
    </div>

    <div class="score-section-title">Infrastructure siting factors</div>
    ${infraRows}

    <div class="score-section-title" style="margin-top:12px">Sovereign advantages</div>
    <div class="hint" style="font-size:11px;color:#9ca3af;padding:4px 0">
      Tribal sovereign lands can offer streamlined permitting, custom power rate negotiation,
      Opportunity Zone tax incentives, and Section 17 corporate charter flexibility.
    </div>`;

  panel.classList.remove('hidden');
}

// ── Map initialization ─────────────────────────────────────────────────────────

function initMap(geojson) {
  map = new maplibregl.Map({
    container: 'map',
    style: {
      version: 8,
      sources: {
        esri: {
          type: 'raster',
          tiles: ['https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}'],
          tileSize: 256,
          attribution: 'Tiles © Esri &mdash; Source: Esri, Maxar, USGS',
          maxzoom: 19,
        },
      },
      layers: [{ id: 'esri-satellite', type: 'raster', source: 'esri' }],
    },
    center: [-96, 39],
    zoom: 3.8,
    minZoom: 2,
  });

  popup = new maplibregl.Popup({
    closeButton: false,
    closeOnClick: false,
    offset: 4,
    maxWidth: '240px',
  });

  overlayPopup = new maplibregl.Popup({
    closeButton: false,
    closeOnClick: false,
    offset: 4,
    maxWidth: '300px',
    className: 'overlay-popup',
  });

  map.on('load', () => {
    map.addImage('icon-dc',   makeIconImage('square',   '#22c55e'), { pixelRatio: 2 });
    map.addImage('icon-acq',  makeIconImage('triangle', '#f59e0b'), { pixelRatio: 2 });
    map.addImage('icon-grid', makeIconImage('diamond',  '#34d399'), { pixelRatio: 2 });

    map.addSource('tribal', { type: 'geojson', data: geojson });

    map.addLayer({
      id: 'tribal-fill',
      type: 'fill',
      source: 'tribal',
      paint: {
        'fill-color': buildFillColor(),
        'fill-opacity': 0.68,
      },
    });

    map.addLayer({
      id: 'tribal-outline',
      type: 'line',
      source: 'tribal',
      paint: {
        'line-color': '#fff',
        'line-width': ['interpolate', ['linear'], ['zoom'], 3, 0.2, 8, 1],
        'line-opacity': 0.25,
      },
    });

    map.addLayer({
      id: 'tribal-fill-hover',
      type: 'fill',
      source: 'tribal',
      filter: ['==', ['get', 'geoid'], ''],
      paint: {
        'fill-color': '#fff',
        'fill-opacity': 0.12,
      },
    });

    map.on('mousemove', 'tribal-fill', (e) => {
      if (!e.features.length) return;
      const intelIds = INTEL_LAYERS.map(n => `overlay-${n}-layer`).filter(id => map.getLayer(id));
      if (intelIds.length && map.queryRenderedFeatures(e.point, { layers: intelIds }).length) {
        map.getCanvas().style.cursor = 'pointer';
        popup.remove();
        map.setFilter('tribal-fill-hover', ['==', ['get', 'geoid'], '']);
        return;
      }
      map.getCanvas().style.cursor = 'pointer';
      const props = e.features[0].properties;
      const tier = props.risk_tier;
      const tierDisplay = TIER_DISPLAY[tier] || tier;
      popup.setLngLat(e.lngLat).setHTML(`
        <div class="popup-name">${props.tribe_name || '—'}</div>
        <div class="popup-tier ${tier}">${tierDisplay}</div>
        <div class="popup-score">
          <span>Siting <span class="popup-score-val">${fmtScore(props.corp_score)}</span></span>
        </div>
        <div class="popup-hint">Click for full breakdown</div>
      `).addTo(map);
      map.setFilter('tribal-fill-hover', ['==', ['get', 'geoid'], props.geoid]);
    });

    map.on('mouseleave', 'tribal-fill', () => {
      map.getCanvas().style.cursor = '';
      popup.remove();
      map.setFilter('tribal-fill-hover', ['==', ['get', 'geoid'], '']);
    });

    map.on('click', 'tribal-fill', (e) => {
      if (e.originalEvent._overlayHandled) return;
      if (!e.features.length) return;
      showDetail(e.features[0].properties);
    });

    document.getElementById('loading').classList.add('hidden');

    document.querySelectorAll('.overlay-cb:checked').forEach(cb => toggleOverlay(cb.value, true));
  });
}

// ── Sidebar wiring ─────────────────────────────────────────────────────────────

function wireSidebar() {
  document.querySelectorAll('.tier-cb').forEach(cb => {
    cb.addEventListener('change', () => {
      if (cb.checked) activeTiers.add(cb.value);
      else activeTiers.delete(cb.value);
      applyFilter();
    });
  });

  document.querySelectorAll('input[name="colorby"]').forEach(r => {
    r.addEventListener('change', () => {
      colorMode = r.value;
      applyColor();
    });
  });

  document.querySelectorAll('.overlay-cb').forEach(cb => {
    cb.addEventListener('change', () => {
      toggleOverlay(cb.value, cb.checked);
    });
  });

  document.getElementById('detail-close').addEventListener('click', () => {
    document.getElementById('detail-panel').classList.add('hidden');
  });
}

// ── Entry point ────────────────────────────────────────────────────────────────

async function main() {
  try {
    const { geojson, stats } = await loadData();
    allFeatures = geojson.features || [];
    renderTierCounts(allFeatures);
    renderStats(stats);
    renderLegend();
    wireSidebar();
    initMap(geojson);
  } catch (err) {
    document.getElementById('loading').textContent = `Error: ${err.message}`;
    console.error(err);
  }
}

main();
