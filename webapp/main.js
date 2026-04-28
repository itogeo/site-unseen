'use strict';

const TIER_COLORS = {
  CRITICAL: '#ff3b30',
  HIGH:     '#ff6b35',
  MODERATE: '#ffd166',
  LOW:      '#4a5568',
};

const SCORE_LABELS = {
  corp: [
    { key: 'score_transmission',  label: 'Transmission lines',  type: 'corp', max: 20 },
    { key: 'score_substation',    label: 'Substation proximity', type: 'corp', max: 15 },
    { key: 'score_water',         label: 'Water availability',  type: 'corp', max: 20 },
    { key: 'score_aquifer',       label: 'Aquifer access',      type: 'corp', max: 10 },
    { key: 'score_land_area',     label: 'Land area',           type: 'corp', max: 15 },
    { key: 'score_terrain',       label: 'Terrain flatness',    type: 'corp', max: 10 },
    { key: 'score_opp_zone',      label: 'Opportunity zone',    type: 'corp', max: 5  },
    { key: 'score_flood_penalty', label: 'Flood risk',          type: 'penalty', max: 10 },
  ],
  vuln: [
    { key: 'score_poverty',          label: 'Poverty rate',          type: 'vuln', max: 25 },
    { key: 'score_ejscreen',         label: 'EJScreen burden',       type: 'vuln', max: 20 },
    { key: 'score_sacrifice_history',label: 'Industry history',      type: 'vuln', max: 15 },
    { key: 'score_remoteness',       label: 'Remoteness',            type: 'vuln', max: 10 },
    { key: 'score_jurisdiction',     label: 'Jurisdictional fragility', type: 'vuln', max: 10 },
  ],
};

// Overlay layer configuration — each entry defines how to render + label it
const OVERLAY_CONFIG = {
  transmission_lines: {
    label: 'Transmission Lines',
    type: 'line',
    paint: {
      'line-color': '#f0c040',
      'line-width': ['interpolate', ['linear'], ['zoom'], 3, 0.6, 8, 2],
      'line-opacity': 0.75,
    },
    popupProp: 'VOLTAGE',
    popupLabel: (p) => `${p.VOLTAGE || '?'}kV — ${p.TYPE || ''} ${p.STATUS || ''}`.trim(),
  },
  substations: {
    label: 'Substations',
    type: 'circle',
    paint: {
      'circle-color': '#ffa500',
      'circle-radius': ['interpolate', ['linear'], ['zoom'], 4, 3, 10, 7],
      'circle-opacity': 0.8,
      'circle-stroke-color': '#fff',
      'circle-stroke-width': 0.5,
    },
    popupLabel: (p) => `${p.NAME || 'Substation'} — ${p.MAX_VOLT || '?'}kV`,
  },
  power_plants: {
    label: 'Power Plants',
    type: 'circle',
    paint: {
      'circle-color': [
        'match', ['get', 'TYPE'],
        'NATURAL GAS', '#c084fc',
        'NUCLEAR', '#f43f5e',
        'COAL', '#78716c',
        'HYDRO', '#38bdf8',
        'WIND', '#86efac',
        'SOLAR', '#fde68a',
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
    type: 'circle',
    paint: {
      'circle-color': '#6ee7b7',
      'circle-radius': ['interpolate', ['linear'], ['zoom'], 4, 2, 10, 5],
      'circle-opacity': 0.8,
      'circle-stroke-color': '#fff',
      'circle-stroke-width': 0.5,
    },
    popupLabel: (p) => `${p.p_name || 'Wind Turbine'}${p.t_cap ? ' — ' + p.t_cap + ' kW' : ''}${p.p_year ? ' (' + p.p_year + ')' : ''}`,
  },
  gas_pipelines: {
    label: 'Natural Gas Pipelines',
    type: 'line',
    paint: {
      'line-color': '#22d3ee',
      'line-width': ['interpolate', ['linear'], ['zoom'], 3, 0.6, 8, 1.8],
      'line-opacity': 0.65,
      'line-dasharray': [3, 2],
    },
    popupLabel: (p) => `${p.Operator || 'Gas Pipeline'} — ${p.Type || ''}`.trim(),
  },
  known_sites: {
    label: 'Known Data Centers',
    type: 'circle',
    paint: {
      'circle-color': '#ff3b30',
      'circle-radius': 7,
      'circle-opacity': 1,
      'circle-stroke-color': '#fff',
      'circle-stroke-width': 1.5,
    },
    popupLabel: (p) => `${p.company || p.name || 'Data Center'} — ${p.status || ''}`,
  },
  land_acquisitions: {
    label: 'Flagged Acquisitions',
    type: 'circle',
    paint: {
      'circle-color': '#f59e0b',
      'circle-radius': 8,
      'circle-opacity': 0.9,
      'circle-stroke-color': '#fff',
      'circle-stroke-width': 1.5,
    },
    popupLabel: (p) => `${p.buyer || 'Unknown buyer'} → ${p.resolved_parent || '?'} (conf ${p.confidence || '?'}%)`,
  },
  ferc_flags: {
    label: 'FERC Queue Flags',
    type: 'circle',
    paint: {
      'circle-color': '#34d399',
      'circle-radius': 7,
      'circle-opacity': 0.9,
      'circle-stroke-color': '#fff',
      'circle-stroke-width': 1.5,
    },
    popupLabel: (p) => `FERC: ${p.project_name || p.applicant || 'Queue entry'} — ${p.mw || '?'} MW`,
  },
};

let map, popup, overlayPopup;
let allFeatures = [];
let activeTiers = new Set(['CRITICAL', 'HIGH', 'MODERATE', 'LOW']);
let colorMode = 'tier';
// Track which overlays are loaded (data fetched) and visible
const overlayState = {};

// ── Data loading ──────────────────────────────────────────────────────────────

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
  // Try overlays/ subfolder first, then root data/
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

// ── Tribal layer color + filter ───────────────────────────────────────────────

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
    0,   '#1a2035',
    0.25,'#1e3a5f',
    0.5, '#2d6a9f',
    0.7, '#ffd166',
    0.85,'#ff6b35',
    1,   '#ff3b30',
  ];
}

function buildFillColor() {
  if (colorMode === 'tier')     return tierFillColor();
  if (colorMode === 'combined') return scoreFillColor('combined_score');
  if (colorMode === 'corp')     return scoreFillColor('corp_score');
  if (colorMode === 'vuln')     return scoreFillColor('vuln_score');
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

// ── Overlay layer management ──────────────────────────────────────────────────

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

  // First time: load data and add source+layer
  if (!map.getSource(sourceId)) {
    if (statusEl) statusEl.textContent = '…';
    const geojson = await fetchOverlay(name);

    if (!geojson || !geojson.features || geojson.features.length === 0) {
      if (statusEl) statusEl.textContent = 'N/A';
      // Uncheck the checkbox since data isn't available
      const cb = document.querySelector(`.overlay-cb[value="${name}"]`);
      if (cb) cb.checked = false;
      return;
    }

    map.addSource(sourceId, { type: 'geojson', data: geojson });

    const layerDef = {
      id: layerId,
      type: cfg.type,
      source: sourceId,
      paint: cfg.paint,
    };
    // Insert overlay layers below tribal fill so tribal lands stay on top
    map.addLayer(layerDef, 'tribal-fill');

    // Hover popup for overlay features
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

    const n = geojson.features.length;
    if (statusEl) statusEl.textContent = n.toLocaleString();
    overlayState[name] = { loaded: true, count: n };
  } else {
    map.setLayoutProperty(layerId, 'visibility', 'visible');
  }
}

// ── UI rendering ──────────────────────────────────────────────────────────────

function renderLegend() {
  const el = document.getElementById('legend');
  if (colorMode === 'tier') {
    el.innerHTML = Object.entries(TIER_COLORS).map(([tier, color]) => `
      <div class="legend-row">
        <span class="legend-swatch" style="background:${color}"></span>
        <span class="legend-label">${tier.charAt(0) + tier.slice(1).toLowerCase()}</span>
      </div>`).join('');
  } else {
    const label = colorMode === 'combined' ? 'Combined Score'
                : colorMode === 'corp'     ? 'Corp Score'
                : 'Vuln Score';
    el.innerHTML = `
      <div class="gradient-bar" style="background:linear-gradient(to right,#1a2035,#2d6a9f,#ffd166,#ff3b30)"></div>
      <div class="gradient-labels"><span>0</span><span>${label}</span><span>1</span></div>`;
  }
}

function renderStats(stats) {
  const el = document.getElementById('stats-panel');
  if (!stats) { el.innerHTML = ''; return; }
  el.innerHTML = `
    <div class="stat-row"><span class="stat-label">Total tribal lands</span><span class="stat-val">${stats.total_tribal_lands}</span></div>
    <div class="stat-row"><span class="stat-label">Critical risk</span><span class="stat-val critical">${stats.critical_count}</span></div>
    <div class="stat-row"><span class="stat-label">High risk</span><span class="stat-val high">${stats.high_count}</span></div>
    <div class="stat-row"><span class="stat-label">Moderate risk</span><span class="stat-val moderate">${stats.moderate_count}</span></div>
    <div class="stat-row"><span class="stat-label">Total area (km²)</span><span class="stat-val">${Math.round(stats.total_area_km2).toLocaleString()}</span></div>
    <div class="stat-row"><span class="stat-label">Known data centers</span><span class="stat-val">${stats.known_sites}</span></div>`;
  const headerEl = document.getElementById('header-stats');
  headerEl.textContent = `${stats.high_count} HIGH · ${stats.moderate_count} MODERATE · ${stats.total_tribal_lands} total`;
}

function renderTierCounts(features) {
  const counts = { CRITICAL: 0, HIGH: 0, MODERATE: 0, LOW: 0 };
  features.forEach(f => { const t = f.properties.risk_tier; if (counts[t] !== undefined) counts[t]++; });
  Object.entries(counts).forEach(([tier, n]) => {
    const el = document.getElementById(`count-${tier}`);
    if (el) el.textContent = n;
  });
}

// ── Detail panel ──────────────────────────────────────────────────────────────

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

  let knownHtml = '';
  if (props.known_datacenter) {
    knownHtml = `<div class="known-dc-badge">
      <span class="known-dc-icon">⚠</span>
      <div class="known-dc-text">
        <div class="known-dc-company">${props.known_dc_company || 'Unknown company'}</div>
        <div>Known data center — ${props.known_dc_status || 'status unknown'}</div>
      </div>
    </div>`;
  }

  // Impact metrics (if available from impact_metrics run)
  let impactHtml = '';
  if (props.water_annual_millions) {
    impactHtml = `
    <div class="score-section-title">Projected impacts (single hyperscale DC)</div>
    <div class="impact-grid">
      <div class="impact-card">
        <div class="impact-val">${props.water_annual_millions}M</div>
        <div class="impact-label">gal/yr water</div>
      </div>
      <div class="impact-card">
        <div class="impact-val">${props.jobs_permanent_actual || 3}</div>
        <div class="impact-label">permanent jobs</div>
      </div>
      <div class="impact-card">
        <div class="impact-val">+${props.elec_rate_increase_low_pct || 50}–${props.elec_rate_increase_high_pct || 267}%</div>
        <div class="impact-label">elec rate increase</div>
      </div>
      <div class="impact-card">
        <div class="impact-val">${props.heat_island_max_f || 16}°F</div>
        <div class="impact-label">heat island</div>
      </div>
    </div>`;
  }

  function barRow(item, value) {
    const r = fmtRaw(value, item.max);
    if (r === '—') return '';
    const isPenalty = item.type === 'penalty';
    const fillClass = isPenalty ? 'penalty' : item.type;
    return `<div class="score-bar-row">
      <span class="score-bar-label">${item.label}</span>
      <div class="score-bar-track">
        <div class="score-bar-fill ${fillClass}" style="width:${r.pct}%"></div>
      </div>
      <span class="score-bar-num">${r.val}</span>
    </div>`;
  }

  const corpRows = SCORE_LABELS.corp.map(it => barRow(it, props[it.key])).join('');
  const vulnRows = SCORE_LABELS.vuln.map(it => barRow(it, props[it.key])).join('');

  content.innerHTML = `
    ${knownHtml}
    <div class="detail-name">${props.tribe_name || '—'}</div>
    <div class="detail-fullname">${props.tribe_name_full || ''}</div>
    <span class="tier-badge ${props.risk_tier}">${props.risk_tier}</span>

    <div class="score-pair">
      <div class="score-card accent">
        <div class="score-card-label">Combined</div>
        <div class="score-card-val">${fmtScore(props.combined_score)}</div>
      </div>
      <div class="score-card">
        <div class="score-card-label">Area (km²)</div>
        <div class="score-card-val" style="font-size:15px">${props.area_km2 ? Math.round(props.area_km2).toLocaleString() : '—'}</div>
      </div>
    </div>

    <div class="score-pair">
      <div class="score-card">
        <div class="score-card-label">Corp Score</div>
        <div class="score-card-val" style="color:var(--c-high);font-size:16px">${fmtScore(props.corp_score)}</div>
      </div>
      <div class="score-card">
        <div class="score-card-label">Vuln Score</div>
        <div class="score-card-val" style="color:#a78bfa;font-size:16px">${fmtScore(props.vuln_score)}</div>
      </div>
    </div>

    ${impactHtml}

    <div class="score-section-title">Corporate attractiveness factors</div>
    ${corpRows}

    <div class="score-section-title">Community vulnerability factors</div>
    ${vulnRows}`;

  panel.classList.remove('hidden');
}

// ── Map initialization ────────────────────────────────────────────────────────

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
    maxWidth: '280px',
    className: 'overlay-popup',
  });

  map.on('load', () => {
    map.addSource('tribal', {
      type: 'geojson',
      data: geojson,
    });

    map.addLayer({
      id: 'tribal-fill',
      type: 'fill',
      source: 'tribal',
      paint: {
        'fill-color': buildFillColor(),
        'fill-opacity': 0.72,
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
      map.getCanvas().style.cursor = 'pointer';
      const props = e.features[0].properties;
      const tier = props.risk_tier;
      popup.setLngLat(e.lngLat).setHTML(`
        <div class="popup-name">${props.tribe_name || '—'}</div>
        <div class="popup-tier ${tier}">${tier}</div>
        <div class="popup-score">
          <span>Combined <span class="popup-score-val">${fmtScore(props.combined_score)}</span></span>
          <span>Corp <span class="popup-score-val">${fmtScore(props.corp_score)}</span></span>
          <span>Vuln <span class="popup-score-val">${fmtScore(props.vuln_score)}</span></span>
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
      if (!e.features.length) return;
      showDetail(e.features[0].properties);
    });

    document.getElementById('loading').classList.add('hidden');

    // Auto-load known_sites (checked by default)
    const defaultOn = document.querySelectorAll('.overlay-cb:checked');
    defaultOn.forEach(cb => toggleOverlay(cb.value, true));
  });
}

// ── Sidebar wiring ────────────────────────────────────────────────────────────

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

// ── Entry point ───────────────────────────────────────────────────────────────

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
