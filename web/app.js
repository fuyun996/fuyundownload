/* fuyun 下载器前端 */
'use strict';

const $ = (sel) => document.querySelector(sel);

const state = {
  tasks: [],
  speedHistory: [],
  settings: null,
};

/* ---------- 格式化 ---------- */

function fmtBytes(n) {
  if (n == null || !isFinite(n) || n < 0) return '—';
  const u = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
  let i = 0;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return (i === 0 ? n.toFixed(0) : n.toFixed(1)) + ' ' + u[i];
}
function fmtSpeed(n) { return (n > 0.5 && isFinite(n)) ? fmtBytes(n) + '/s' : '—'; }
function fmtETA(s) {
  if (s == null || s < 0 || !isFinite(s)) return '--:--';
  s = Math.ceil(s);
  const h = Math.floor(s / 3600), m = Math.floor((s % 3600) / 60), x = s % 60;
  const pad = (v) => String(v).padStart(2, '0');
  return h > 0 ? `${h}:${pad(m)}:${pad(x)}` : `${pad(m)}:${pad(x)}`;
}

const STATE_TEXT = {
  pending: '排队中', probing: '连接中', downloading: '下载中',
  pausing: '暂停中', paused: '已暂停', completed: '已完成',
  failed: '失败', canceled: '已取消',
};

/* ---------- API ---------- */

async function api(path, opts) {
  const r = await fetch(path, opts);
  if (!r.ok) {
    let msg = r.statusText;
    try { msg = (await r.json()).error || msg; } catch (_) { /* ignore */ }
    throw new Error(msg);
  }
  return r.json();
}

async function addTask() {
  const input = $('#url-input');
  const url = input.value.trim();
  if (!url) return;
  hideError();
  try {
    await api('/api/tasks', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url }),
    });
    input.value = '';
  } catch (e) {
    showError('添加失败：' + e.message);
  }
}

/* ---------- 渲染（按任务 id 原地更新，避免整表重建冲掉确认菜单等临时元素） ---------- */

const cardRefs = new Map(); // id -> {root, name, badge, actions, bar, pct, speed, size, eta, conns, mini, err, state}

function render() {
  const wrap = $('#tasks');
  $('#empty').classList.toggle('hidden', state.tasks.length > 0);

  let totalSpeed = 0, doneCount = 0, activeCount = 0;
  for (const t of state.tasks) {
    totalSpeed += t.speed || 0;
    if (t.state === 'completed') doneCount++;
    if (t.state === 'downloading' || t.state === 'probing') activeCount++;
  }
  $('#global-speed').textContent = activeCount > 0 ? '▲ ' + fmtSpeed(totalSpeed) : '';
  $('#footer-summary').textContent =
    `任务 ${state.tasks.length} · 完成 ${doneCount} · 总速度 ${fmtSpeed(totalSpeed)}`;

  const seen = new Set();
  for (const t of state.tasks) {
    seen.add(t.id);
    let entry = cardRefs.get(t.id);
    if (!entry) {
      entry = createCard(t);
      cardRefs.set(t.id, entry);
    }
    updateCard(entry, t);
    wrap.appendChild(entry.root); // 按快照顺序排列（已存在节点为移动，保留子元素）
  }
  for (const [id, entry] of [...cardRefs]) {
    if (!seen.has(id)) {
      entry.root.remove();
      cardRefs.delete(id);
    }
  }
  drawSpark();
}

function createCard(t) {
  const root = document.createElement('div');
  root.className = 'task ' + t.state;
  root.dataset.id = t.id;

  const row1 = document.createElement('div');
  row1.className = 'row1';
  const name = document.createElement('span');
  name.className = 'name';
  const badge = document.createElement('span');
  badge.className = 'badge';
  const actions = document.createElement('span');
  actions.className = 'actions';
  row1.append(name, badge, actions);

  const row2 = document.createElement('div');
  row2.className = 'row2';
  const barWrap = document.createElement('div');
  barWrap.className = 'pbar-wrap';
  const bar = document.createElement('div');
  bar.className = 'pbar';
  barWrap.appendChild(bar);
  const pct = span('pct'), speed = span('speed'), size = span('size'),
        eta = span('eta'), conns = span('conns');
  row2.append(barWrap, pct, speed, size, eta, conns);

  const mini = document.createElement('div');
  mini.className = 'mini-conns';
  const err = document.createElement('div');
  err.className = 'err';

  root.append(row1, row2, mini, err);
  return { root, name, badge, actions, bar, pct, speed, size, eta, conns, mini, err, state: null };
}

function span(cls) {
  const el = document.createElement('span');
  el.className = cls;
  return el;
}

function updateCard(entry, t) {
  const nameText = t.file_name || t.url;
  if (entry.name.textContent !== nameText) {
    entry.name.textContent = nameText;
    entry.name.title = t.path || t.url;
  }
  const badgeText = STATE_TEXT[t.state] || t.state;
  if (entry.badge.textContent !== badgeText) entry.badge.textContent = badgeText;
  entry.badge.className = 'badge ' + t.state;
  const rootCls = 'task ' + t.state;
  if (entry.root.className !== rootCls) entry.root.className = rootCls;

  const pctVal = t.total_size > 0 ? (t.downloaded / t.total_size) * 100 : 0;
  entry.bar.style.width = (t.total_size > 0 ? pctVal.toFixed(2) : 0) + '%';
  entry.pct.textContent = t.total_size > 0 ? pctVal.toFixed(1) + '%' : '—';
  entry.speed.textContent = fmtSpeed(t.speed);
  entry.size.textContent = t.total_size > 0
    ? `${fmtBytes(t.downloaded)} / ${fmtBytes(t.total_size)}`
    : fmtBytes(t.downloaded);
  entry.eta.textContent = t.eta >= 0 ? 'ETA ' + fmtETA(t.eta) : '';
  entry.conns.textContent = t.state === 'downloading' ? `连接 ${t.connections}/${t.max_conns}` : '';

  // 各连接速度迷你条
  if (t.state === 'downloading' && t.conns && t.conns.length > 0) {
    renderMini(entry.mini, t.conns);
    entry.mini.style.display = '';
  } else {
    entry.mini.textContent = '';
    entry.mini.style.display = 'none';
  }

  // 错误行
  const showErr = t.error && !['paused', 'pending'].includes(t.state);
  entry.err.textContent = showErr ? t.error : '';
  entry.err.style.display = showErr ? '' : 'none';

  // 操作按钮：仅在状态变化时重建（避免每秒闪烁与误触）
  if (entry.state !== t.state) {
    entry.state = t.state;
    renderActions(entry.actions, t);
  }
}

function renderMini(el, conns) {
  el.textContent = '';
  const speeds = conns.map((c) => c.speed || 0).slice(0, 32);
  const maxC = Math.max(1, ...speeds);
  for (const sp of speeds) {
    const b = document.createElement('div');
    b.className = 'cbar';
    b.style.height = Math.max(6, (sp / maxC) * 14) + 'px';
    b.title = fmtSpeed(sp);
    el.appendChild(b);
  }
}

function renderActions(el, t) {
  el.textContent = '';
  if (['downloading', 'probing'].includes(t.state)) {
    el.appendChild(btn('⏸ 暂停', 'small', () => act(t.id, 'pause')));
  }
  if (['paused', 'failed', 'canceled'].includes(t.state)) {
    el.appendChild(btn('▶ 继续', 'small primary', () => act(t.id, 'resume')));
  }
  if (['downloading', 'probing', 'paused'].includes(t.state)) {
    el.appendChild(btn('✕ 停止', 'small ghost', () => act(t.id, 'cancel')));
  }
  el.appendChild(btn('删除', 'small ghost danger', (ev) => confirmDelete(t.id, ev)));
}

function btn(label, cls, onClick) {
  const b = document.createElement('button');
  b.className = 'btn ' + cls;
  b.textContent = label;
  b.addEventListener('click', onClick);
  return b;
}

async function act(id, action) {
  try {
    await api(`/api/tasks/${id}/${action}`, { method: 'POST' });
  } catch (e) {
    showError(action + '失败：' + e.message);
  }
}

function confirmDelete(id, ev) {
  const card = ev.target.closest('.task');
  if (!card) return;
  const old = card.querySelector('.confirm-bar');
  if (old) old.remove(); // 重复点击时重新显示

  const bar = document.createElement('div');
  bar.className = 'confirm-bar';
  bar.append(
    document.createTextNode('删除该任务记录？'),
    btn('仅移除记录', 'small', async () => {
      try {
        await api(`/api/tasks/${id}`, { method: 'DELETE' });
      } catch (e) {
        showError('删除失败：' + e.message);
      }
      bar.remove();
    }),
    btn('删除记录和文件', 'small danger', async () => {
      try {
        await api(`/api/tasks/${id}?files=1`, { method: 'DELETE' });
      } catch (e) {
        showError('删除失败：' + e.message);
      }
      bar.remove();
    }),
    btn('取消', 'small ghost', () => bar.remove()),
  );
  card.appendChild(bar);
}

/* ---------- 速度曲线 ---------- */

function drawSpark() {
  const canvas = $('#spark');
  const dpr = window.devicePixelRatio || 1;
  const w = canvas.clientWidth, h = canvas.clientHeight;
  if (canvas.width !== w * dpr) { canvas.width = w * dpr; }
  if (canvas.height !== h * dpr) { canvas.height = h * dpr; }
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);

  const hist = state.speedHistory;
  if (hist.length < 2) return;
  const max = Math.max(1, ...hist);
  ctx.beginPath();
  hist.forEach((v, i) => {
    const x = (i / (hist.length - 1)) * w;
    const y = h - (v / max) * (h - 6) - 2;
    if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
  });
  ctx.strokeStyle = '#4f8cff';
  ctx.lineWidth = 1.6;
  ctx.stroke();
  ctx.lineTo(w, h);
  ctx.lineTo(0, h);
  ctx.closePath();
  ctx.fillStyle = 'rgba(79, 140, 255, .12)';
  ctx.fill();
}

/* ---------- 设置 ---------- */

async function openSettings() {
  try {
    state.settings = await api('/api/settings');
  } catch (e) {
    showError('读取设置失败：' + e.message);
    return;
  }
  const s = state.settings;
  $('#set-dir').value = s.download_dir || '';
  $('#set-conns').value = s.max_conns_per_task;
  $('#set-tasks').value = s.max_tasks;
  $('#set-global').value = s.max_global_conns;
  $('#set-port').value = s.port || 0;
  $('#set-ua').value = s.user_agent || '';
  $('#set-headers').value = Object.entries(s.headers || {})
    .map(([k, v]) => `${k}: ${v}`).join('\n');
  $('#set-insecure').checked = !!s.insecure;
  $('#settings-modal').classList.remove('hidden');
}

async function saveSettings() {
  const headers = {};
  for (const line of $('#set-headers').value.split('\n')) {
    const i = line.indexOf(':');
    if (i > 0) {
      headers[line.slice(0, i).trim()] = line.slice(i + 1).trim();
    }
  }
  const body = {
    download_dir: $('#set-dir').value.trim(),
    max_conns_per_task: +$('#set-conns').value || 32,
    max_tasks: +$('#set-tasks').value || 3,
    max_global_conns: +$('#set-global').value || 64,
    port: +$('#set-port').value || 0,
    user_agent: $('#set-ua').value.trim(),
    headers,
    insecure: $('#set-insecure').checked,
  };
  try {
    state.settings = await api('/api/settings', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    $('#settings-modal').classList.add('hidden');
  } catch (e) {
    showError('保存设置失败：' + e.message);
  }
}

/* ---------- 错误提示 ---------- */

let errorTimer = null;
function showError(msg) {
  const bar = $('#error-bar');
  bar.textContent = msg;
  bar.classList.remove('hidden');
  clearTimeout(errorTimer);
  errorTimer = setTimeout(hideError, 8000);
}
function hideError() { $('#error-bar').classList.add('hidden'); }

/* ---------- SSE 与初始化 ---------- */

function onFrame(list) {
  state.tasks = list || [];
  let total = 0;
  for (const t of state.tasks) total += t.speed || 0;
  state.speedHistory.push(total);
  if (state.speedHistory.length > 180) state.speedHistory.shift();
  render();
}

function init() {
  $('#btn-add').addEventListener('click', addTask);
  $('#url-input').addEventListener('keydown', (e) => {
    if (e.key === 'Enter') addTask();
  });
  $('#btn-settings').addEventListener('click', openSettings);
  $('#btn-settings-cancel').addEventListener('click', () => $('#settings-modal').classList.add('hidden'));
  $('#btn-settings-save').addEventListener('click', saveSettings);
  $('#settings-modal').addEventListener('click', (e) => {
    if (e.target === e.currentTarget) $('#settings-modal').classList.add('hidden');
  });

  const es = new EventSource('/api/events');
  es.onmessage = (e) => {
    try { onFrame(JSON.parse(e.data)); } catch (_) { /* ignore */ }
  };
  es.onerror = () => { /* EventSource 自动重连 */ };

  window.addEventListener('resize', drawSpark);
  fetch('/api/tasks').then((r) => r.json()).then(onFrame).catch(() => {});
}

init();
