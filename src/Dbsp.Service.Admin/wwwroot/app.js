/* Dbsp admin — a client of the JSON API beside it (api/). Hand-written: no build
   step, no dependencies, no network requests except to that API.

   Shape: one store, one router, one render pass. A view is { load, render }; render builds
   a fresh DOM tree from the store and `morph` patches the page to match it, so live
   updates neither flash nor move focus. Handlers are properties (`_on`) found by one
   delegated listener, which is what lets a patched node keep working. */
(() => {
  'use strict';

  const API = new URL('api/', document.baseURI).href;
  const $ = id => document.getElementById(id);

  // ── DOM ─────────────────────────────────────────────────────────────────────────────

  const SVG_NS = 'http://www.w3.org/2000/svg';

  function build(el, props, kids) {
    if (props) {
      for (const [k, v] of Object.entries(props)) {
        if (v == null || v === false) continue;
        if (k === 'on') el._on = v;
        else if (k === 'props') el._props = v;
        else if (k === 'class') el.setAttribute('class', Array.isArray(v) ? v.filter(Boolean).join(' ') : v);
        else if (k === 'key') el.setAttribute('data-key', v);
        else if (k === 'value' && el.namespaceURI !== SVG_NS) { el.setAttribute('value', v); el.value = v; }
        else el.setAttribute(k, v === true ? '' : v);
      }
    }
    const add = kid => {
      if (kid == null || kid === false) return;
      if (Array.isArray(kid)) kid.forEach(add);
      else el.append(kid instanceof Node ? kid : document.createTextNode(String(kid)));
    };
    kids.forEach(add);
    return el;
  }
  const h = (tag, props, ...kids) => build(document.createElement(tag), props, kids);
  const s = (tag, props, ...kids) => build(document.createElementNS(SVG_NS, tag), props, kids);

  /** Makes `a` equal to `b`, touching only what differs. Returns the node now in the page. */
  function morph(a, b) {
    if (a.nodeType !== b.nodeType || a.nodeName !== b.nodeName ||
        (a.nodeType === 1 && a.getAttribute('data-key') !== b.getAttribute('data-key'))) {
      a.replaceWith(b);
      return b;
    }
    if (a.nodeType !== 1) {
      if (a.data !== b.data) a.data = b.data;
      return a;
    }
    a._on = b._on;
    a._props = b._props;
    if (a.hasAttribute('data-keep') && b.hasAttribute('data-keep')) return a;
    const tag = a.tagName;
    // Whether a <details> is open is the reader's choice once it is on the page.
    const keep = tag === 'DETAILS' ? 'open' : null;
    for (const { name, value } of [...b.attributes]) {
      if (name === keep || a.getAttribute(name) === value) continue;
      // The page's content security policy refuses style attributes; the CSSOM is allowed.
      if (name === 'style') a.style.cssText = value;
      else a.setAttribute(name, value);
    }
    for (const { name } of [...a.attributes]) if (name !== keep && !b.hasAttribute(name)) a.removeAttribute(name);
    // A form control shows its properties, not its attributes: once a select has been
    // chosen from, a box ticked or a field typed in, the browser stops following
    // `selected`, `checked` and `value` in the markup. So the properties are set here,
    // from the new tree, and the control shows what the model says whatever was done to it.
    if (tag === 'SELECT') {
      // Read before the options are patched: an option the page did not have is moved
      // out of `b`, and `b` no longer knows it was the selected one.
      const selected = [...b.options].map(o => o.selected);
      morphKids(a, b);
      // After the patch the options are b's, in b's order.
      const options = a.options;
      if (a.multiple) {
        for (let i = 0; i < options.length; i++) if (options[i].selected !== !!selected[i]) options[i].selected = !!selected[i];
      } else {
        const index = selected.indexOf(true);
        if (a.selectedIndex !== index) a.selectedIndex = index;
      }
      return a;
    }
    if (tag === 'INPUT' || tag === 'TEXTAREA') {
      if (a.type === 'checkbox' || a.type === 'radio') {
        const checked = b.hasAttribute('checked');
        if (a.checked !== checked) a.checked = checked;
      }
      // What is being typed is the one thing left alone: the model follows it at each
      // keystroke, and writing it back would move the caret.
      else if (document.activeElement !== a && a.value !== b.value) a.value = b.value;
      return a;
    }
    morphKids(a, b);
    return a;
  }

  function morphKids(a, b) {
    const keyed = new Map();
    for (const c of a.children) {
      const k = c.getAttribute('data-key');
      if (k != null) keyed.set(k, c);
    }
    let cur = a.firstChild;
    for (const nb of [...b.childNodes]) {
      const key = nb.nodeType === 1 ? nb.getAttribute('data-key') : null;
      let match = null;
      if (key != null) {
        match = keyed.get(key) || null;
        keyed.delete(key);
      } else if (cur && cur.nodeType === nb.nodeType && cur.nodeName === nb.nodeName &&
                 !(cur.nodeType === 1 && cur.hasAttribute('data-key'))) {
        match = cur;
      }
      if (match) {
        if (match !== cur) a.insertBefore(match, cur);
        else cur = cur.nextSibling;
        morph(match, nb);
      } else {
        a.insertBefore(nb, cur);
      }
    }
    while (cur) {
      const next = cur.nextSibling;
      a.removeChild(cur);
      cur = next;
    }
  }

  for (const type of ['click', 'input', 'change', 'submit', 'keydown', 'pointermove', 'pointerout', 'dragover', 'dragleave', 'drop']) {
    document.addEventListener(type, e => {
      for (let n = e.target; n && n !== document; n = n.parentNode) {
        const fn = n._on && n._on[type];
        if (fn) {
          fn(e, n);
          if (e.cancelBubble) return;
        }
      }
    });
  }

  // ── icons: 16×16, stroke ────────────────────────────────────────────────────────────

  const ICONS = {
    overview: 'M2.5 2.5h4.5v4.5h-4.5zM9 2.5h4.5v4.5H9zM2.5 9h4.5v4.5h-4.5zM9 9h4.5v4.5H9z',
    pipelines: 'M2 4h4M10 4h4M6 4a2 2 0 0 1 4 0a2 2 0 0 1-4 0M2 12h2M8 12h6M4 12a2 2 0 0 1 4 0a2 2 0 0 1-4 0',
    tables: 'M2.5 3.5h11v9h-11zM2.5 6.5h11M2.5 9.5h11M6.5 6.5v6',
    plugins: 'M6 2.5h4v2.2a1.3 1.3 0 1 0 0 2.6V9.5H7.8a1.3 1.3 0 1 1-2.6 0H2.5v-4H5a1.3 1.3 0 1 0 1-3zM2.5 9.5v4h8v-4',
    dead: 'M2.5 9.5l2-6h7l2 6v3.5h-11zM2.5 9.5h3.2l.8 1.5h3l.8-1.5h3.2M6.5 5l3 3M9.5 5l-3 3',
    cluster: 'M2.5 3h4.5v4h-4.5zM9 9h4.5v4H9zM7 5h2.5a1.5 1.5 0 0 1 1.5 1.5V9M9 11H6.5A1.5 1.5 0 0 1 5 9.5V7',
    audit: 'M4 2.5h8v11H4zM6 5.5h4M6 8h4M6 10.5h2.5',
    play: 'M5 3.5v9l7.5-4.5z',
    stop: 'M4 4h8v8H4z',
    restart: 'M12.5 8a4.5 4.5 0 1 1-1.4-3.2M12.5 2.5v2.6H9.9',
    reset: 'M3.5 8a4.5 4.5 0 1 0 1.4-3.2M3.5 2.5v2.6h2.6M8 5.5v3M8 10.6v.2',
    trash: 'M3 4.5h10M6.5 4.5V3h3v1.5M4.5 4.5l.6 8.5h5.8l.6-8.5M6.8 7v3.5M9.2 7v3.5',
    x: 'M4 4l8 8M12 4l-8 8',
    check: 'M3 8.5l3.2 3L13 4.5',
    alert: 'M8 2.2l6 10.8H2zM8 6.5v3M8 11.2v.2',
    clock: 'M8 2.5a5.5 5.5 0 1 0 0 11a5.5 5.5 0 0 0 0-11zM8 5v3.2l2.2 1.3',
    pause: 'M5.5 3.5v9M10.5 3.5v9',
    circle: 'M8 3a5 5 0 1 0 0 10A5 5 0 0 0 8 3z',
    dots: 'M3.5 8h.1M8 8h.1M12.5 8h.1',
    upload: 'M8 10.5v-7M5 6l3-3 3 3M3 10.5v2.5h10v-2.5',
    sun: 'M8 5.2a2.8 2.8 0 1 0 0 5.6a2.8 2.8 0 0 0 0-5.6zM8 1.5v1.6M8 12.9v1.6M1.5 8h1.6M12.9 8h1.6M3.4 3.4l1.1 1.1M11.5 11.5l1.1 1.1M3.4 12.6l1.1-1.1M11.5 4.5l1.1-1.1',
    moon: 'M13 9.6A5.5 5.5 0 0 1 6.4 3a5.5 5.5 0 1 0 6.6 6.6z',
    auto: 'M8 2.5a5.5 5.5 0 1 0 0 11a5.5 5.5 0 0 0 0-11zM8 2.5v11M8 4.5l3.8 1.2M8 8l5 .2M8 11.5l3.8-1',
    link: 'M9.5 3h3.5v3.5M13 3L7.5 8.5M11 9.5V13H3V5h3.5',
    copy: 'M5.5 5.5h7v7h-7zM3.5 10.5v-7h7',
    crown: 'M2.5 11.5l-.8-6 3.5 2.6L8 3.5l2.8 4.6 3.5-2.6-.8 6zM2.8 13.5h10.4',
    follow: 'M2.5 8h6M6 5l3 3-3 3M11.5 3.5v9M13.5 5v6',
    lock: 'M4 7.5h8v6H4zM5.5 7.5V5.5a2.5 2.5 0 0 1 5 0v2',
    info: 'M8 2.5a5.5 5.5 0 1 0 0 11a5.5 5.5 0 0 0 0-11zM8 7.2v3.6M8 5v.2',
    plus: 'M8 3v10M3 8h10',
    down: 'M4 6l4 4 4-4',
    user: 'M8 8.2a2.7 2.7 0 1 0 0-5.4a2.7 2.7 0 0 0 0 5.4zM3 13.5c.5-2.4 2.5-3.6 5-3.6s4.5 1.2 5 3.6',
    diff: 'M5 2.5v7M2.5 6H7.5M9 12h5M11 4.5l3 0M3 13.5l10-11',
  };
  const FILLED = new Set(['play', 'stop']);
  function icon(name, size = 14) {
    return s('svg', { viewBox: '0 0 16 16', width: size, height: size, 'aria-hidden': 'true', focusable: 'false' },
      s('path', {
        d: ICONS[name] || ICONS.circle, fill: FILLED.has(name) ? 'currentColor' : 'none', stroke: 'currentColor',
        'stroke-width': 1.5, 'stroke-linecap': 'round', 'stroke-linejoin': 'round',
      }));
  }

  // ── formatting: numbers for people ──────────────────────────────────────────────────

  const nf = new Intl.NumberFormat('en-US');
  const trim = (n, digits) => String(+n.toFixed(digits));
  function fmtNum(n) {
    if (n == null || Number.isNaN(n)) return '–';
    const a = Math.abs(n);
    if (a < 1000) return trim(n, a < 10 && !Number.isInteger(n) ? 1 : 0);
    if (a < 10_000) return nf.format(Math.round(n));
    if (a < 1e6) return trim(n / 1e3, a < 1e5 ? 1 : 0) + 'k';
    if (a < 1e9) return trim(n / 1e6, a < 1e8 ? 1 : 0) + 'M';
    return trim(n / 1e9, 1) + 'B';
  }
  const fmtInt = n => (n == null ? '–' : nf.format(n));
  function fmtMs(ms) {
    if (ms == null || Number.isNaN(ms)) return '–';
    if (ms < 1) return trim(ms * 1000, 0) + ' µs';
    if (ms < 1000) return trim(ms, ms < 10 ? 2 : ms < 100 ? 1 : 0) + ' ms';
    if (ms < 60_000) return trim(ms / 1000, ms < 10_000 ? 1 : 0) + ' s';
    if (ms < 3_600_000) return trim(ms / 60_000, 0) + ' min';
    return trim(ms / 3_600_000, 1) + ' h';
  }
  function fmtBytes(n) {
    if (n == null) return '–';
    if (n < 1024) return n + ' B';
    if (n < 1024 ** 2) return trim(n / 1024, 1) + ' KB';
    if (n < 1024 ** 3) return trim(n / 1024 ** 2, 1) + ' MB';
    return trim(n / 1024 ** 3, 1) + ' GB';
  }
  function ago(time) {
    if (!time) return '–';
    const sec = Math.max(0, (Date.now() - new Date(time).getTime()) / 1000);
    if (sec < 2) return 'just now';
    if (sec < 60) return Math.floor(sec) + ' s ago';
    if (sec < 3600) return Math.floor(sec / 60) + ' min ago';
    if (sec < 86_400) return Math.floor(sec / 3600) + ' h ago';
    return Math.floor(sec / 86_400) + ' d ago';
  }
  const clock = time => (time ? new Date(time).toLocaleTimeString([], { hour12: false }) : '–');
  const stamp = time => (time ? new Date(time).toLocaleString([], { hour12: false }) : '–');
  const timeEl = (time, text) => h('time', { datetime: time, title: stamp(time) }, text || ago(time));
  const plural = (n, one, many) => `${fmtInt(n)} ${n === 1 ? one : many || one + 's'}`;
  const short = (v, n = 8) => (v && v.length > n ? v.slice(0, n) : v || '');
  function pretty(value) {
    if (value == null) return '';
    if (typeof value === 'string') {
      try { return JSON.stringify(JSON.parse(value), null, 2); } catch { return value; }
    }
    return JSON.stringify(value, null, 2);
  }

  // ── what states look like: a word, a shape and a colour ─────────────────────────────

  const PIPELINE_STATES = {
    Running: ['good', 'play', 'Processing its inputs'],
    Starting: ['info', 'dots', 'Being built and started'],
    Stopping: ['idle', 'dots', 'Draining and committing'],
    Stopped: ['idle', 'stop', 'Stopped on purpose'],
    Backoff: ['warn', 'clock', 'Faulted; waiting to restart'],
    Blocked: ['serious', 'pause', 'Waiting on something it needs'],
    Failed: ['crit', 'x', 'Faulted too often; needs a person'],
    NeedsReset: ['crit', 'alert', 'Its state cannot be read; reset it'],
    Wedged: ['crit', 'alert', 'A teardown did not finish on this node'],
    Unbuilt: ['idle', 'circle', 'Not built on this node; built when it takes over'],
    Built: ['info', 'circle', 'Built on this standby; a takeover starts it'],
  };
  const STATE_ORDER = ['Failed', 'Wedged', 'NeedsReset', 'Blocked', 'Backoff', 'Starting', 'Stopping', 'Running', 'Built', 'Unbuilt', 'Stopped'];
  const TABLE_STATES = {
    Open: ['good', 'check', 'Open'],
    Opening: ['info', 'dots', 'Being opened from its log'],
    LoaderFaulted: ['serious', 'alert', 'Its loader faulted; the rows are the last version loaded'],
    Faulted: ['crit', 'x', 'Faulted; it will be reopened'],
    Failed: ['crit', 'x', 'Faulted too often; reopen it by hand'],
  };
  const badge = (table, state) => {
    const [cls, ic, hint] = table[state] || ['idle', 'circle', ''];
    return h('span', { class: ['badge', cls], title: hint }, icon(ic, 12), state || 'Unknown');
  };
  /** A node that no longer leads runs nothing: what it shows of pipelines is what it last saw. */
  const lastKnown = () => !!store.node && (store.node.role === 'Fenced' || store.node.role === 'Resigned');
  const pipelineBadge = state => (lastKnown()
    ? h('span', { class: 'badge last-known', title: `This node no longer leads and runs nothing. ${state} is the state it last saw; the leader has the current one.` },
      icon('clock', 12), state || 'Unknown', h('span', { class: 'lk' }, 'last known'))
    : badge(PIPELINE_STATES, state));
  const tableBadge = state => badge(TABLE_STATES, state);

  const ROLES = {
    Leader: ['leader', 'crown', 'Leader'],
    Standby: ['standby', 'follow', 'Standby'],
    Fenced: ['fenced', 'lock', 'Fenced'],
    Resigned: ['resigned', 'stop', 'Resigned'],
  };
  function roleBadge(role, big) {
    const [cls, ic, label] = ROLES[role] || ['unknown', 'circle', role || 'Unknown'];
    return h('span', { class: ['role', cls, big && 'big'] }, icon(ic, big ? 16 : 13), label);
  }

  const REASONS = {
    'pipeline-failed': 'A pipeline has failed and will not restart by itself',
    'pipeline-needsreset': 'A pipeline cannot read its state and needs a reset',
    'pipeline-wedged': 'A pipeline is wedged on this node',
    'pipeline-blocked': 'A pipeline is blocked on something it needs',
    'loader-faulted': "A table's loader has faulted",
    'loader-waiting': "A table's loader has been waiting a while for what it reads, which is not there",
    'source-waiting': "A pipeline's input has been waiting a while for what it reads, which is not there",
    'table-faulted': 'A table has faulted',
    'no-ready-standby': 'No ready standby answers: a failure of this node stops the service',
    'every-pipeline-failed': 'Every pipeline that should run has failed',
    'not-ready': 'The node is not ready',
    'catalog-not-open': 'The catalog is not open on this node yet',
    'catalog-behind': 'This node has not applied the latest catalog version',
    'table-opening': 'A table is still being opened',
    'state-behind': "This node's copy of pipeline state is behind the log",
    'format-level': "The logs are written at a format level this node's build does not read; replace it with the build the leader runs",
    'plugin-pending': 'Plugin files are still being fetched',
    fenced: 'Another node took the writer; this process cannot lead again',
    resigned: 'This node stepped down; this process cannot lead again',
  };
  const reasonText = code => REASONS[code] || code;
  const HEALTH = { Healthy: ['good', 'check'], Degraded: ['warn', 'alert'], Unhealthy: ['crit', 'x'] };
  const healthBadge = level => {
    const [cls, ic] = HEALTH[level] || ['idle', 'circle'];
    return h('span', { class: ['badge', cls] }, icon(ic, 12), level || 'Unknown');
  };

  // ── the API ─────────────────────────────────────────────────────────────────────────

  class ApiError extends Error {
    constructor(status, body) {
      const e = (body && body.error) || {};
      super(e.message || `The server answered ${status}.`);
      this.status = status;
      this.code = e.code || 'http-' + status;
      this.details = e.details || [];
      this.leader = e.leader || null;
    }
  }

  async function api(path, { method = 'GET', body, headers = {}, raw } = {}) {
    const init = { method, headers: { Accept: 'application/json', ...headers }, cache: 'no-store' };
    if (method !== 'GET') init.headers['X-Dbsp-Admin'] = '1';
    // The name this browser's operator gave, for the audit. The node records it as the
    // caller's own word, and ignores it where the host has verified who the caller is.
    // A header carries Latin-1 only, so the name is sent percent-encoded when it needs it.
    if (store.operator) init.headers['X-Dbsp-Operator'] = /^[\x20-\x7e]*$/.test(store.operator) ? store.operator : encodeURIComponent(store.operator);
    if (raw) init.body = raw;
    else if (body !== undefined) {
      init.headers['Content-Type'] = 'application/json';
      init.body = typeof body === 'string' ? body : JSON.stringify(body);
    }
    let response;
    try {
      response = await fetch(API + path, init);
    } catch (e) {
      const err = new ApiError(0, { error: { code: 'unreachable', message: 'The node did not answer. It may be stopped or restarting.' } });
      err.cause = e;
      throw err;
    }
    let data = null;
    const text = await response.text();
    if (text) {
      try { data = JSON.parse(text); } catch { data = null; }
    }
    if (!response.ok) throw new ApiError(response.status, data);
    return data;
  }

  /** Every page of a paged list, up to a bound the UI states when it is hit. */
  async function all(path, cap = 2000) {
    const items = [];
    let skip = 0, total = 0;
    for (;;) {
      const page = await api(`${path}${path.includes('?') ? '&' : '?'}skip=${skip}&limit=200`);
      items.push(...page.items);
      total = page.total;
      if (page.next == null || items.length >= cap) break;
      skip = page.next;
    }
    return { items, total, capped: items.length < total };
  }

  // ── the store ───────────────────────────────────────────────────────────────────────

  const SLOTS = 60;
  const store = {
    info: null,
    node: null,          // the newest tick's node summary (or /node's, before the first tick)
    tick: null,
    tickAt: 0,
    feed: 'wait',        // wait | live | off
    feedSince: Date.now(),
    feedRefused: null,   // why the node refused a feed, when it said (its cap on clients)
    lastLiveAt: 0,
    series: new Map(),   // pipeline id ('' = the node) → { in, out, stepMs, ackMs, lag, ckpt }
    events: [],          // newest last, bounded
    route: { name: 'overview', args: [], query: new URLSearchParams() },
    view: null,          // { key, data, error, loading, refreshError, ui }
    theme: 'auto',
  };

  const FIELDS = ['in', 'out', 'stepMs', 'stepMaxMs', 'ackMs', 'lag', 'checkpointAgeS'];
  function pushSeries(id, rates) {
    let sr = store.series.get(id);
    if (!sr) store.series.set(id, sr = Object.fromEntries(FIELDS.map(f => [f, []])));
    for (const f of FIELDS) {
      sr[f].push(rates[f] ?? null);
      if (sr[f].length > SLOTS) sr[f].shift();
    }
  }
  function seedSeries(id, samples) {
    const have = store.series.get(id);
    if (!samples || !samples.length || (have && have.in.length >= samples.length)) return;
    const sr = Object.fromEntries(FIELDS.map(f => [f, samples.slice(-SLOTS).map(x => x.rates[f] ?? null)]));
    store.series.set(id, sr);
  }
  const seriesOf = (id, field) => (store.series.get(id) || {})[field] || [];
  const ratesOf = id => (store.tick && (id === '' ? store.tick.total : store.tick.pipelines[id])) || null;

  // ── charts ──────────────────────────────────────────────────────────────────────────

  /** A 60-slot sparkline that fills from the right, so its scale does not jump as it grows. */
  function spark(values, { cls = '', label = '', unit = '' } = {}) {
    const W = 100, H = 28, n = SLOTS;
    const pts = [];
    let max = 0, last = null;
    values.forEach((v, i) => {
      if (v == null) return;
      max = Math.max(max, v);
      last = v;
      pts.push([(n - values.length + i) / (n - 1) * W, v]);
    });
    const scale = max > 0 ? (H - 3) / max : 0;
    const xy = pts.map(([x, v]) => `${x.toFixed(1)},${(H - 1 - v * scale).toFixed(1)}`);
    const text = label ? `${label}, last ${values.length} s: now ${last == null ? 'none' : fmtNum(last) + unit}, peak ${fmtNum(max)}${unit}` : '';
    return s('svg', { class: ['spark', cls], viewBox: `0 0 ${W} ${H}`, preserveAspectRatio: 'none', role: 'img', 'aria-label': text },
      text && s('title', null, text),
      s('line', { class: 'base', x1: 0, x2: W, y1: H - 0.5, y2: H - 0.5 }),
      xy.length > 1 && s('path', { class: 'area', d: `M${pts[0][0].toFixed(1)},${H - 1} L${xy.join(' L')} L${pts[pts.length - 1][0].toFixed(1)},${H - 1} Z` }),
      xy.length > 1 && s('path', { class: 'line', d: 'M' + xy.join(' L') }));
  }

  const niceMax = v => {
    if (v <= 0) return 1;
    const p = 10 ** Math.floor(Math.log10(v));
    for (const m of [1, 2, 2.5, 5, 10]) if (v <= m * p) return m * p;
    return 10 * p;
  };

  /** Two series on one axis, with a crosshair and a tooltip. `points`: [{ t, in, out }]. */
  function lineChart(points, { width, unit = '/s', span = 's' }) {
    const W = Math.max(280, width), H = 220, L = 44, R = 12, T = 10, B = 24;
    const max = niceMax(Math.max(1, ...points.map(p => Math.max(p.in || 0, p.out || 0))));
    const n = Math.max(2, SLOTS);
    const x = i => L + (n - points.length + i) / (n - 1) * (W - L - R);
    const y = v => T + (1 - v / max) * (H - T - B);
    const path = f => {
      const seg = points.map((p, i) => (p[f] == null ? null : `${x(i).toFixed(1)},${y(p[f]).toFixed(1)}`)).filter(Boolean);
      return seg.length > 1 ? 'M' + seg.join(' L') : '';
    };
    const ticks = [0, max / 2, max];
    const move = (e, el) => {
      const svg = el.querySelector('svg'), box = svg.getBoundingClientRect();
      const data = el._props;
      if (!data.points.length) return;
      const px = (e.clientX - box.left) / box.width * data.W;
      let best = 0, d = Infinity;
      data.points.forEach((p, i) => {
        const dist = Math.abs(data.x(i) - px);
        if (dist < d) { d = dist; best = i; }
      });
      const p = data.points[best], cx = data.x(best);
      const hover = el.querySelector('.hover'), tip = el.querySelector('.tip');
      hover.replaceChildren(
        s('line', { class: 'cross', x1: cx, x2: cx, y1: T, y2: H - B }),
        p.in != null && s('circle', { class: 'pt in', cx, cy: data.y(p.in), r: 4 }),
        p.out != null && s('rect', { class: 'pt out', x: cx - 4, y: data.y(p.out) - 4, width: 8, height: 8, rx: 1 }));
      tip.replaceChildren(
        h('div', { class: 'muted' }, p.label),
        h('div', null, h('span', { class: 'swatch in' }), 'In', h('b', null, fmtNum(p.in) + unit)),
        h('div', null, h('span', { class: 'swatch out' }), 'Out', h('b', null, fmtNum(p.out) + unit)));
      tip.hidden = false;
      const left = cx / data.W * box.width;
      tip.style.left = Math.min(Math.max(0, left + 12), box.width - tip.offsetWidth - 4) + 'px';
    };
    const leave = (e, el) => {
      if (el.contains(e.relatedTarget)) return;
      el.querySelector('.hover').replaceChildren();
      el.querySelector('.tip').hidden = true;
    };
    return h('div', { class: 'chart', props: { points, x, y, W }, on: { pointermove: move, pointerout: leave } },
      s('svg', { viewBox: `0 0 ${W} ${H}`, preserveAspectRatio: 'none', role: 'img',
        'aria-label': `Records in and out per second. Now ${points.length ? fmtNum(points[points.length - 1].in) : 'no'} in, ${points.length ? fmtNum(points[points.length - 1].out) : 'no'} out.` },
        ticks.map(t => s('line', { class: 'grid-line', x1: L, x2: W - R, y1: y(t), y2: y(t) })),
        ticks.map(t => s('text', { class: 'axis-text', x: L - 6, y: y(t) + 4, 'text-anchor': 'end' }, fmtNum(t))),
        s('text', { class: 'axis-text', x: L, y: H - 6 }, `−60 ${span}`),
        s('text', { class: 'axis-text', x: (L + W - R) / 2, y: H - 6, 'text-anchor': 'middle' }, `−30 ${span}`),
        s('text', { class: 'axis-text', x: W - R, y: H - 6, 'text-anchor': 'end' }, 'now'),
        s('path', { class: 'series out', d: path('out') }),
        s('path', { class: 'series in', d: path('in') }),
        s('g', { class: 'hover', 'data-keep': true })),
      h('div', { class: 'tip', hidden: true, 'data-keep': true }));
  }

  const chartWidth = () => Math.max(280, $('main').clientWidth - 82);

  // ── small pieces ────────────────────────────────────────────────────────────────────

  const card = (title, sub, body, { flush = true, extra, cls } = {}) =>
    h('section', { class: ['card', cls] },
      h('header', null, h('h2', null, title), sub && h('span', { class: 'sub' }, sub), extra && h('span', { class: 'spacer' }), extra),
      h('div', { class: ['body', flush && 'flush'] }, body));

  const empty = (title, text) => h('div', { class: 'empty' }, h('strong', null, title), text);

  const notice = (kind, ...kids) =>
    h('div', { class: ['notice', kind], role: kind === 'error' ? 'alert' : null }, icon(kind === 'error' || kind === 'warn' ? 'alert' : 'info', 14), h('div', null, ...kids));

  function errorNotice(error, prefix) {
    return notice('error',
      h('strong', null, prefix || 'This did not load. '),
      error.message,
      error.details && error.details.length ? h('ul', null, error.details.map(d => h('li', null, d))) : null,
      error.leader && error.leader.adminUrl ? h('div', null, 'The leader is ', h('a', { href: peerHref(error.leader.adminUrl) }, error.leader.nodeId), '.') : null);
  }

  /**
   * The address of another node's admin UI, carrying what this browser chose here — the
   * theme, the operator's name — because the other node is another origin with its own
   * storage: without this the two nodes of one cluster come up looking different.
   */
  function peerHref(url) {
    try {
      const u = new URL(url, document.baseURI);
      if (store.theme && store.theme !== 'auto') u.searchParams.set('theme', store.theme);
      if (store.operator) u.searchParams.set('as', store.operator);
      return u.href;
    } catch { return url; }
  }

  /** Text that can be long — an exception message in an event — shown short until asked for. */
  function clamped(text, max = 180) {
    if (!text || text.length <= max) return text;
    const cut = text.lastIndexOf(' ', max);
    return h('details', { class: 'clamp' },
      h('summary', null, h('span', { class: 'short' }, text.slice(0, cut > max * 0.6 ? cut : max) + '… '), h('span', { class: 'toggle' })),
      h('div', { class: 'full' }, text));
  }

  const skeleton = () => h('div', { class: 'loading', 'aria-busy': 'true' },
    h('span', { class: 'sr' }, 'Loading'),
    [80, 60, 72, 40].map(w => { const d = h('div', { class: 'skeleton' }); d.style.width = w + '%'; return d; }));

  const tile = (label, value, unit, foot, sparkline) =>
    h('div', { class: 'tile' },
      h('div', { class: 'label' }, label),
      h('div', { class: 'value num' }, value, unit && h('span', { class: 'unit' }, unit)),
      h('div', { class: 'foot' }, foot || ''),
      sparkline || null);

  const kv = pairs => h('dl', { class: 'kv' }, pairs.filter(Boolean).map(([k, v]) => [h('dt', null, k), h('dd', null, v == null || v === '' ? '–' : v)]));

  const td = (label, cls, ...kids) => h('td', { class: cls, 'data-label': label }, ...kids);

  /** A data table drawn from one list of columns, so that a header and the cells under it
      cannot disagree in number or in name: every list in the UI is built here. A column is
      { label, cls, title, cell(row, i) } and optionally `cellCls` (the cells' class, when it
      is not the header's), `cellTitle(row)` (a cell's tooltip), `sr` (the header is for screen readers only) and `sort` (a key
      for `opts.sort(key, label, cls)`, which draws a sorting header). `label` is the
      header's text and every cell's `data-label`, which the phone layout shows beside the
      value. A falsy column is left out, header and cells alike. Options: `key(row, i)`
      (required), `cls` (the table's classes; `data` by default), `row(row)` (more
      attributes for a row). */
  function grid(columns, rows, opts) {
    const cols = columns.filter(Boolean);
    const { key, cls = 'data', row, sort } = opts;
    const head = c => c.sort && sort ? sort(c.sort, c.label, c.cls)
      : h('th', { class: c.cls, title: c.title || null }, c.sr ? h('span', { class: 'sr' }, c.label) : c.label);
    return h('table', { class: cls },
      h('thead', null, h('tr', null, cols.map(head))),
      h('tbody', null, rows.map((r, i) => h('tr', { key: key(r, i), ...(row ? row(r) : null) },
        cols.map(c => h('td', { class: c.cellCls !== undefined ? c.cellCls : c.cls, 'data-label': c.label, title: c.cellTitle ? c.cellTitle(r) : null }, c.cell(r, i)))))));
  }

  function toast(text, kind) {
    const el = h('div', { class: ['toast', kind] }, text);
    $('toasts').append(el);
    setTimeout(() => el.remove(), kind === 'error' ? 8000 : 4000);
  }

  const can = () => store.info && !store.info.readOnly;
  const isLeader = () => store.node && store.node.role === 'Leader';
  /** Mutating controls exist only where they can work: not read-only, and on the leader. */
  const canAct = () => can() && isLeader();

  const pageHead = ({ crumbs, title, lede, actions }) =>
    h('div', { class: 'page-head' },
      h('div', { class: 'titles' },
        crumbs && h('nav', { class: 'crumbs', 'aria-label': 'Breadcrumb' }, crumbs),
        h('h1', { tabindex: -1, id: 'page-title' }, title),
        lede && h('p', { class: 'lede' }, lede)),
      actions && h('div', { class: 'actions' }, actions));

  window.__admin = { h, s, morph, icon, api, all, store, fmtNum, fmtInt, fmtMs, fmtBytes, ago, clock, stamp, timeEl, plural, short, pretty,
    pipelineBadge, lastKnown, peerHref, clamped, tableBadge, roleBadge, healthBadge, reasonText, spark, lineChart, chartWidth, card, empty, notice, errorNotice, skeleton,
    tile, kv, td, grid, toast, can, canAct, isLeader, pageHead, seriesOf, ratesOf, seedSeries, pushSeries, ApiError, STATE_ORDER, PIPELINE_STATES, SLOTS, $ };
})();
