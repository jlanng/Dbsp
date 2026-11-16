/* The frame around the views: the event feed, the header, the navigation, the router and
   the render loop. Loaded last. */
(() => {
  'use strict';
  const A = window.__admin;
  const { h, morph, icon, api, store, $, roleBadge, healthBadge, errorNotice, skeleton, ago, clock, views } = A;

  // ── theme: follows the system until told otherwise ──────────────────────────────────

  const THEMES = ['auto', 'light', 'dark'];
  function applyTheme(theme) {
    store.theme = theme;
    if (theme === 'auto') document.documentElement.removeAttribute('data-theme');
    else document.documentElement.setAttribute('data-theme', theme);
    try { localStorage.setItem('incremental-admin-theme', theme); } catch { /* private mode */ }
    const button = $('theme');
    button.replaceChildren(icon(theme === 'auto' ? 'auto' : theme === 'light' ? 'sun' : 'moon', 16));
    const label = `Theme: ${theme === 'auto' ? 'follows the system' : theme}. Change theme`;
    button.setAttribute('aria-label', label);
    button.title = label;
  }
  // Links to the other node carry the theme, so the page is drawn again with it.
  $('theme').addEventListener('click', () => { applyTheme(THEMES[(THEMES.indexOf(store.theme) + 1) % THEMES.length]); if (store.view) render(); });
  let saved = 'auto';
  try { saved = localStorage.getItem('incremental-admin-theme') || 'auto'; } catch { /* private mode */ }
  try { store.operator = localStorage.getItem('incremental-admin-operator') || ''; } catch { store.operator = ''; }
  // Arriving from the other node's admin UI: its links carry the theme and the
  // operator's name chosen there, since each node is an origin with storage of its own.
  const carried = new URLSearchParams(location.search);
  if (carried.has('theme') || carried.has('as')) {
    if (THEMES.includes(carried.get('theme'))) saved = carried.get('theme');
    if (carried.has('as')) setOperator(carried.get('as'));
    history.replaceState(null, '', location.pathname + location.hash);
  }
  applyTheme(THEMES.includes(saved) ? saved : 'auto');

  // ── who the audit will say did it ───────────────────────────────────────────────────

  function setOperator(name) {
    store.operator = (name || '').trim().slice(0, 64);
    try {
      if (store.operator) localStorage.setItem('incremental-admin-operator', store.operator);
      else localStorage.removeItem('incremental-admin-operator');
    } catch { /* private mode */ }
  }

  async function refreshWho() {
    try { store.who = await api('whoami'); } catch { store.who = null; }
    drawWho();
  }

  function drawWho() {
    const button = $('who'), who = store.who;
    if (!who) { button.hidden = true; return; }
    const named = who.caller !== 'local';
    const editable = !who.verified && !!who.operatorHeader;
    button.hidden = !named && !editable;
    button.disabled = !editable;
    button.replaceChildren(icon('user', 14), h('span', { class: 'who-name' }, named ? who.caller.replace(' (self-declared)', '') : 'Unnamed'));
    const label = who.verified ? `Signed in as ${who.caller}: the audit records changes under this name.`
      : named ? `Changes are recorded as “${who.caller}”. Change the name`
      : 'Changes made here are recorded as “local”: nobody is signed in. Give a name for the audit';
    button.setAttribute('aria-label', label);
    button.title = label;
  }

  $('who').addEventListener('click', () => {
    const who = store.who;
    if (!who || who.verified) return;
    const dlg = A.modal('Your name, for the audit');
    let text = store.operator || '';
    const save = async () => { setOperator(text); dlg.close(); await refreshWho(); if (store.view) render(); };
    const draw = () => dlg.set([
      h('p', null, 'This admin surface has no sign-in, so the audit cannot know who made a change: it records ',
        h('code', null, 'local'), '. A name given here is sent with every change from this browser and recorded as ',
        h('code', null, `${text.trim() || 'name'} (self-declared)`), '.'),
      h('p', { class: 'muted' }, 'It is your word, not a check: anyone who can reach this page can give any name. Where the host signs operators in, the audit uses that identity and this is ignored.'),
      h('div', { class: 'field' },
        h('label', { for: 'operator-name' }, 'Name'),
        h('input', { class: 'input', id: 'operator-name', autocomplete: 'name', spellcheck: 'false', maxlength: 64, value: text,
          on: { input: e => { text = e.target.value; draw(); }, keydown: e => { if (e.key === 'Enter') { e.preventDefault(); save(); } } } })),
    ], [
      h('button', { class: 'btn', type: 'button', on: { click: () => dlg.close() } }, 'Cancel'),
      store.operator && h('button', { class: 'btn', type: 'button', on: { click: () => { text = ''; save(); } } }, 'Forget the name'),
      h('button', { class: 'btn primary', type: 'button', on: { click: save } }, 'Use this name'),
    ]);
    draw();
    document.getElementById('operator-name').focus();
  });

  // ── navigation ──────────────────────────────────────────────────────────────────────

  const NAV = [
    ['overview', 'Overview', '#/'],
    ['pipelines', 'Pipelines', '#/pipelines'],
    ['tables', 'Tables', '#/tables'],
    ['plugins', 'Plugins', '#/plugins'],
    ['dead', 'Dead letters', '#/dead-letters'],
    ['cluster', 'Cluster', '#/cluster'],
    ['audit', 'Audit', '#/audit'],
  ];
  const SECTION = { overview: 'overview', pipelines: 'pipelines', pipeline: 'pipelines', tables: 'tables', table: 'tables', plugins: 'plugins', dead: 'dead', cluster: 'cluster', audit: 'audit' };

  function drawNav() {
    const node = store.node;
    const by = (node && node.pipelinesByState) || {};
    // A node that no longer leads has nothing in trouble: its states are history.
    const trouble = A.lastKnown() ? 0 : (by.Failed || 0) + (by.Wedged || 0) + (by.NeedsReset || 0) + (by.Blocked || 0) + (by.Backoff || 0);
    // The total in the plain colour; how many are in trouble beside it, in the alert
    // colour, as its own number: a total painted red reads as that many in trouble.
    const counts = {
      pipelines: node ? [node.pipelines, trouble] : null,
      tables: node ? [node.tables, 0] : null,
    };
    const current = SECTION[store.route.name];
    morph($('nav'), h('nav', { class: 'nav', id: 'nav', 'aria-label': 'Sections' },
      NAV.map(([key, label, href]) => h('a', { href, 'aria-current': current === key ? 'page' : null },
        icon(key, 16), h('span', null, label),
        counts[key] && h('span', { class: 'counts' },
          counts[key][1] > 0 && h('span', { class: 'count alert', title: `${counts[key][1]} of ${counts[key][0]} need attention` },
            counts[key][1], h('span', { class: 'sr' }, ' need attention, of ')),
          h('span', { class: 'count' }, counts[key][0]))))));
  }

  // ── header: who this node is, and whether what is shown is live ─────────────────────

  function drawHeader() {
    const node = store.node;
    morph($('node-chip'), h('div', { class: 'node-chip', id: 'node-chip', 'aria-live': 'polite' },
      node ? [
        roleBadge(node.role),
        h('span', { class: 'node-id', title: `node ${node.nodeId}, term ${node.term}` }, node.nodeId, h('span', { class: 'muted' }, ` · term ${node.term}`)),
        node.health && node.health.level !== 'Healthy' && healthBadge(node.health.level),
      ] : h('span', { class: 'muted' }, 'Connecting…')));

    const feed = store.feed;
    const text = feed === 'live' ? 'Live' : feed === 'wait' ? 'Connecting…' : 'Disconnected';
    morph($('feed'), h('div', { class: ['feed', feed], id: 'feed', role: 'status', 'aria-live': 'polite', title: 'The live event feed from this node' },
      h('span', { class: 'dot' }), h('span', { class: 'feed-text' }, text)));

    const banner = $('banner');
    if (feed === 'off') {
      banner.hidden = false;
      const refused = store.feedRefused;
      morph(banner, h('div', { class: 'banner', id: 'banner', role: 'alert' },
        icon('alert', 18),
        h('div', null,
          refused ? 'No live feed: this node already serves as many as it allows. What this page shows is stale. '
            : 'Live feed disconnected. What this page shows is stale. ',
          h('span', { class: 'banner-detail' },
            refused ? `${refused} Close another tab of this admin UI, or reload this page for a fresh reading. `
              : store.lastLiveAt ? `Last update at ${clock(store.lastLiveAt)} (${ago(store.lastLiveAt)}). ` : 'Nothing has been received yet. ',
            refused ? 'Trying again every few seconds…' : 'Trying to reconnect…'))));
    } else {
      banner.hidden = true;
    }
    $('main').classList.toggle('stale', feed === 'off');
    document.body.classList.toggle('stale', feed === 'off');

    const strip = $('role-strip');
    const leader = store.leader;
    let content = null, cls = '';
    if (node && node.role === 'Standby') {
      cls = 'standby';
      content = [
        icon('follow', 15), h('strong', null, 'This node is a standby.'),
        ` It follows the leader${node.leader ? ' ' + node.leader : ''} and runs no pipelines; changes are made on the leader.`,
        leader && leader.adminUrl && leader.nodeId !== node.nodeId && h('a', { class: 'btn small', href: A.peerHref(leader.adminUrl) }, icon('link', 12), `Open ${leader.nodeId}`),
      ];
    } else if (node && (node.role === 'Fenced' || node.role === 'Resigned')) {
      cls = node.role.toLowerCase();
      content = [
        icon('lock', 15), h('strong', null, `This node is ${node.role.toLowerCase()}.`),
        node.role === 'Fenced'
          ? ' Another node took over; this process runs nothing and cannot lead again. Pipeline states shown here are the last it saw. Restart it to rejoin as a standby.'
          : ' It stepped down; this process runs nothing and cannot lead again. Pipeline states shown here are the last it saw. Restart it to rejoin as a standby.',
        leader && leader.adminUrl && leader.nodeId !== node.nodeId && h('a', { class: 'btn small', href: A.peerHref(leader.adminUrl) }, icon('link', 12), `Open ${leader.nodeId}`),
      ];
    } else if (store.info && store.info.readOnly) {
      cls = 'readonly';
      content = [icon('lock', 15), h('strong', null, 'Read-only.'), ' This admin surface shows everything and changes nothing.'];
    }
    strip.hidden = !content;
    if (content) morph(strip, h('div', { class: ['role-strip', cls], id: 'role-strip' }, content));
  }

  // ── router ──────────────────────────────────────────────────────────────────────────

  function parseHash() {
    const raw = (location.hash || '#/').slice(1);
    const [path, query = ''] = raw.split('?');
    const parts = path.split('/').filter(Boolean).map(decodeURIComponent);
    const q = new URLSearchParams(query);
    const [a, b, c] = parts;
    if (!a) return { name: 'overview', args: [], query: q };
    if (a === 'pipelines') return b ? { name: 'pipeline', args: [b, c || 'overview'], query: q } : { name: 'pipelines', args: [], query: q };
    if (a === 'tables') return b ? { name: 'table', args: [b], query: q } : { name: 'tables', args: [], query: q };
    if (a === 'plugins') return { name: 'plugins', args: [], query: q };
    if (a === 'dead-letters') return { name: 'dead', args: b ? [b] : [], query: q };
    if (a === 'cluster') return { name: 'cluster', args: [], query: q };
    if (a === 'audit') return { name: 'audit', args: [], query: q };
    return { name: 'missing', args: [raw], query: q };
  }

  let loadSeq = 0;
  let timer = 0;

  function context() {
    const view = store.view;
    return {
      args: store.route.args, query: store.route.query, ui: view.ui, data: view.data,
      render, reload: () => load(false),
      /** Changes the query part of the address without loading again: filters are deep links. */
      setQuery(params) {
        const usp = new URLSearchParams();
        for (const [k, v] of Object.entries(params)) if (v) usp.set(k, v);
        const base = (location.hash || '#/').split('?')[0];
        const text = usp.toString();
        history.replaceState(null, '', base + (text ? '?' + text : ''));
        store.route.query = usp;
        render();
      },
    };
  }

  async function load(first) {
    const view = store.view, def = views[store.route.name];
    if (!def || !def.load) return;
    const seq = ++loadSeq;
    clearTimeout(timer);
    try {
      const data = await def.load(context(), view.data);
      if (seq !== loadSeq) return;
      view.data = data;
      view.error = null;
      view.refreshError = null;
      view.loadedAt = Date.now();
    } catch (e) {
      if (seq !== loadSeq) return;
      if (view.data == null) view.error = e;
      else view.refreshError = e;
    }
    view.loading = false;
    render();
    if (first) {
      const title = document.getElementById('page-title');
      if (title && document.activeElement === document.body) title.focus({ preventScroll: true });
    }
    schedule();
  }

  function schedule() {
    clearTimeout(timer);
    const def = views[store.route.name];
    const every = (def && def.every) || 3000;
    timer = setTimeout(() => {
      if (document.hidden) schedule();
      else load(false);
    }, store.feed === 'off' ? Math.max(every, 5000) : every);
  }

  function navigate() {
    store.route = parseHash();
    const key = store.route.name + '/' + store.route.args[0];
    const same = store.view && store.view.key === key;
    // A tab of the same page keeps its data; a different page starts clean.
    store.view = same ? store.view : { key, data: null, error: null, loading: true, refreshError: null, ui: {} };
    const def = views[store.route.name];
    document.title = `${def && def.title ? def.title(store.route.args) : 'Not found'} · Incremental admin`;
    // A dialog belongs to the page it was opened on. Left open across Back or Forward it
    // would act on that page's pipeline or table over another's.
    if (!same) document.querySelectorAll('dialog.modal[open]').forEach(d => d.close());
    drawNav();
    render();
    if (!same) window.scrollTo(0, 0);
    load(!same);
  }

  function render() {
    const main = $('main');
    const def = views[store.route.name];
    const view = store.view;
    let body;
    if (!def) {
      body = h('div', null, A.pageHead({ title: 'Nothing here' }),
        A.empty('No such page', h('span', null, 'The address ', h('code', null, location.hash), ' is not a page of this admin UI. ', h('a', { href: '#/' }, 'Go to the overview'), '.')));
    } else if (view.error) {
      body = h('div', null, A.pageHead({ title: def.title(store.route.args) }),
        errorNotice(view.error),
        h('p', { class: 'lede' }, h('button', { class: 'btn', type: 'button', on: { click: () => { view.error = null; view.loading = true; render(); load(true); } } }, 'Try again')));
    } else if (view.data == null) {
      body = h('div', null, A.pageHead({ title: def.title(store.route.args) }), h('div', { class: 'card' }, skeleton()));
    } else {
      try {
        body = def.render(context());
      } catch (e) {
        console.error(e);
        body = h('div', null, A.pageHead({ title: def.title(store.route.args) }),
          errorNotice({ message: `This view failed to draw: ${e.message}` }, 'A bug in the admin UI. '));
      }
      if (view.refreshError && store.feed !== 'off')
        body.prepend(A.notice('warn', 'The last refresh failed, so this is the previous answer: ', view.refreshError.message));
    }
    const wrapped = h('div', { class: 'view live-data', 'data-key': store.view ? store.view.key : 'none' }, body);
    if (main.firstElementChild) morph(main.firstElementChild, wrapped);
    else main.append(wrapped);
  }
  A.render = render;

  // ── the feed ────────────────────────────────────────────────────────────────────────

  let source = null;
  let reloadSoon = 0;

  function setFeed(state) {
    if (store.feed === state) return;
    store.feed = state;
    store.feedSince = Date.now();
    drawHeader();
    if (store.view) render();
  }

  function onTick(tick) {
    store.tick = tick;
    store.tickAt = Date.now();
    store.lastLiveAt = Date.now();
    store.node = tick.node;
    if (tick.total) A.pushSeries('', tick.total);
    for (const [id, rates] of Object.entries(tick.pipelines || {})) A.pushSeries(id, rates);
    for (const id of store.series.keys()) if (id !== '' && !(id in (tick.pipelines || {}))) store.series.delete(id);
    setFeed('live');
    drawHeader();
    drawNav();
    if (store.view && store.view.data != null && !document.hidden) render();
  }

  function onEvent(e) {
    store.lastLiveAt = Date.now();
    store.events.push(e);
    if (store.events.length > 200) store.events.splice(0, store.events.length - 200);
    const def = views[store.route.name];
    if (def && (!def.kinds || def.kinds.includes(e.kind))) {
      clearTimeout(reloadSoon);
      reloadSoon = setTimeout(() => load(false), 250);
    }
    if (e.kind === 'node') refreshNode();
  }

  function connect() {
    if (source) source.close();
    source = new EventSource(API_EVENTS);
    source.addEventListener('hello', () => { store.lastLiveAt = Date.now(); setFeed('live'); refreshNode(); });
    source.addEventListener('metrics', e => onTick(JSON.parse(e.data)));
    source.addEventListener('lagged', () => load(false));
    for (const kind of ['node', 'pipeline', 'table', 'catalog', 'deadletter', 'plugin'])
      source.addEventListener(kind, e => onEvent(JSON.parse(e.data)));
    source.addEventListener('hello', () => { store.feedRefused = null; });
    // The browser reconnects by itself; until it has, what is on the page is stale.
    source.onerror = () => { setFeed('off'); if (source.readyState === EventSource.CLOSED) whyClosed(); };
  }
  const API_EVENTS = new URL('api/events', document.baseURI).href;

  // An EventSource that was refused says nothing of why: no status, no body. So when it
  // closes for good the same address is asked once with fetch, which does say. A node
  // that is serving its limit of feeds answers 503 too-many-clients; that is shown, and
  // the next attempt waits a few seconds instead of one.
  let asking = false, retryAt = 0;
  async function whyClosed() {
    if (asking) return;
    asking = true;
    const abort = new AbortController();
    try {
      const response = await fetch(API_EVENTS, { headers: { Accept: 'text/event-stream' }, signal: abort.signal });
      if (response.ok) { store.feedRefused = null; retryAt = 0; }
      else {
        let body = null;
        try { body = await response.json(); } catch { /* not ours */ }
        const error = body && body.error;
        store.feedRefused = error && error.code === 'too-many-clients' ? error.message : null;
        retryAt = store.feedRefused ? Date.now() + 5000 : 0;
      }
    } catch { store.feedRefused = null; retryAt = 0; }
    abort.abort(); // a stream that did open is not kept: the EventSource opens its own
    asking = false;
    drawHeader();
  }

  // A feed that is open but silent is as stale as one that is closed.
  let seconds = 0;
  setInterval(() => {
    // A node that does not lead learns who does from its probe of the other node, which
    // can answer a moment after the role changed: ask again until it is known.
    if (++seconds % 3 === 0 && store.node && store.node.role !== 'Leader' && (!store.leader || store.leader.nodeId === store.node.nodeId)) refreshNode();
    if (store.feed === 'live' && store.tickAt && Date.now() - store.tickAt > 5000) setFeed('off');
    if (store.feed === 'off') drawHeader();
    if (store.feed === 'off' && source && source.readyState === EventSource.CLOSED && !asking && Date.now() >= retryAt) connect();
  }, 1000);

  async function refreshNode() {
    try {
      const node = await api('node');
      store.leader = node.leader;
      if (!store.tick) {
        const st = node.status;
        store.node = {
          nodeId: st.nodeId, role: st.leadership.role, term: st.leadership.role === 'Standby' ? st.leadership.leaderTerm : st.leadership.term, leader: st.leadership.leaderNodeId, health: st.health,
          catalogVersion: st.catalogVersion, tables: st.tables, pipelines: st.pipelines, pipelinesByState: st.pipelinesByState, peer: st.peer,
        };
      }
      drawHeader();
      drawNav();
    } catch { /* the banner says so */ }
  }

  async function start() {
    drawHeader();
    drawNav();
    try {
      store.info = await api('info');
    } catch (e) {
      store.info = { readOnly: true, exposeRowData: false, maxRows: 200 };
      store.infoError = e;
    }
    await refreshNode();
    await refreshWho();
    window.addEventListener('hashchange', navigate);
    window.addEventListener('resize', () => store.view && render());
    document.addEventListener('visibilitychange', () => { if (!document.hidden) load(false); });
    navigate();
    connect();
  }
  start();
})();
