/* The views. Each is { title, load, render }: load fetches what the page shows, render
   builds it from that and from the live store (the feed's ticks). */
(() => {
  'use strict';
  const A = window.__admin;
  const { h, s, icon, api, all, store, fmtNum, fmtInt, fmtMs, fmtBytes, ago, clock, stamp, timeEl, plural, short, pretty,
    pipelineBadge, tableBadge, roleBadge, healthBadge, reasonText, spark, lineChart, chartWidth, card, empty, notice, errorNotice,
    tile, kv, td, toast, canAct, pageHead, seriesOf, ratesOf, seedSeries, STATE_ORDER, PIPELINE_STATES } = A;

  const views = A.views = {};
  const enc = encodeURIComponent;
  const pipelineLink = id => h('a', { class: 'id', href: '#/pipelines/' + enc(id) }, id);
  const tableLink = name => h('a', { class: 'id', href: '#/tables/' + enc(name) }, name);
  const STATE_FILL = { good: 'fill-good', warn: 'fill-warn', serious: 'fill-serious', crit: 'fill-crit', idle: 'fill-idle', info: 'fill-info' };
  const last = arr => (arr.length ? arr[arr.length - 1] : null);
  const rate = v => (v == null ? '–' : fmtNum(v));

  const eventList = events => h('ul', { class: 'events', role: 'log', 'aria-label': 'Recent events' },
    events.slice().reverse().map(e => h('li', { key: 'e' + e.sequence },
      h('span', { class: 'when' }, timeEl(e.time, clock(e.time))),
      h('span', { class: 'kind' }, e.kind),
      h('span', { class: 'msg' },
        e.kind === 'pipeline' || e.kind === 'deadletter' ? h('a', { class: 'subject', href: '#/pipelines/' + enc(e.subject) }, e.subject)
          : e.kind === 'table' ? h('a', { class: 'subject', href: '#/tables/' + enc(e.subject) }, e.subject)
          : h('span', { class: 'subject' }, e.subject),
        e.message))));

  // ── overview ────────────────────────────────────────────────────────────────────────

  function nodeFacts(node) {
    const st = node.status, lead = st.leadership, kafka = lead.kafka;
    const fact = (label, value, mono) => h('div', { class: 'fact' }, h('div', { class: 'label' }, label), h('div', { class: ['v', mono && 'mono'] }, value));
    return [
      fact('Term', fmtInt(lead.role === 'Standby' ? lead.leaderTerm : lead.term)),
      fact('Leader', lead.leaderNodeId ? (lead.leaderNodeId === st.nodeId ? 'this node' : lead.leaderNodeId) : 'none known', true),
      fact('Heartbeat', kafka ? (lead.role === 'Leader' ? 'sending' : fmtMs(kafka.heartbeatAge) + ' ago') : h('span', { class: 'muted', title: 'The in-memory backend has no heartbeat: nodes share a process.' }, 'not applicable')),
      fact('Health', h('span', null, healthBadge(st.health.level))),
      fact('Catalog', 'version ' + fmtInt(st.catalogVersion)),
      fact('Backend', st.backend.kind, true),
    ];
  }

  views.overview = {
    title: () => 'Overview',
    every: 3000,
    async load() {
      const [node, pipelines, tables, events, metrics] = await Promise.all([
        api('node'), all('pipelines'), all('tables'), api('events/recent?limit=40'), api('node/metrics'),
      ]);
      seedSeries('', metrics.seconds);
      store.leader = node.leader;
      return { node, pipelines, tables, events };
    },
    render({ data }) {
      const { node, pipelines, tables, events } = data;
      const st = node.status, role = st.leadership.role;
      const live = store.node || {};
      const total = ratesOf('') || node.rates || {};
      const byState = live.pipelinesByState || st.pipelinesByState || {};
      const count = Object.values(byState).reduce((a, b) => a + b, 0);
      const states = STATE_ORDER.filter(x => byState[x]);
      const standby = role === 'Standby';

      const attention = [];
      for (const p of pipelines.items) {
        if (['Failed', 'Wedged', 'NeedsReset', 'Blocked', 'Backoff'].includes(p.state))
          attention.push(h('li', { key: 'p' + p.id }, pipelineBadge(p.state),
            h('div', { class: 'text' }, pipelineLink(p.id),
              h('span', { class: 'sub' }, p.blockedOn ? blockedText(p.blockedOn) : p.lastFault ? `${p.lastFault.exceptionType}${p.lastFault.message ? ': ' + p.lastFault.message : ''}` : PIPELINE_STATES[p.state][2]))));
      }
      for (const t of tables.items) {
        if (t.state !== 'Open' && t.state !== 'Opening')
          attention.push(h('li', { key: 't' + t.name }, tableBadge(t.state), h('div', { class: 'text' }, tableLink(t.name), h('span', { class: 'sub' }, t.fault || (t.loader && t.loader.fault) || 'See the table.'))));
      }
      for (const reason of st.health.reasons.filter(r => !r.startsWith('pipeline-') && r !== 'loader-faulted' && r !== 'table-faulted' && r !== 'every-pipeline-failed'))
        attention.push(h('li', { key: 'r' + reason }, healthBadge(st.health.level), h('div', { class: 'text' }, reasonText(reason), h('span', { class: 'sub' }, h('a', { href: '#/cluster' }, 'See the cluster page')))));

      return h('div', { class: 'stack' },
        pageHead({ title: 'Overview', lede: 'This node, what it runs, and what has happened lately.' }),

        h('section', { class: ['card', 'hero', role.toLowerCase()], 'aria-label': 'This node' },
          h('div', { class: 'who' }, roleBadge(role, true), h('div', { class: 'node-name' }, st.nodeId)),
          h('div', { class: 'facts' }, nodeFacts(node))),

        h('div', { class: 'tiles' },
          tile('Records in', rate(total.in), '/s', `${fmtInt(total.steps || 0)} steps in the last second`, spark(seriesOf('', 'in'), { label: 'Records in per second', unit: '/s' })),
          tile('Records out', rate(total.out), '/s', 'delivered to sinks', spark(seriesOf('', 'out'), { cls: 'out', label: 'Records out per second', unit: '/s' })),
          standby
            ? tile('State lag', fmtNum(live.stateLag ?? (st.standby && st.standby.stateLagRecords)), 'records', 'behind the state log', null)
            : tile('Input lag', total.lag == null ? '–' : fmtNum(total.lag), total.lag == null ? '' : 'records', total.lag == null ? 'no source reports lag' : 'across every source', spark(seriesOf('', 'lag'), { cls: 'neutral', label: 'Input lag in records' })),
          tile('Step latency', total.stepMs == null ? '–' : fmtMs(total.stepMs), '', total.stepMaxMs == null ? 'no steps in the last second' : `slowest ${fmtMs(total.stepMaxMs)}`, spark(seriesOf('', 'stepMs'), { cls: 'neutral', label: 'Mean step latency', unit: ' ms' }))),

        h('div', { class: 'split' },
          h('div', { class: 'stack' },
            card('Pipelines', plural(count, 'pipeline'),
              count === 0 ? empty('No pipelines are deployed', canAct() ? h('a', { href: '#/pipelines' }, 'Deploy one from the pipelines page.') : 'Nothing has been deployed to this service yet.')
                : h('div', { class: 'stack' },
                  h('div', { class: 'statebar', role: 'img', 'aria-label': states.map(x => `${byState[x]} ${x}`).join(', ') },
                    states.map(x => { const el = h('span', { class: STATE_FILL[PIPELINE_STATES[x][0]], title: `${byState[x]} ${x}` }); el.style.flex = byState[x]; return el; })),
                  h('ul', { class: 'legend' }, states.map(x => h('li', null,
                    h('a', { href: '#/pipelines?state=' + x }, pipelineBadge(x)), h('strong', { class: 'num' }, byState[x]))))),
              { flush: false, extra: h('a', { href: '#/pipelines' }, 'All pipelines') }),

            card('Tables', plural(tables.total, 'table'),
              tables.items.length === 0 ? empty('No tables are declared', 'Shared tables appear here once the host or the API declares them.')
                : h('div', { class: 'table-wrap' }, h('table', { class: 'data cards' },
                  h('thead', null, h('tr', null, h('th', null, 'Table'), h('th', null, 'State'), h('th', { class: 'r' }, 'Rows'), h('th', { class: 'r' }, 'Version'), h('th', { class: 'r' }, standby ? 'Behind' : 'Readers'))),
                  h('tbody', null, tables.items.map(t => h('tr', { key: t.name },
                    td('Table', 'id', tableLink(t.name)),
                    td('State', null, tableBadge(t.state)),
                    td('Rows', 'r', fmtNum(t.rows)),
                    td('Version', 'r', fmtInt(t.version)),
                    td(standby ? 'Behind' : 'Readers', 'r', standby ? (t.lagVersions == null ? '–' : plural(t.lagVersions, 'version')) : fmtInt(t.readers))))))),
              { extra: h('a', { href: '#/tables' }, 'All tables') })),

          h('div', { class: 'stack' },
            card('Needs attention', attention.length ? String(attention.length) : null,
              attention.length ? h('ul', { class: 'attn' }, attention) : empty('Nothing needs attention', 'Every pipeline and table is where it was asked to be.')),
            card('Recent events', 'this node, newest first',
              events.items.length ? h('div', { class: 'scroll' }, eventList(events.items)) : empty('No events yet', 'Role changes, pipeline and table transitions appear here as they happen.')))));
    },
  };

  function blockedText(blockedOn) {
    if (!blockedOn) return '';
    if (blockedOn.startsWith('backoff until ')) {
      const until = new Date(blockedOn.slice('backoff until '.length));
      const left = Math.max(0, until - Date.now());
      return `Restarting ${left < 1000 ? 'now' : 'in ' + fmtMs(left)}`;
    }
    const [kind, what] = blockedOn.split(':');
    return ({ table: `Waiting for table ${what}`, 'sink-ack': `Sink ${what} has not acknowledged a write`, 'source-paused': `Source ${what} is paused`, lease: 'Waiting for the lease', 'no-progress': 'Making no progress' })[kind] || blockedOn;
  }

  // ── pipelines ───────────────────────────────────────────────────────────────────────

  const SORTS = {
    id: p => p.id, state: p => STATE_ORDER.indexOf(p.state), definition: p => p.definition,
    in: p => (ratesOf(p.id) || p.rates || {}).in ?? -1, out: p => (ratesOf(p.id) || p.rates || {}).out ?? -1,
    step: p => (ratesOf(p.id) || p.rates || {}).stepMs ?? -1, lag: p => (ratesOf(p.id) || p.rates || {}).lag ?? -1,
    dead: p => p.deadLetters, since: p => -new Date(p.since || 0).getTime(),
  };

  function sortHeader(ctx, key, label, cls) {
    const current = ctx.query.get('sort') || 'state', dir = ctx.query.get('dir') || 'asc';
    const active = current === key;
    return h('th', { class: cls, 'aria-sort': active ? (dir === 'asc' ? 'ascending' : 'descending') : null },
      h('button', { class: 'sort', type: 'button', on: { click: () => ctx.setQuery({ ...Object.fromEntries(ctx.query), sort: key, dir: active && dir === 'asc' ? 'desc' : 'asc' }) } },
        label, h('span', { class: 'arrow', 'aria-hidden': 'true' }, active ? (dir === 'asc' ? '↑' : '↓') : '')));
  }

  views.pipelines = {
    title: () => 'Pipelines',
    kinds: ['pipeline', 'deadletter', 'node', 'catalog'],
    async load() {
      const [pipelines, plugins] = await Promise.all([all('pipelines'), all('plugins')]);
      return { pipelines, plugins };
    },
    render(ctx) {
      const { pipelines, plugins } = ctx.data;
      const q = ctx.query.get('q') || '';
      const chosen = new Set((ctx.query.get('state') || '').split(',').filter(Boolean));
      const sort = ctx.query.get('sort') || 'state', dir = ctx.query.get('dir') === 'desc' ? -1 : 1;
      const counts = {};
      for (const p of pipelines.items) counts[p.state] = (counts[p.state] || 0) + 1;
      const needle = q.toLowerCase();
      const shown = pipelines.items
        .filter(p => !needle || p.id.toLowerCase().includes(needle) || p.definition.toLowerCase().includes(needle) || p.plugin.toLowerCase().includes(needle))
        .filter(p => chosen.size === 0 || chosen.has(p.state))
        .sort((a, b) => {
          const x = SORTS[sort](a), y = SORTS[sort](b);
          return (x < y ? -1 : x > y ? 1 : a.id.localeCompare(b.id)) * dir;
        });
      const set = patch => ctx.setQuery({ ...Object.fromEntries(ctx.query), ...patch });
      const toggle = state => {
        const next = new Set(chosen);
        next.has(state) ? next.delete(state) : next.add(state);
        set({ state: [...next].join(',') });
      };

      return h('div', { class: 'stack' },
        pageHead({
          title: 'Pipelines', lede: 'One circuit each. Deployed, replaced and restarted one at a time while the others run.',
          actions: canAct() && h('button', { class: 'btn primary', type: 'button', on: { click: () => A.specDialog({ title: 'Deploy a pipeline', spec: { id: '', plugin: '', definition: '' }, plugins: plugins.items, after: ctx.reload }) } }, icon('plus', 14), 'Deploy'),
        }),
        h('section', { class: 'card' },
          h('div', { class: 'toolbar', role: 'search' },
            h('input', { class: 'input grow', type: 'search', placeholder: 'Filter by id, definition or plugin', 'aria-label': 'Filter pipelines', value: q, on: { input: e => set({ q: e.target.value }) } }),
            h('div', { class: 'chips', role: 'group', 'aria-label': 'Filter by state' },
              STATE_ORDER.filter(x => counts[x]).map(x => h('button', { class: 'chip', type: 'button', 'aria-pressed': String(chosen.has(x)), on: { click: () => toggle(x) } }, x, h('span', { class: 'n' }, counts[x])))),
            (q || chosen.size > 0) && h('button', { class: 'btn ghost small', type: 'button', on: { click: () => set({ q: '', state: '' }) } }, 'Clear')),
          pipelines.items.length === 0
            ? empty('No pipelines are deployed', canAct() ? 'Use Deploy to add the first one.' : 'Nothing has been deployed to this service yet.')
            : shown.length === 0 ? empty('No pipeline matches', 'Clear the filter to see all ' + pipelines.items.length + '.')
            : h('div', { class: 'table-wrap' }, h('table', { class: 'data cards' },
              h('thead', null, h('tr', null,
                sortHeader(ctx, 'id', 'Pipeline'), sortHeader(ctx, 'state', 'State'), sortHeader(ctx, 'definition', 'Definition'),
                sortHeader(ctx, 'in', 'In /s', 'r'), h('th', null, h('span', { class: 'sr' }, 'Records in, last minute')), sortHeader(ctx, 'out', 'Out /s', 'r'),
                sortHeader(ctx, 'step', 'Step', 'r'), sortHeader(ctx, 'lag', 'Lag', 'r'), sortHeader(ctx, 'dead', 'Dead letters', 'r'), sortHeader(ctx, 'since', 'In state', 'r'))),
              h('tbody', null, shown.map(p => {
                const r = ratesOf(p.id) || p.rates || {};
                return h('tr', { key: p.id },
                  td('Pipeline', 'id', pipelineLink(p.id)),
                  td('State', null, pipelineBadge(p.state), p.blockedOn && h('span', { class: 'sub' }, blockedText(p.blockedOn)), p.pending && h('span', { class: 'sub' }, `unfinished ${p.pending}`)),
                  td('Definition', null, p.definition, h('span', { class: 'sub mono' }, `${p.plugin}@${short(p.version)}`)),
                  td('In /s', 'r', rate(r.in)),
                  td('Last minute', 'spark', spark(seriesOf(p.id, 'in'), { label: `${p.id}: records in per second`, unit: '/s' })),
                  td('Out /s', 'r', rate(r.out)),
                  td('Step', 'r', r.stepMs == null ? '–' : fmtMs(r.stepMs)),
                  td('Lag', 'r', r.lag == null ? '–' : fmtNum(r.lag)),
                  td('Dead letters', 'r', p.deadLetters ? h('a', { href: '#/pipelines/' + enc(p.id) + '/dead-letters' }, fmtNum(p.deadLetters)) : '0'),
                  td('In state', 'r', timeEl(p.since)));
              })))),
          pipelines.capped && h('div', { class: 'pager' }, h('span', { class: 'info' }, `Showing the first ${fmtInt(pipelines.items.length)} of ${fmtInt(pipelines.total)}. Filter on the server with the API's q and state parameters.`))));
    },
  };

  // ── one pipeline ────────────────────────────────────────────────────────────────────

  const TABS = [['overview', 'Overview'], ['topology', 'Topology'], ['config', 'Config'], ['faults', 'Faults'], ['dead-letters', 'Dead letters']];

  views.pipeline = {
    title: args => args[0],
    kinds: ['pipeline', 'deadletter', 'table'],
    every: 2500,
    async load(ctx, previous) {
      const [id, tab] = ctx.args;
      const detail = await api('pipelines/' + enc(id));
      const data = { detail, graph: previous && previous.graph, graphOf: previous && previous.graphOf, metrics: previous && previous.metrics, letters: previous && previous.letters };
      if (tab === 'overview' || !data.metrics) {
        data.metrics = await api(`pipelines/${enc(id)}/metrics`);
        seedSeries(id, data.metrics.seconds);
      }
      if (tab === 'topology') {
        const stampOf = detail.pipeline.generation + '/' + detail.pipeline.state + '/' + detail.pipeline.deadLetters;
        if (!data.graph || data.graphOf !== stampOf) {
          data.graph = await api(`pipelines/${enc(id)}/graph`);
          data.graphOf = stampOf;
        }
      }
      if (tab === 'dead-letters') {
        const before = ctx.ui.before;
        data.letters = await api(`pipelines/${enc(id)}/dead-letters?limit=25${before ? '&before=' + before : ''}`);
      }
      return data;
    },
    render(ctx) {
      const [id, tab] = ctx.args;
      const { detail } = ctx.data;
      const p = detail.pipeline;
      const acting = canAct();
      const act = name => () => A.pipelineAction(name, p, name === 'remove' ? () => { location.hash = '#/pipelines'; } : ctx.reload);
      const button = (name, label, ic, cls) => h('button', { class: ['btn', cls], type: 'button', on: { click: act(name) } }, icon(ic, 13), label);
      const stopped = p.desired === 'Stopped';

      const body = tab === 'topology' ? topologyTab(ctx)
        : tab === 'config' ? configTab(ctx)
        : tab === 'faults' ? faultsTab(ctx)
        : tab === 'dead-letters' ? lettersTab(ctx, id)
        : overviewTab(ctx);

      return h('div', null,
        pageHead({
          crumbs: h('a', { href: '#/pipelines' }, 'Pipelines'),
          title: [h('span', { class: 'mono' }, id), pipelineBadge(p.state)],
          lede: [
            `${p.definition} from ${p.plugin}@${short(p.version)} · generation ${p.generation}`,
            p.operation && ` · ${p.operation} in progress`,
            p.blockedOn && ` · ${blockedText(p.blockedOn)}`,
            p.pending && ` · unfinished ${p.pending}`,
          ],
          actions: acting && [
            stopped ? button('start', 'Start', 'play') : button('stop', 'Stop', 'stop'),
            button('restart', 'Restart', 'restart'),
            h('button', { class: 'btn', type: 'button', on: { click: () => A.specDialog({ title: `Replace ${id}`, spec: detail.spec, existing: p, after: ctx.reload }) } }, 'Edit spec…'),
            button('reset', 'Reset…', 'reset', 'danger'),
            button('remove', 'Remove…', 'trash', 'danger'),
          ],
        }),
        h('nav', { class: 'tabs', 'aria-label': 'Pipeline sections' },
          TABS.map(([key, label]) => h('a', { href: `#/pipelines/${enc(id)}${key === 'overview' ? '' : '/' + key}`, 'aria-current': tab === key ? 'page' : null },
            label,
            key === 'faults' && detail.faults.length > 0 && h('span', { class: ['n', p.state === 'Failed' && 'alert'] }, detail.faults.length),
            key === 'dead-letters' && p.deadLetters > 0 && h('span', { class: 'n alert' }, fmtNum(p.deadLetters))))),
        body);
    },
  };

  function overviewTab(ctx) {
    const { detail, metrics } = ctx.data;
    const p = detail.pipeline, id = p.id;
    const r = ratesOf(id) || p.rates || {};
    const ui = ctx.ui;
    const span = ui.span || 's';
    const running = p.state === 'Running';
    const points = span === 's'
      ? seriesOf(id, 'in').map((v, i, arr) => ({ in: v, out: seriesOf(id, 'out')[i], label: `${arr.length - 1 - i} s ago` }))
      : (metrics.minutes || []).map((m, i, arr) => ({ in: m.rates.in, out: m.rates.out, label: i === arr.length - 1 ? 'the last minute' : `${arr.length - 1 - i} min ago` }));
    const fault = detail.faults[0];

    return h('div', { class: 'stack' },
      !running && p.state !== 'Stopped' && p.state !== 'Unbuilt' && notice(p.state === 'Failed' || p.state === 'Wedged' || p.state === 'NeedsReset' ? 'error' : 'warn',
        h('strong', null, PIPELINE_STATES[p.state] ? PIPELINE_STATES[p.state][2] + '. ' : ''),
        fault ? `${fault.exceptionType}${fault.message ? ': ' + fault.message : ''} ` : '',
        fault && h('a', { href: `#/pipelines/${enc(id)}/faults` }, 'See the faults')),
      p.state === 'Unbuilt' && notice('info', 'This node is a standby: it holds this pipeline’s state as bytes and builds the circuit only when it takes over. Rates and topology are on the leader.'),

      h('div', { class: 'tiles' },
        tile([h('span', { class: 'swatch in' }), 'Records in'], rate(r.in), '/s', `${fmtInt(r.steps || 0)} steps in the last second`, spark(seriesOf(id, 'in'), { label: 'Records in per second', unit: '/s' })),
        tile([h('span', { class: 'swatch out' }), 'Records out'], rate(r.out), '/s', 'upserts, deletes and appends', spark(seriesOf(id, 'out'), { cls: 'out', label: 'Records out per second', unit: '/s' })),
        tile('Step latency', r.stepMs == null ? '–' : fmtMs(r.stepMs), '', r.stepMaxMs == null ? 'no steps in the last second' : `slowest ${fmtMs(r.stepMaxMs)}`, spark(seriesOf(id, 'stepMs'), { cls: 'neutral', label: 'Mean step latency', unit: ' ms' })),
        tile('Sink-ack wait', r.ackMs == null ? '–' : fmtMs(r.ackMs), '', 'per commit, waiting for sinks', spark(seriesOf(id, 'ackMs'), { cls: 'neutral', label: 'Sink acknowledgement wait', unit: ' ms' })),
        tile('Checkpoint age', r.checkpointAgeS == null ? '–' : r.checkpointAgeS < 1 ? '< 1 s' : fmtMs(r.checkpointAgeS * 1000), '', p.committedStep == null ? 'no committed step' : `committed step ${fmtInt(p.committedStep)}`, spark(seriesOf(id, 'checkpointAgeS'), { cls: 'neutral', label: 'Checkpoint age', unit: ' s' })),
        tile('Input lag', r.lag == null ? '–' : fmtNum(r.lag), r.lag == null ? '' : 'records', r.lag == null ? 'its sources report no lag' : 'across its sources', spark(seriesOf(id, 'lag'), { cls: 'neutral', label: 'Input lag in records' }))),

      card('Throughput', span === 's' ? 'records per second, last 60 seconds' : 'records per second, by minute, last hour',
        h('div', { class: 'stack' },
          h('ul', { class: 'legend' },
            h('li', null, h('span', { class: 'swatch in' }), 'In', h('strong', { class: 'num' }, rate(r.in) + '/s')),
            h('li', null, h('span', { class: 'swatch out' }), 'Out', h('strong', { class: 'num' }, rate(r.out) + '/s'))),
          points.length < 2 ? empty('Not enough samples yet', span === 's' ? 'The chart fills as the live feed delivers ticks.' : 'A point is added each minute this node runs the pipeline.')
            : lineChart(points, { width: chartWidth(), span: span === 's' ? 's' : 'min' })),
        { flush: false, extra: h('div', { class: 'seg', role: 'group', 'aria-label': 'Time span' },
          [['s', '60 s'], ['m', '60 min']].map(([k, label]) => h('button', { type: 'button', 'aria-pressed': String(span === k), on: { click: () => { ui.span = k; ctx.render(); } } }, label))) }),

      h('div', { class: 'grid two' },
        card('Sources', 'input lag and positions per slot',
          detail.sources.length === 0
            ? empty(p.inputs && p.inputs.length ? 'Not reading' : 'No inputs', p.inputs && p.inputs.length ? 'Sources run only while the pipeline does.' : 'This definition declares no input slot; it reads tables.')
            : h('div', { class: 'table-wrap' }, h('table', { class: 'data' },
              h('thead', null, h('tr', null, h('th', null, 'Slot'), h('th', { class: 'r' }, 'Records'), h('th', { class: 'r' }, 'Lag'), h('th', { class: 'r' }, 'Unreadable'), h('th', null, 'Read to / committed'))),
              h('tbody', null, detail.sources.map(src => h('tr', { key: src.slot },
                h('td', { class: 'id' }, src.slot, src.pausedSince && h('span', { class: 'sub' }, 'paused ', timeEl(src.pausedSince))),
                h('td', { class: 'r' }, fmtNum(src.records)),
                h('td', { class: 'r' }, src.lag == null ? '–' : fmtNum(src.lag)),
                h('td', { class: 'r' }, fmtNum(src.unreadable)),
                h('td', { class: 'mono wrap' }, offsets(src.positions), ' / ', offsets(src.committed)))))))),
        card('Sinks', 'delivered since this generation started',
          detail.sinks.length === 0
            ? empty(p.sinks && p.sinks.length ? 'Not delivering' : 'No sinks', p.sinks && p.sinks.length ? 'Deliveries run only while the pipeline does.' : 'This definition declares no sink slot; it writes a table.')
            : h('div', { class: 'table-wrap' }, h('table', { class: 'data' },
              h('thead', null, h('tr', null, h('th', null, 'Slot'), h('th', { class: 'r' }, 'Batches'), h('th', { class: 'r' }, 'Upserts'), h('th', { class: 'r' }, 'Deletes'), h('th', { class: 'r' }, 'Appends'), h('th', null, 'Write'))),
              h('tbody', null, detail.sinks.map(sink => h('tr', { key: sink.slot },
                h('td', { class: 'id' }, sink.slot), h('td', { class: 'r' }, fmtNum(sink.batches)), h('td', { class: 'r' }, fmtNum(sink.upserts)),
                h('td', { class: 'r' }, fmtNum(sink.deletes)), h('td', { class: 'r' }, fmtNum(sink.appends)),
                h('td', null, sink.writeInFlightSince ? ['in flight since ', timeEl(sink.writeInFlightSince)] : h('span', { class: 'muted' }, 'idle'))))))))),

      h('div', { class: 'grid two' },
        card('Tables', 'the version its checkpoint has committed, beside the table’s',
          detail.tables.length === 0 ? empty('No tables', 'This pipeline reads and writes no shared table.')
            : h('div', { class: 'table-wrap' }, h('table', { class: 'data' },
              h('thead', null, h('tr', null, h('th', null, 'Table'), h('th', null, 'Use'), h('th', { class: 'r' }, 'Committed'), h('th', { class: 'r' }, 'Current'), h('th', { class: 'r' }, 'Behind'))),
              h('tbody', null, detail.tables.map(t => h('tr', { key: t.name },
                h('td', { class: 'id' }, tableLink(t.name), t.holdsFloor && h('span', { class: 'sub' }, 'holds the retention floor')),
                h('td', null, h('span', { class: 'tag' }, t.writes ? 'writes' : 'reads')),
                h('td', { class: 'r' }, t.committedVersion == null ? '–' : fmtInt(t.committedVersion)),
                h('td', { class: 'r' }, t.currentVersion == null ? '–' : fmtInt(t.currentVersion)),
                h('td', { class: 'r' }, t.versionsBehind == null ? '–' : fmtInt(t.versionsBehind)))))))),
        card('State timeline', 'on this node, newest first',
          !detail.history || detail.history.length === 0 ? empty('No transitions yet', 'State changes on this node appear here.')
            : h('ul', { class: 'timeline scroll' }, detail.history.slice().reverse().map((t, i) => h('li', { key: t.time + i },
              h('span', { class: 'when' }, timeEl(t.time, clock(t.time))),
              h('span', { class: 'rail' }, h('i', { class: PIPELINE_STATES[t.state] ? PIPELINE_STATES[t.state][0] : '' })),
              h('span', { class: 'what' }, h('strong', null, t.state), h('span', { class: 'muted' }, ` · generation ${t.generation}`), t.why && h('span', { class: 'why' }, t.why))))))));
  }

  const offsets = map => {
    const entries = Object.entries(map || {});
    if (!entries.length) return '–';
    return entries.map(([k, v]) => `${k}:${fmtInt(v)}`).join(' ');
  };

  function configTab(ctx) {
    const { detail } = ctx.data;
    const p = detail.pipeline, spec = detail.spec;
    const text = JSON.stringify(spec, null, 2);
    return h('div', { class: 'split' },
      card('Spec', 'as the catalog holds it; secrets are shown as ***',
        h('pre', { class: 'code', tabindex: 0, 'aria-label': 'Pipeline spec as JSON' }, text),
        { flush: false, extra: h('button', { class: 'btn small', type: 'button', on: { click: () => navigator.clipboard.writeText(text).then(() => toast('Spec copied.'), () => toast('Copying is not allowed here.', 'error')) } }, icon('copy', 12), 'Copy') }),
      card('In short', null, kv([
        ['Definition', spec.definition],
        ['Plugin', h('a', { href: '#/plugins' }, `${spec.plugin}@${short(spec.version, 16)}`)],
        ['Desired', spec.desired],
        ['Generation', fmtInt(spec.generation)],
        ['Workers', fmtInt((spec.circuit || {}).workers)],
        ['On a record error', (spec.circuit || {}).errorPolicy === 'DeadLetter' ? 'drop it to the dead letters' : 'fault the pipeline'],
        ['Checkpoint every', plural((spec.circuit || {}).checkpointInterval || 1, 'step')],
        ['Inputs', Object.keys(spec.inputs || {}).join(', ')],
        ['Sinks', Object.keys(spec.sinks || {}).join(', ')],
        ['Reads', (spec.reads || []).length ? spec.reads.map((t, i) => [i > 0 && ', ', tableLink(t)]) : null],
        ['Writes', (spec.writes || []).length ? spec.writes.map((t, i) => [i > 0 && ', ', tableLink(t)]) : null],
        ['Restarts', `${fmtInt(p.restarts)} since it last ran clean`],
      ]), { flush: false }));
  }

  function faultsTab(ctx) {
    const { detail } = ctx.data;
    const hidden = store.info && !store.info.exposeRowData;
    if (detail.faults.length === 0)
      return card('Faults', null, empty('No faults', 'This pipeline has not faulted on this node since it started leading.'));
    return card('Faults', `the last ${detail.faults.length} on this node, newest first`,
      h('div', null,
        hidden && h('div', { class: 'record' }, notice('info', 'Messages and stacks are hidden: this admin API does not expose row data, and an exception message can quote a row.')),
        detail.faults.map((f, i) => h('div', { class: 'record', key: f.time + i },
          h('div', { class: 'head' }, h('strong', { class: 'mono' }, f.exceptionType), h('span', { class: 'when' }, timeEl(f.time, `${stamp(f.time)} · ${ago(f.time)}`)), h('span', { class: 'tag' }, `generation ${f.generation}`)),
          f.message && h('div', { class: 'err' }, f.message),
          f.stack && h('details', { class: 'more', open: i === 0 || null }, h('summary', null, 'Stack'), h('pre', { class: 'code' }, f.stack))))));
  }

  /** A dropped record: on one line when it is short, formatted on request. */
  function recordBlock(record) {
    let compact = record;
    try { compact = JSON.stringify(JSON.parse(record)); } catch { /* not JSON: shown as it is */ }
    if (compact === record && record.includes('\n')) return h('pre', { class: 'code wrap' }, record);
    return h('div', null,
      h('pre', { class: 'code wrap' }, compact),
      compact.length > 60 && h('details', { class: 'more' }, h('summary', null, 'Formatted'), h('pre', { class: 'code' }, pretty(record))));
  }

  function letterList(letters, hiddenNote) {
    return letters.items.slice().reverse().map(l => h('div', { class: 'record', key: 'l' + l.sequence },
      h('div', { class: 'head' },
        h('strong', { class: 'num' }, '#' + l.sequence), h('span', { class: 'when' }, timeEl(l.time, `${stamp(l.time)} · ${ago(l.time)}`)),
        h('span', { class: 'tag', title: 'The operator that dropped it' }, l.operator),
        h('span', { class: 'tag', title: 'Weight: +1 an insert, −1 a retraction' }, (l.weight > 0 ? '+' : '') + l.weight),
        l.position && h('span', { class: 'tag', title: 'Source position' }, l.position)),
      h('div', { class: 'err' }, h('span', { class: 'mono' }, l.exceptionType), l.message ? ': ' + l.message : ''),
      l.record ? recordBlock(l.record) : hiddenNote && h('div', { class: 'muted' }, 'Record hidden.')));
  }

  function lettersTab(ctx, id) {
    const { letters, detail } = ctx.data;
    if (!letters) return card('Dead letters', null, A.skeleton());
    const hidden = !letters.rowDataExposed;
    const policy = detail && detail.pipeline.errorPolicy;
    return card('Dead letters', `${fmtInt(letters.total)} dropped; the newest ${fmtInt(letters.kept)} are kept on this node`,
      h('div', null,
        hidden && h('div', { class: 'record' }, notice('info', 'Records and messages are hidden: this admin API does not expose row data. The operator, the time and the exception type are shown.')),
        letters.items.length === 0
          ? empty(letters.total ? 'None kept on this node' : 'No dead letters',
            letters.total ? 'The ring is per node and in memory: a new leader starts it empty.'
              : policy === 'DeadLetter' ? 'Records its operators cannot process are dropped here instead of faulting the pipeline.'
              : 'This pipeline faults on a bad record instead of dropping it (its error policy is Fault).')
          : letterList(letters, hidden),
        (letters.hasOlder || ctx.ui.before) && h('div', { class: 'pager' },
          h('span', { class: 'info' }, ctx.ui.before ? `Older than #${ctx.ui.before}` : 'Newest'),
          ctx.ui.before && h('button', { class: 'btn small', type: 'button', on: { click: () => { ctx.ui.before = null; ctx.reload(); } } }, 'Newest'),
          letters.hasOlder && h('button', { class: 'btn small', type: 'button', on: { click: () => { ctx.ui.before = letters.items[0].sequence; ctx.reload(); } } }, 'Older'))));
  }

  // ── topology: a layered drawing of the circuit ──────────────────────────────────────

  const NODE_H = 42, GAP_Y = 12, GAP_X = 34, PAD = 20;
  const ROLE_TAG = { input: 'input', table: 'table', sink: 'sink', 'table-out': 'writes table', circuit: 'circuit' };
  const clip = (text, max) => (text.length > max ? text.slice(0, max - 1) + '…' : text);
  /** The second line of a node: what kind of edge it is and what it is bound to. */
  const subText = n => clip(n.role === 'operator' || n.role === 'circuit' ? (n.detail || '') : [ROLE_TAG[n.role].toLowerCase(), n.role === 'table' || n.role === 'table-out' ? null : n.detail].filter(Boolean).join(' · '), 28);

  function layout(nodes) {
    const byId = new Map(nodes.map(n => [n.id, n]));
    const outs = new Map(nodes.map(n => [n.id, []]));
    for (const n of nodes) for (const i of n.inputs) if (outs.has(i)) outs.get(i).push(n.id);
    const layer = new Map();
    for (const n of nodes) layer.set(n.id, n.inputs.length ? 1 + Math.max(...n.inputs.map(i => layer.get(i) ?? 0)) : 0);
    // Sources sit just before their first consumer, not all in the first column: short edges.
    for (const n of nodes) if (!n.inputs.length && outs.get(n.id).length) layer.set(n.id, Math.max(0, Math.min(...outs.get(n.id).map(o => layer.get(o))) - 1));
    const depth = Math.max(0, ...layer.values());
    for (const n of nodes) if ((n.role === 'sink' || n.role === 'table-out') && !outs.get(n.id).length) layer.set(n.id, depth);

    const cols = Array.from({ length: depth + 1 }, () => []);
    for (const n of nodes) cols[layer.get(n.id)].push(n.id);
    const order = new Map();
    cols.forEach(col => col.forEach((id, i) => order.set(id, i)));
    const mean = ids => (ids.length ? ids.reduce((a, id) => a + order.get(id), 0) / ids.length : null);
    for (let sweep = 0; sweep < 6; sweep++) {
      const forward = sweep % 2 === 0;
      for (const col of forward ? cols : cols.slice().reverse()) {
        const key = new Map(col.map(id => [id, mean(forward ? byId.get(id).inputs.filter(i => order.has(i)) : outs.get(id)) ?? order.get(id)]));
        col.sort((a, b) => key.get(a) - key.get(b));
        col.forEach((id, i) => order.set(id, i));
      }
    }

    const width = n => (n.role === 'exchange' ? 30 : Math.max(84, Math.min(200, 26 + 7.4 * Math.max(clip(n.label, 22).length, subText(n).length * 0.8))));
    const pos = new Map();
    let x = PAD;
    const tallest = Math.max(...cols.map(c => c.length));
    const step = NODE_H + GAP_Y;
    const height = PAD * 2 + tallest * step - GAP_Y;
    cols.forEach(col => {
      const w = Math.max(...col.map(id => width(byId.get(id))), 30);
      col.forEach((id, i) => {
        const n = byId.get(id), nw = width(n);
        // Spread each column over the full height: a fan-out then reads as a tree.
        pos.set(id, { x: x + (w - nw) / 2, y: PAD + (i + 0.5) * (tallest * step) / col.length - step / 2, w: nw, h: NODE_H });
      });
      x += w + GAP_X;
    });
    // Then pull each node toward the middle of its neighbours, keeping the column's order
    // and its spacing: straight chains come out straight.
    // Nodes of a column that want the same place share it out around that place, in
    // order (pool-adjacent-violators), instead of queueing below it.
    const settle = col => {
      const blocks = [];
      col.forEach(id => {
        let block = { ids: [id], sum: pos.get(id).y };
        while (blocks.length) {
          const prev = blocks[blocks.length - 1];
          if (prev.sum / prev.ids.length + prev.ids.length * step <= block.sum / block.ids.length) break;
          blocks.pop();
          block = { ids: prev.ids.concat(block.ids), sum: prev.sum + block.sum - block.ids.length * prev.ids.length * step };
        }
        blocks.push(block);
      });
      for (const block of blocks) {
        const top = block.sum / block.ids.length;
        block.ids.forEach((id, k) => { pos.get(id).y = top + k * step; });
      }
      let floor = PAD;
      for (const id of col) { const p = pos.get(id); p.y = Math.max(p.y, floor); floor = p.y + step; }
      let ceiling = height - PAD - NODE_H;
      for (const id of col.slice().reverse()) { const p = pos.get(id); p.y = Math.min(p.y, ceiling); ceiling = p.y - step; }
    };
    for (let pass = 0; pass < 8; pass++) {
      for (const col of pass % 2 === 0 ? cols : cols.slice().reverse()) {
        for (const id of col) {
          const near = (pass % 2 === 0 ? byId.get(id).inputs : outs.get(id)).filter(i => pos.has(i));
          if (near.length) pos.get(id).y = near.reduce((a, i) => a + pos.get(i).y, 0) / near.length;
        }
        settle(col);
      }
    }
    return { pos, width: x - GAP_X + PAD, height, outs };
  }

  function topologyTab(ctx) {
    const { graph, detail } = ctx.data;
    if (!graph) return card('Topology', null, A.skeleton());
    const ui = ctx.ui;
    const { pos, width, height } = layout(graph.nodes);
    const byId = new Map(graph.nodes.map(n => [n.id, n]));
    const avail = Math.max(280, A.$('main').clientWidth - 50);
    const fit = Math.min(1, avail / width);
    const zoom = ui.zoom || (fit >= 0.85 ? fit : 1);
    const selected = ui.node && byId.get(ui.node);
    const related = new Set();
    if (selected) {
      related.add(selected.id);
      selected.inputs.forEach(i => related.add(i));
      graph.nodes.filter(n => n.inputs.includes(selected.id)).forEach(n => related.add(n.id));
    }
    const dead = graph.deadLetters || {};
    const select = id => { ui.node = ui.node === id ? null : id; ctx.render(); };

    const edges = [];
    for (const n of graph.nodes) {
      for (const from of n.inputs) {
        const a = pos.get(from), b = pos.get(n.id);
        if (!a || !b) continue;
        const x1 = a.x + a.w, y1 = a.y + a.h / 2, x2 = b.x, y2 = b.y + b.h / 2, bend = Math.max(18, (x2 - x1) / 2);
        edges.push(s('path', { class: ['g-edge', selected && (from === selected.id || n.id === selected.id) && 'hl'], d: `M${x1},${y1} C${x1 + bend},${y1} ${x2 - bend},${y2} ${x2},${y2}`, 'marker-end': 'url(#arrow)' }));
      }
    }

    const drawn = graph.nodes.map(n => {
      const p = pos.get(n.id);
      const describe = `${ROLE_TAG[n.role] ? ROLE_TAG[n.role].toLowerCase() : n.role} ${n.label}${n.detail ? ', ' + n.detail : ''}${n.inputs.length ? '. Reads ' + n.inputs.map(i => byId.get(i) ? byId.get(i).label : i).join(', ') : ''}`;
      const common = {
        class: ['g-node', n.role, n.name && 'named', ui.node === n.id && 'sel'], tabindex: 0, role: 'button', 'aria-label': describe, 'aria-pressed': String(ui.node === n.id),
        transform: `translate(${p.x.toFixed(1)},${p.y.toFixed(1)})`, key: n.id,
        on: { click: () => select(n.id), keydown: e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); select(n.id); } } },
      };
      const count = n.stateName && dead[n.stateName];
      const badgeMark = count ? [s('circle', { class: 'dl', cx: p.w - 2, cy: 2, r: 9 }), s('text', { class: 'dlt', x: p.w - 2, y: 5.5, 'text-anchor': 'middle' }, count > 99 ? '99+' : count)] : null;
      if (n.role === 'exchange')
        return s('g', common, s('title', null, 'exchange: repartitions records across workers by key'), s('circle', { cx: 15, cy: p.h / 2, r: 13 }), s('text', { x: 15, y: p.h / 2 + 4, 'text-anchor': 'middle' }, '⇄'), badgeMark);
      const sub = subText(n);
      return s('g', common,
        s('title', null, describe),
        s('rect', { width: p.w, height: p.h, rx: 6 }),
        s('rect', { class: 'bar', width: 4, height: p.h - 8, x: 3, y: 4, rx: 2 }),
        s('text', { class: 't', x: 14, y: sub ? 18 : 26 }, clip(n.label, 22)),
        sub && s('text', { class: 's', x: 14, y: 33 }, sub),
        badgeMark);
    });

    const svg = s('svg', { width: Math.round(width * zoom), height: Math.round(height * zoom), viewBox: `0 0 ${Math.round(width)} ${Math.round(height)}`, role: 'group', 'aria-label': `Circuit of ${detail.pipeline.id}: ${graph.nodes.length} nodes` },
      s('defs', null, s('marker', { id: 'arrow', viewBox: '0 0 8 8', refX: 7, refY: 4, markerWidth: 7, markerHeight: 7, orient: 'auto' }, s('path', { d: 'M0,0.5 L7,4 L0,7.5 z', fill: 'context-stroke' }))),
      s('g', null, edges), s('g', null, drawn));

    const zoomTo = z => () => { ui.zoom = Math.min(2, Math.max(0.3, z)); ctx.render(); };
    return h('div', { class: 'stack' },
      graph.source === 'declared' && notice('info', 'This node has not built this pipeline’s circuit (a standby builds at takeover), so this is the declared edges only: its input slots, tables and sinks around one box. The leader shows the operators.'),
      card('Topology', `${graph.nodes.length} nodes · ${graph.source === 'circuit' ? 'the circuit as its definition declares it' : 'declared edges'}`,
        h('div', null,
          h('div', { class: 'graph-wrap', tabindex: 0, 'aria-label': 'Topology drawing; scroll to pan' }, svg),
          h('div', { class: 'graph-side', 'aria-live': 'polite' },
            selected ? kv([
              ['Node', h('strong', null, selected.label)],
              ['Kind', h('span', { class: 'mono' }, selected.kind || selected.role)],
              selected.detail && ['Bound to', selected.detail],
              selected.stateName && ['State stores', h('span', null, h('span', { class: 'mono' }, selected.stateName), selected.name ? ' — named, so it survives changes to the circuit around it' : ' — positional: an upstream change renumbers it')],
              selected.errorPolicy && ['On a record error', selected.errorPolicy === 'DeadLetter' ? 'drop the record to the dead letters' : 'fault the pipeline'],
              selected.stateName && dead[selected.stateName] && ['Dead letters', h('a', { href: `#/pipelines/${enc(detail.pipeline.id)}/dead-letters` }, `${dead[selected.stateName]} of those kept came from here`)],
              ['Reads', selected.inputs.length ? selected.inputs.map(i => byId.get(i) ? byId.get(i).label : i).join(', ') : 'nothing: it is a source'],
            ]) : h('span', { class: 'muted' }, 'Select a node to see its kind, its state stores and what it reads. Tab moves between nodes.'))),
        { extra: [
          h('ul', { class: 'legend' },
            h('li', null, h('span', { class: 'swatch in' }), 'input'), h('li', null, h('span', { class: 'swatch tbl' }), 'table'),
            h('li', null, h('span', { class: 'swatch out' }), 'sink'), h('li', null, '⇄ exchange')),
          h('div', { class: 'seg', role: 'group', 'aria-label': 'Zoom' },
            h('button', { type: 'button', 'aria-label': 'Zoom out', on: { click: zoomTo(zoom - 0.15) } }, '−'),
            h('button', { type: 'button', 'aria-label': 'Fit to width', on: { click: zoomTo(fit) } }, 'Fit'),
            h('button', { type: 'button', 'aria-label': 'Actual size', on: { click: zoomTo(1) } }, '100%'),
            h('button', { type: 'button', 'aria-label': 'Zoom in', on: { click: zoomTo(zoom + 0.15) } }, '+')),
        ] }));
  }

  // ── tables ──────────────────────────────────────────────────────────────────────────

  const writerText = w => (w === 'none' ? 'the host' : w.startsWith('pipeline:') ? pipelineLink(w.slice(9)) : w.startsWith('loader:') ? `${w.slice(7)} loader` : w);

  views.tables = {
    title: () => 'Tables',
    kinds: ['table', 'node', 'catalog'],
    load: async () => ({ tables: await all('tables') }),
    render({ data }) {
      const { tables } = data;
      const standby = store.node && store.node.role === 'Standby';
      return h('div', { class: 'stack' },
        pageHead({ title: 'Tables', lede: 'Shared reference tables: held once per node, read by any number of pipelines.' }),
        h('section', { class: 'card' },
          tables.items.length === 0 ? empty('No tables are declared', 'Tables are declared by the host or through the API (POST api/tables).')
            : h('div', { class: 'table-wrap' }, h('table', { class: 'data cards' },
              h('thead', null, h('tr', null, h('th', null, 'Table'), h('th', null, 'State'), h('th', { class: 'r' }, 'Rows'), h('th', { class: 'r' }, 'Version'),
                h('th', { class: 'r' }, standby ? 'Behind' : 'Versions /s'), h('th', null, 'Written by'), h('th', null, 'Loader'), h('th', { class: 'r' }, 'Readers'), h('th', null, 'Floor held by'))),
              h('tbody', null, tables.items.map(t => h('tr', { key: t.name },
                td('Table', 'id', tableLink(t.name), h('span', { class: 'sub' }, `${t.keyType} → ${t.rowType}`)),
                td('State', null, tableBadge(t.state)),
                td('Rows', 'r', fmtNum(t.rows)),
                td('Version', 'r', fmtInt(t.version)),
                td(standby ? 'Behind' : 'Versions /s', 'r', standby ? (t.lagVersions == null ? '–' : fmtInt(t.lagVersions)) : fmtNum(t.versionsPerSecond)),
                td('Written by', null, t.writer ? writerText(t.writer) : '–'),
                td('Loader', null, t.loader ? [h('span', { class: 'tag' }, t.loader.kind), ' ', t.loader.state, t.loader.unreadable > 0 && h('span', { class: 'sub' }, plural(t.loader.unreadable, 'unreadable record'))] : h('span', { class: 'muted' }, 'none')),
                td('Readers', 'r', fmtInt(t.readers)),
                td('Floor held by', null, t.floorHeldBy ? pipelineLink(t.floorHeldBy) : h('span', { class: 'muted' }, 'nobody')))))))));
    },
  };

  views.table = {
    title: args => args[0],
    kinds: ['table', 'pipeline'],
    load: async ctx => ({ detail: await api('tables/' + enc(ctx.args[0])) }),
    render(ctx) {
      const name = ctx.args[0];
      const { detail } = ctx.data;
      const t = detail.table, ui = ctx.ui;
      const standby = t.following;
      const acting = canAct();
      const exposing = store.info && store.info.exposeRowData;
      return h('div', { class: 'stack' },
        pageHead({
          crumbs: h('a', { href: '#/tables' }, 'Tables'),
          title: [h('span', { class: 'mono' }, name), tableBadge(t.state)],
          lede: `${t.keyType} → ${t.rowType} · ${t.definition}`,
          actions: acting && [
            t.loader && h('button', { class: 'btn', type: 'button', on: { click: () => A.tableAction('loader-restart', t, ctx.reload) } }, icon('restart', 13), 'Restart loader'),
            h('button', { class: 'btn', type: 'button', on: { click: () => A.tableAction('reopen', t, ctx.reload) } }, 'Reopen'),
            h('button', { class: 'btn danger', type: 'button', on: { click: () => A.tableAction('remove', t, () => { location.hash = '#/tables'; }) } }, icon('trash', 13), 'Remove…'),
          ],
        }),
        t.fault && notice('error', h('strong', null, 'This table has faulted. '), t.fault),
        h('div', { class: 'tiles' },
          tile('Rows', fmtNum(t.rows), '', fmtInt(t.rows) + ' exactly'),
          tile('Version', fmtInt(t.version), '', t.oldestRetainedVersion >= t.version ? 'no older version kept' : `history back to version ${fmtInt(t.oldestRetainedVersion)}`),
          standby ? tile('Behind the log', t.lagVersions == null ? '–' : fmtInt(t.lagVersions), t.lagVersions == null ? '' : 'versions', 'this node follows the table')
            : tile('Change rate', fmtNum(t.versionsPerSecond), 'versions/s', `history capped at ${fmtInt(detail.retainedHistoryVersions)} versions`),
          tile('Written by', h('span', { class: 'mono' }, t.writer === 'none' ? 'host' : t.writer.split(':')[0]), '', t.writer === 'none' ? 'no loader and no pipeline' : t.writer.split(':')[1])),

        h('div', { class: 'grid two' },
          card('Loader', t.loader ? t.loader.kind : null, t.loader ? h('div', { class: 'stack' },
            kv([
              ['State', h('span', { class: ['badge', t.loader.state === 'Faulted' ? 'crit' : t.loader.state === 'Streaming' || t.loader.state === 'Idle' ? 'good' : 'info'] }, t.loader.state)],
              ['Position', h('span', { class: 'mono' }, t.loader.position)],
              ['Lag', t.loader.lag == null ? 'not reported' : plural(t.loader.lag, 'record')],
              ['Unreadable', `${plural(t.loader.unreadable, 'record')} · policy: ${detail.onUnreadable === 'Skip' ? 'skip and count' : 'fault at the record'}`],
              ['Restarts', fmtInt(t.loader.restarts)],
              t.loader.fault && ['Fault', h('span', { class: 'err' }, t.loader.fault)],
            ]),
            detail.unreadable.length > 0 && h('details', { class: 'more' }, h('summary', null, `The last ${plural(detail.unreadable.length, 'unreadable record')}`),
              detail.unreadable.slice().reverse().map(u => h('div', { class: 'record', key: 'u' + u.sequence },
                h('div', { class: 'head' }, h('span', { class: 'tag' }, u.position), h('span', { class: 'when' }, timeEl(u.time)), h('span', { class: 'mono' }, u.exceptionType)),
                u.message && h('div', { class: 'err' }, u.message), u.record && h('pre', { class: 'code wrap' }, u.record)))))
            : empty('No loader', t.writer.startsWith('pipeline:') ? ['Pipeline ', pipelineLink(t.writer.slice(9)), ' writes this table.'] : 'The host writes this table itself.'), { flush: !t.loader }),
          card('Indexes', plural(t.indexes.length, 'index', 'indexes'),
            t.indexes.length === 0 ? empty('No secondary index', 'Rows are found by key only.')
              : h('table', { class: 'data' }, h('thead', null, h('tr', null, h('th', null, 'Index'), h('th', null, 'Key type'), h('th', null))),
                h('tbody', null, t.indexes.map(i => h('tr', { key: i.name }, h('td', { class: 'id' }, i.name), h('td', { class: 'mono' }, i.keyType || '–'),
                  h('td', { class: 'r' }, exposing && h('button', { class: 'btn small ghost', type: 'button', on: { click: () => { ui.mode = 'index:' + i.name; ui.key = ''; ctx.render(); document.getElementById('row-key')?.focus(); } } }, 'Look up')))))))),

        card('Readers and writers', 'the version each pipeline’s checkpoint has committed; the furthest behind holds the retention floor',
          detail.pipelines.length === 0 ? empty('No pipeline names this table', 'It can be removed, or read by the next pipeline deployed.')
            : h('div', { class: 'table-wrap' }, h('table', { class: 'data cards' },
              h('thead', null, h('tr', null, h('th', null, 'Pipeline'), h('th', null, 'Use'), h('th', { class: 'r' }, 'Committed version'), h('th', { class: 'r' }, 'Behind'), h('th', null, 'Until cut loose'), h('th', null, 'Retention'))),
              h('tbody', null, detail.pipelines.map(p => {
                const reader = detail.readers.find(r => r.pipeline === p.id);
                const cap = detail.retainedHistoryVersions || 1;
                const left = reader ? reader.versionsUntilCutLoose : null;
                const meter = h('span');
                if (reader) meter.style.width = Math.round(100 * (1 - left / cap)) + '%';
                return h('tr', { key: p.id },
                  td('Pipeline', 'id', pipelineLink(p.id)),
                  td('Use', null, h('span', { class: 'tag' }, p.writes ? 'writes' : 'reads')),
                  td('Committed version', 'r', p.committedVersion == null ? h('span', { class: 'muted', title: 'It has not committed a checkpoint that reads this table' }, 'none') : fmtInt(p.committedVersion)),
                  td('Behind', 'r', reader ? fmtInt(reader.versionsBehind) : '–'),
                  td('Until cut loose', null, reader ? h('div', null, h('div', { class: 'meter', role: 'img', 'aria-label': `${fmtInt(left)} of ${fmtInt(cap)} versions of history left` }, meter),
                    h('span', { class: 'sub' }, `${fmtInt(left)} versions${reader.timeUntilCutLoose != null ? ' · about ' + fmtMs(reader.timeUntilCutLoose) + ' at this rate' : ''}`)) : '–'),
                  td('Retention', null, p.holdsFloor ? h('span', { class: 'badge warn' }, icon('lock', 12), 'holds the floor') : ''));
              }))))),

        rowBrowser(ctx, name, t, exposing));
    },
  };

  function rowBrowser(ctx, name, t, exposing) {
    const ui = ctx.ui;
    if (!exposing)
      return card('Rows', null, empty('Row data is not exposed', 'This admin API is configured without row data (ServiceAdminOptions.ExposeRowData), so rows, records and exception messages are not returned.'));
    ui.mode = ui.mode || 'browse';
    ui.limit = ui.limit || 15;
    const run = async (skip = 0) => {
      ui.skip = skip; ui.busy = true; ui.rowError = null; ctx.render();
      const params = new URLSearchParams({ limit: ui.limit });
      if (ui.mode === 'browse') { params.set('skip', skip); if (ui.contains) params.set('contains', ui.contains); }
      else { params.set('key', ui.key || ''); if (ui.mode.startsWith('index:')) params.set('index', ui.mode.slice(6)); }
      try {
        ui.rows = await api(`tables/${enc(name)}/rows?${params}`);
        ui.rowsFor = ui.mode;
      } catch (e) { ui.rowError = e; ui.rows = null; }
      ui.busy = false; ctx.render();
    };
    if (!ui.rows && !ui.busy && !ui.rowError && !ui.started && t.state !== 'Opening') { ui.started = true; setTimeout(() => run(0)); }
    const rows = ui.rows;
    const columns = rows ? [...new Set(rows.rows.flatMap(r => (r.row && typeof r.row === 'object' ? Object.keys(r.row) : ['value'])))].slice(0, 12) : [];
    const cell = v => (v == null ? '' : typeof v === 'object' ? JSON.stringify(v) : String(v));
    const lookup = ui.mode !== 'browse';

    return card('Rows', 'read from this node’s copy, at one version',
      h('div', null,
        h('form', { class: 'toolbar', on: { submit: e => { e.preventDefault(); run(0); } } },
          h('select', { class: 'input', 'aria-label': 'How to read', on: { change: e => { ui.mode = e.target.value; ui.rows = null; ui.rowError = null; if (e.target.value === 'browse') run(0); else ctx.render(); } } },
            h('option', { value: 'browse', selected: ui.mode === 'browse' }, 'Browse'),
            h('option', { value: 'key', selected: ui.mode === 'key' }, `By key (${t.keyType})`),
            t.indexes.map(i => h('option', { value: 'index:' + i.name, selected: ui.mode === 'index:' + i.name }, `By index ${i.name} (${i.keyType || '?'})`))),
          lookup
            ? h('input', { class: 'input grow', id: 'row-key', placeholder: ui.mode === 'key' ? 'A key, e.g. 42 or CP0003' : 'An index key', 'aria-label': 'Key', value: ui.key || '', on: { input: e => { ui.key = e.target.value; } } })
            : h('input', { class: 'input grow', type: 'search', placeholder: 'Contains text (scans rows)', 'aria-label': 'Rows containing', value: ui.contains || '', on: { input: e => { ui.contains = e.target.value; } } }),
          h('button', { class: 'btn', type: 'submit', disabled: ui.busy || (lookup && !ui.key) }, ui.busy ? 'Reading…' : lookup ? 'Look up' : 'Read')),
        ui.rowError && h('div', { class: 'record' }, errorNotice(ui.rowError, 'The rows could not be read. ')),
        !rows && !ui.rowError && (lookup ? empty('Enter a key', 'A lookup reads one row by key, or the rows under one index key.') : A.skeleton()),
        rows && rows.rows.length === 0 && empty(lookup ? 'No row under that key' : 'No rows',
          rows.truncated ? `The scan stopped after ${fmtInt(rows.scanned)} of ${fmtInt(rows.tableRows)} rows, so a match may exist further on.`
            : rows.lagVersions ? `This node is ${plural(rows.lagVersions, 'version')} behind the table’s log: the row may not have arrived here yet.`
            : lookup ? `Not in version ${fmtInt(rows.version)} of the table.` : ui.contains ? 'No row contains that text.' : 'The table is empty.'),
        rows && rows.rows.length > 0 && h('div', { class: 'table-wrap' }, h('table', { class: 'data rowgrid' },
          h('thead', null, h('tr', null, h('th', null, 'Key'), columns.map(c => h('th', null, c)))),
          h('tbody', null, rows.rows.map((r, i) => h('tr', { key: 'r' + i + cell(r.key) },
            h('td', { class: 'cell id', title: cell(r.key) }, cell(r.key)),
            columns.map(c => { const v = r.row && typeof r.row === 'object' ? r.row[c] : r.row; return h('td', { class: 'cell', title: cell(v) }, cell(v)); })))))),
        rows && h('div', { class: 'pager' },
          h('span', { class: 'info' },
            `Version ${fmtInt(rows.version)} · ${fmtInt(rows.rows.length)} shown · scanned ${fmtInt(rows.scanned)} of ${fmtInt(rows.tableRows)}`,
            rows.truncated && ' · the scan stopped at its bound', rows.lagVersions ? ` · this node is ${plural(rows.lagVersions, 'version')} behind` : ''),
          !lookup && h('button', { class: 'btn small', type: 'button', disabled: !ui.skip || ui.busy, on: { click: () => run(Math.max(0, ui.skip - ui.limit)) } }, 'Previous'),
          !lookup && h('button', { class: 'btn small', type: 'button', disabled: !rows.more || ui.busy, on: { click: () => run(ui.skip + ui.limit) } }, 'Next'),
          h('button', { class: 'btn small ghost', type: 'button', disabled: ui.busy, on: { click: () => run(ui.skip || 0) } }, 'Refresh'))),
      { flush: true });
  }

  // ── plugins ─────────────────────────────────────────────────────────────────────────

  function definitionsTable(p) {
    if (p.definitions.length === 0) return null;
    const settings = d => (d.settings.length === 0 ? h('span', { class: 'muted' }, 'none')
      : d.settings.map(st => h('div', { key: st.name, title: st.description || null },
        h('span', { class: 'mono' }, st.name), h('span', { class: 'muted' }, `: ${st.jsonType}`),
        st.required && [' ', h('span', { class: 'tag' }, 'required')],
        st.stateNeutral && [' ', h('span', { class: 'tag', title: 'Changing it keeps the pipeline’s state without being asked' }, 'state-neutral')])));
    const row = d => h('tr', { key: d.name },
      h('td', { class: 'id' }, d.name),
      h('td', { class: 'wrap' }, d.description || h('span', { class: 'muted' }, 'none')),
      h('td', { class: 'wrap' }, settings(d)));
    return h('div', { class: 'table-wrap' }, h('table', { class: 'data' },
      h('thead', null, h('tr', null, h('th', null, 'Definition'), h('th', null, 'Description'), h('th', null, 'Settings'))),
      h('tbody', null, p.definitions.map(row))));
  }

  views.plugins = {
    title: () => 'Plugins',
    kinds: ['plugin', 'pipeline', 'catalog'],
    every: 5000,
    load: async () => ({ plugins: await all('plugins'), node: await api('node') }),
    render(ctx) {
      const { plugins, node } = ctx.data;
      const ui = ctx.ui, info = store.info || {};
      const groups = new Map();
      for (const p of plugins.items) groups.set(p.name, [...(groups.get(p.name) || []), p]);

      const upload = async file => {
        if (!file) return;
        ui.upload = { name: file.name, busy: true }; ctx.render();
        try {
          const result = await api('plugins', { method: 'POST', raw: file, headers: { 'Content-Type': 'application/zip' } });
          ui.upload = { name: file.name, done: result.plugin };
          ctx.reload();
        } catch (e) {
          ui.upload = { name: file.name, error: e };
        }
        ctx.render();
      };
      const rescan = async () => {
        ui.rescan = { busy: true }; ctx.render();
        try {
          const result = await api('plugins/rescan', { method: 'POST' });
          ui.rescan = null;
          toast(result.imported.length ? `Imported ${result.imported.map(p => p.name).join(', ')}.` : 'Nothing new in the plugin directory.');
          ctx.reload();
        } catch (e) { ui.rescan = { error: e }; ctx.render(); }
      };

      return h('div', { class: 'stack' },
        pageHead({
          title: 'Plugins', lede: `Assemblies that hold pipeline definitions and table contracts. ${plural(node.liveContexts, 'plugin load context')} alive on this node.`,
          actions: canAct() && info.allowPluginRescan && h('button', { class: 'btn', type: 'button', disabled: ui.rescan && ui.rescan.busy, on: { click: rescan } }, icon('restart', 13), ui.rescan && ui.rescan.busy ? 'Scanning…' : 'Rescan directory'),
        }),
        ui.rescan && ui.rescan.error && errorNotice(ui.rescan.error, 'The rescan failed. '),

        canAct() && (info.allowPluginUpload
          ? card('Upload a plugin', 'a zip of the plugin’s build output: its assembly, its .deps.json and its private dependencies',
            h('div', { class: 'stack' },
              h('div', { class: ['drop', ui.over && 'over'], on: {
                dragover: e => { e.preventDefault(); if (!ui.over) { ui.over = true; ctx.render(); } },
                dragleave: () => { ui.over = false; ctx.render(); },
                drop: e => { e.preventDefault(); ui.over = false; upload(e.dataTransfer.files[0]); },
              } },
                icon('upload', 20),
                h('div', null, h('label', { for: 'plugin-file' }, h('strong', null, 'Choose a zip')), ' or drop one here. ', h('span', { class: 'muted' }, `At most ${fmtBytes(info.maxUploadBytes)}.`)),
                h('input', { type: 'file', id: 'plugin-file', accept: '.zip,application/zip', on: { change: e => { upload(e.target.files[0]); e.target.value = ''; } } })),
              ui.upload && ui.upload.busy && h('div', { class: 'notice', role: 'status' }, `Uploading and checking ${ui.upload.name}…`),
              ui.upload && ui.upload.done && notice('info', h('strong', null, `Stored ${ui.upload.done.name} version ${short(ui.upload.done.version, 16)}. `),
                `${plural(ui.upload.done.files, 'file')}, ${fmtBytes(ui.upload.done.bytes)}; ${plural(ui.upload.done.definitions.length, 'definition')}${ui.upload.done.definitions.length ? ': ' + ui.upload.done.definitions.map(d => d.name).join(', ') : ''}.`),
              ui.upload && ui.upload.error && errorNotice(ui.upload.error, `${ui.upload.name} was refused. `)),
            { flush: false })
          : notice('info', 'Uploading plugins over this API is switched off (ServiceAdminOptions.AllowPluginUpload). Plugins come from the host’s plugin directory.')),

        plugins.items.length === 0 ? h('section', { class: 'card' }, empty('No plugins', 'Nothing is compiled into the host or stored in the catalog.'))
          : [...groups].map(([name, versions]) => card(h('span', { class: 'mono' }, name), `${versions[0].kind === 'Contract' ? 'contract' : 'pipeline plugin'} · ${plural(versions.length, 'version')}`,
            h('div', null, versions.map(p => h('div', { class: 'record', key: p.version },
              h('div', { class: 'head' },
                h('strong', { class: 'mono', title: p.version }, short(p.version, 16)),
                h('span', { class: ['badge', p.loadState === 'loaded' || p.loadState === 'built-in' ? 'good' : p.loadState === 'pending' ? 'warn' : 'idle'],
                  title: { 'built-in': 'Compiled into the host', loaded: 'Loaded on this node', stored: 'Stored; loaded when a pipeline needs it', pending: 'A newer contract version: loaded when the process restarts' }[p.loadState] }, p.loadState),
                p.files != null && h('span', { class: 'muted' }, `${plural(p.files, 'file')} · ${fmtBytes(p.bytes)}`),
                h('span', { class: 'muted' }, p.users ? `used by ${plural(p.users, 'pipeline')}` : 'unused'),
                canAct() && !p.builtIn && h('button', { class: 'btn small danger', type: 'button', disabled: p.users > 0, title: p.users > 0 ? 'A pipeline names this version' : null, on: { click: () => A.pluginDelete(p, ctx.reload) } }, 'Delete…')),
              p.pipelines.length > 0 && h('div', null, h('span', { class: 'muted' }, 'Pipelines: '), p.pipelines.map((x, i) => [i > 0 && ', ', pipelineLink(x.id)])),
              definitionsTable(p),
              p.tableDefinitions.length > 0 && h('div', null, h('span', { class: 'muted' }, 'Table definitions: '), h('span', { class: 'mono' }, p.tableDefinitions.map(x => x.split('.').pop()).join(', ')))))))));
    },
  };

  // ── dead letters ────────────────────────────────────────────────────────────────────

  views.dead = {
    title: () => 'Dead letters',
    kinds: ['deadletter', 'pipeline'],
    async load(ctx) {
      const pipelines = await all('pipelines');
      const withLetters = pipelines.items.filter(p => p.deadLetters > 0 || p.errorPolicy === 'DeadLetter');
      const id = ctx.args[0] || (withLetters.find(p => p.deadLetters > 0) || {}).id;
      const before = ctx.ui.before;
      const letters = id ? await api(`pipelines/${enc(id)}/dead-letters?limit=25${before ? '&before=' + before : ''}`) : null;
      return { pipelines: withLetters, id, letters };
    },
    render(ctx) {
      const { pipelines, id, letters } = ctx.data;
      return h('div', { class: 'stack' },
        pageHead({ title: 'Dead letters', lede: 'Records a pipeline dropped instead of faulting on them: the record, the error and where it came from. Kept per node, in memory, newest first.' }),
        pipelines.length === 0
          ? h('section', { class: 'card' }, empty('No dead letters', 'No pipeline has dropped a record, and none is deployed with the DeadLetter error policy.'))
          : h('div', { class: 'split' },
            h('div', null, letters ? lettersTab({ ...ctx, data: { letters, detail: { pipeline: pipelines.find(p => p.id === id) || {} } } }, id) : h('section', { class: 'card' }, empty('Choose a pipeline', 'Its dropped records appear here.'))),
            card('Pipelines', 'with dropped records, or able to drop them',
              h('table', { class: 'data' },
                h('thead', null, h('tr', null, h('th', null, 'Pipeline'), h('th', { class: 'r' }, 'Dropped'))),
                h('tbody', null, pipelines.map(p => h('tr', { key: p.id, 'aria-current': p.id === id ? 'true' : null },
                  h('td', { class: 'id' }, h('a', { href: '#/dead-letters/' + enc(p.id), 'aria-current': p.id === id ? 'page' : null }, p.id), p.id === id && h('span', { class: 'sub' }, 'shown')),
                  h('td', { class: 'r' }, fmtNum(p.deadLetters)))))))));
    },
  };

  // ── cluster ─────────────────────────────────────────────────────────────────────────

  views.cluster = {
    title: () => 'Cluster',
    kinds: ['node', 'catalog', 'table'],
    every: 2000,
    async load() {
      const [node, tables] = await Promise.all([api('node'), all('tables')]);
      store.leader = node.leader;
      return { node, tables };
    },
    render(ctx) {
      const { node, tables } = ctx.data;
      const st = node.status, lead = st.leadership, role = lead.role, kafka = lead.kafka, peer = st.peer, standby = st.standby;
      const acting = store.info && !store.info.readOnly;
      const take = st.lastTakeover;
      const stages = take ? [['Catalog', take.catalog], ['Tables', take.tables], ['Pipelines', take.pipelines], ['Everything else', Math.max(0, take.startLeading - take.pipelines)]] : [];
      const stageTotal = stages.reduce((a, [, v]) => a + v, 0) || 1;

      const selfCard = h('section', { class: 'card nodecard self', 'aria-label': 'This node' },
        h('div', { class: 'who' }, roleBadge(role, true), h('span', { class: 'node-name' }, st.nodeId), h('span', { class: 'tag' }, 'this node')),
        kv([
          ['Term', role === 'Standby' ? `${fmtInt(lead.leaderTerm)} (the leader’s)` : fmtInt(lead.term)],
          ['Health', h('span', null, healthBadge(st.health.level), st.health.reasons.length > 0 && h('ul', { class: 'muted' }, st.health.reasons.map(r => h('li', null, reasonText(r)))))],
          ['Ready', st.health.ready ? 'yes' : 'no'],
          ['Admin address', st.advertisedAdminUrl ? h('a', { href: st.advertisedAdminUrl }, st.advertisedAdminUrl) : h('span', { class: 'muted' }, 'not advertised')],
          kafka && ['Heartbeat', role === 'Leader' ? 'sending' : `${fmtMs(kafka.heartbeatAge)} since the leader’s last`],
          kafka && ['Lease', kafka.holdsLease ? 'held' : 'not held'],
          kafka && kafka.fault && ['Fence cause', h('span', { class: 'err' }, kafka.fault)],
        ]),
        acting && role === 'Leader' && h('div', { class: 'actions' }, h('button', { class: 'btn danger', type: 'button', on: { click: () => A.nodeAction('stepdown', st.nodeId, ctx.reload) } }, 'Step down…')),
        acting && role === 'Standby' && h('div', { class: 'actions' }, h('button', { class: 'btn danger', type: 'button', on: { click: () => A.nodeAction('takeover', st.nodeId, ctx.reload) } }, 'Take over…')));

      const leaderElsewhere = node.leader && !node.leader.isSelf;
      const other = peer ? h('section', { class: 'card nodecard', 'aria-label': 'The other node' },
          h('div', { class: 'who' }, roleBadge(peer.role, true), h('span', { class: 'node-name' }, peer.nodeId), h('span', { class: 'tag' }, 'peer, as it last answered')),
          kv([
            ['Term', fmtInt(peer.term)],
            ['Ready', peer.role === 'Standby' ? (peer.ready ? h('span', { class: 'badge good' }, icon('check', 12), 'ready to take over') : h('span', { class: 'badge warn' }, icon('alert', 12), 'not ready')) : peer.ready ? 'yes' : 'no'],
            peer.notReadyReason && ['Because', reasonText(peer.notReadyReason)],
            leaderElsewhere && node.leader.adminUrl && ['Admin address', h('a', { href: node.leader.adminUrl }, node.leader.adminUrl)],
          ]))
        : h('section', { class: 'card nodecard', 'aria-label': 'The other node' },
          h('div', { class: 'who' }, h('span', { class: 'role unknown big' }, icon('circle', 16), node.peerExpected ? 'No answer' : 'No peer'), h('span', { class: 'tag' }, 'peer')),
          h('p', { class: 'muted' }, node.peerExpected
            ? 'A second node is configured and does not answer. If this node fails, nothing takes over.'
            : 'This node was not told of another. A lone node is a valid deployment: there is just nothing to fail over to.'),
          leaderElsewhere && kv([['Leader on record', node.leader.adminUrl ? h('a', { href: node.leader.adminUrl }, node.leader.nodeId) : node.leader.nodeId]]));

      const maxLag = Math.max(1, ...(node.stateLag || [0]));
      return h('div', { class: 'stack' },
        pageHead({ title: 'Cluster', lede: 'Two nodes exist for availability, not for scale: one leads and runs every pipeline, the other follows and is ready to.' }),
        h('div', { class: 'nodes' }, selfCard, other),

        standby && card('Standby readiness', standby.takeoverWouldBeWarm ? 'a takeover now would be warm' : 'a takeover now would not be warm',
          h('div', { class: 'stack' },
            standby.takeoverWouldBeWarm ? notice('info', 'This node has applied the catalog, follows every table and has all of the state: it can lead in the time a takeover takes.')
              : notice('warn', h('strong', null, 'Not warm yet: '), h('ul', null, standby.notWarmBecause.map(r => h('li', null, reasonText(r))))),
            kv([
              ['Catalog', `version ${fmtInt(standby.catalogVersionApplied)} applied of ${fmtInt(standby.catalogVersionLatest)}`],
              ['State', `${plural(standby.stateLagRecords, 'record')} behind the state log`],
              ['Tables', `${fmtInt(standby.tablesBehind)} opening, ${fmtInt(standby.tablesFaulted)} faulted`],
              ['Plugins', `${fmtInt(standby.pluginsPending)} versions still to fetch`],
            ])), { flush: false }),

        h('div', { class: 'grid two' },
          card('State lag by partition', role === 'Standby' ? 'records this node has yet to read' : 'measured on a standby',
            !node.stateLag ? empty(role === 'Leader' ? 'The leader writes the state log' : 'Not measured', role === 'Leader' ? 'Lag is a follower’s number: open the standby to see it.' : 'The backend did not report it.')
              : h('table', { class: 'data' }, h('thead', null, h('tr', null, h('th', null, 'Partition'), h('th', { class: 'r' }, 'Behind'), h('th', null, h('span', { class: 'sr' }, 'Share')))),
                h('tbody', null, node.stateLag.map((lag, i) => { const m = h('span'); m.style.width = Math.round(100 * lag / maxLag) + '%'; return h('tr', { key: 'p' + i }, h('td', null, i), h('td', { class: 'r' }, fmtInt(lag)), h('td', null, h('div', { class: 'meter' }, m))); })))),
          card('Table lag', role === 'Standby' ? 'versions this node has yet to apply' : 'measured on a standby',
            tables.items.length === 0 ? empty('No tables', 'Nothing to follow.')
              : h('table', { class: 'data' }, h('thead', null, h('tr', null, h('th', null, 'Table'), h('th', null, 'State'), h('th', { class: 'r' }, 'Version'), h('th', { class: 'r' }, 'Behind'))),
                h('tbody', null, tables.items.map(t => h('tr', { key: t.name }, h('td', { class: 'id' }, tableLink(t.name)), h('td', null, tableBadge(t.state)), h('td', { class: 'r' }, fmtInt(t.version)),
                  h('td', { class: 'r' }, t.lagVersions == null ? h('span', { class: 'muted', title: 'The writer is never behind its own log' }, t.following ? '–' : 'writer') : fmtInt(t.lagVersions)))))))),

        h('div', { class: 'grid two' },
          card('Last takeover', take ? `term ${take.term}, ${ago(take.at)}` : null,
            take ? h('div', { class: 'stack' },
              h('div', { class: 'stages', role: 'img', 'aria-label': stages.map(([k, v]) => `${k} ${fmtMs(v)}`).join(', ') },
                stages.map(([k, v], i) => { const el = h('span', { class: 'stage-' + (i + 1), title: `${k}: ${fmtMs(v)}` }); el.style.flex = Math.max(v, stageTotal / 200); return el; })),
              h('table', { class: 'data' }, h('tbody', null,
                stages.map(([k, v], i) => h('tr', { key: k }, h('td', null, h('span', { class: 'swatch stage-' + (i + 1) }), ' ', k, i === 1 && h('span', { class: 'muted' }, ` · ${take.tableCount}`), i === 2 && h('span', { class: 'muted' }, ` · ${take.pipelineCount}`)), h('td', { class: 'r' }, fmtMs(v)))),
                kafka && kafka.lastTakeover && h('tr', null, h('td', null, 'Whole takeover, as the leadership timed it'), h('td', { class: 'r' }, fmtMs(kafka.lastTakeover.total ?? null))))))
              : empty('This node has not led', 'Timings of its takeover appear here once it has.'), { flush: !take }),
          card('Backend', st.backend.kind, kv([
            ...Object.entries(st.backend.settings).map(([k, v]) => [k, h('span', { class: 'mono' }, v || '–')]),
            ...Object.entries(st.backend.counters).map(([k, v]) => [k, fmtInt(v)]),
            ['Contracts', Object.entries(node.contracts).map(([k, v]) => h('div', { key: k, class: 'mono' }, `${k}@${short(v, 12)}`))],
            ['Event streams', `${fmtInt(node.events.clients)} open · ${fmtInt(node.events.dropped)} frames dropped for slow readers`],
          ]), { flush: false })));
    },
  };

  // ── audit ───────────────────────────────────────────────────────────────────────────

  views.audit = {
    title: () => 'Audit',
    kinds: ['catalog', 'pipeline', 'table', 'plugin', 'node'],
    every: 5000,
    load: async ctx => ({ audit: await api(`audit?limit=50${ctx.ui.before ? '&before=' + ctx.ui.before : ''}`) }),
    render(ctx) {
      const { audit } = ctx.data;
      return h('div', { class: 'stack' },
        pageHead({ title: 'Audit', lede: 'Every change made to this service, kept in the catalog: it survives a failover. Newest first.' }),
        h('section', { class: 'card' },
          audit.items.length === 0 ? empty('No entries', 'Deploys, resets, removals and role changes are recorded here with who asked for them.')
            : h('div', { class: 'table-wrap' }, h('table', { class: 'data cards' },
              h('thead', null, h('tr', null, h('th', { class: 'r' }, '#'), h('th', null, 'When'), h('th', null, 'Who'), h('th', null, 'Action'), h('th', null, 'Target'), h('th', null, 'Generation'), h('th', null, 'Node · term'), h('th', null, 'What it did'))),
              h('tbody', null, audit.items.slice().reverse().map(a => h('tr', { key: 'a' + a.sequence },
                td('#', 'r', a.sequence),
                td('When', 'nowrap', timeEl(a.time, stamp(a.time))),
                td('Who', null, a.caller),
                td('Action', null, h('span', { class: 'tag' }, a.action)),
                td('Target', 'id wrap', a.action.startsWith('pipeline.') && a.action !== 'pipeline.remove' ? pipelineLink(a.target) : a.target),
                td('Generation', 'nowrap', a.generationBefore == null && a.generationAfter == null ? '' : `${a.generationBefore ?? '–'} → ${a.generationAfter ?? '–'}`),
                td('Node · term', 'nowrap', `${a.node} · ${a.term}`),
                td('What it did', 'wrap', a.plan && a.plan.length ? h('details', { class: 'more' }, h('summary', null, plural(a.plan.length, 'step')), A.planList({ steps: a.plan })) : '')))))),
          (audit.hasOlder || ctx.ui.before) && h('div', { class: 'pager' },
            h('span', { class: 'info' }, `${fmtInt(audit.total)} entries kept`),
            ctx.ui.before && h('button', { class: 'btn small', type: 'button', on: { click: () => { ctx.ui.before = null; ctx.reload(); } } }, 'Newest'),
            audit.hasOlder && h('button', { class: 'btn small', type: 'button', on: { click: () => { ctx.ui.before = audit.oldest; ctx.reload(); } } }, 'Older'))));
    },
  };
})();
