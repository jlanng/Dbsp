/* The views. Each is { title, load, render }: load fetches what the page shows, render
   builds it from that and from the live store (the feed's ticks). */
(() => {
  'use strict';
  const A = window.__admin;
  const { h, s, icon, api, all, store, fmtNum, fmtInt, fmtMs, fmtBytes, ago, clock, stamp, timeEl, plural, short, pretty,
    pipelineBadge, lastKnown, peerHref, clamped, tableBadge, roleBadge, healthBadge, reasonText, spark, lineChart, chartWidth, card, empty, notice, errorNotice,
    tile, kv, td, toast, canAct, pageHead, seriesOf, ratesOf, seedSeries, STATE_ORDER, PIPELINE_STATES } = A;

  const views = A.views = {};
  const enc = encodeURIComponent;
  const pipelineLink = id => h('a', { class: 'id', href: '#/pipelines/' + enc(id) }, id);
  const tableLink = name => h('a', { class: 'id', href: '#/tables/' + enc(name) }, name);
  const STATE_FILL = { good: 'fill-good', warn: 'fill-warn', serious: 'fill-serious', crit: 'fill-crit', idle: 'fill-idle', info: 'fill-info' };
  const last = arr => (arr.length ? arr[arr.length - 1] : null);

  // A link to the leader's admin UI, where this node knows of a leader that is not itself
  // and of its address; nothing otherwise, so the sentence around it still reads.
  const leaderLink = (before = '', after = '') => {
    const leader = store.leader, node = store.node;
    return leader && leader.adminUrl && node && leader.nodeId !== node.nodeId ? [before, h('a', { href: peerHref(leader.adminUrl) }, leader.nodeId), after] : null;
  };
  const rate = v => (v == null ? '–' : fmtNum(v));

  const eventList = events => h('ul', { class: 'events', role: 'log', 'aria-label': 'Recent events' },
    events.slice().reverse().map(e => h('li', { key: 'e' + e.sequence },
      h('span', { class: 'when' }, timeEl(e.time, clock(e.time))),
      h('span', { class: 'kind' }, e.kind),
      h('span', { class: 'msg' },
        e.kind === 'pipeline' || e.kind === 'deadletter' ? h('a', { class: 'subject', href: '#/pipelines/' + enc(e.subject) }, e.subject)
          : e.kind === 'table' ? h('a', { class: 'subject', href: '#/tables/' + enc(e.subject) }, e.subject)
          : h('span', { class: 'subject' }, e.subject),
        clamped(e.message)))));

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
        // On a node that no longer leads these are history, not something to act on here.
        if (!lastKnown() && ['Failed', 'Wedged', 'NeedsReset', 'Blocked', 'Backoff'].includes(p.state))
          attention.push(h('li', { key: 'p' + p.id }, pipelineBadge(p.state),
            h('div', { class: 'text' }, pipelineLink(p.id),
              h('span', { class: 'sub' }, p.blockedOn ? blockedText(p.blockedOn) : p.lastFault ? `${p.lastFault.exceptionType}${p.lastFault.message ? ': ' + p.lastFault.message : ''}` : PIPELINE_STATES[p.state][2]))));
      }
      for (const p of pipelines.items) {
        if (p.stateGrowingSince)
          attention.push(h('li', { key: 'g' + p.id }, h('span', { class: 'badge warn' }, 'Growing'),
            h('div', { class: 'text' }, pipelineLink(p.id), h('span', { class: 'sub' },
              `its state has grown without a pause since ${ago(p.stateGrowingSince)}, to ${plural(p.stateSize ? p.stateSize.entries : 0, 'entry', 'entries')}: something kept per record is never closed`))));
      }
      for (const p of pipelines.items) {
        if (!lastKnown() && p.waiting)
          for (const w of p.waiting)
            attention.push(h('li', { key: 'w' + p.id + '/' + w.slot }, h('span', { class: 'badge info' }, 'Waiting'),
              h('div', { class: 'text' }, pipelineLink(p.id), h('span', { class: 'sub' }, `input ${w.slot} is waiting for ${w.waitingFor}${w.since ? `; it began ${ago(w.since)}` : ''}`))));
      }
      for (const t of tables.items) {
        if (t.state !== 'Open' && t.state !== 'Opening')
          attention.push(h('li', { key: 't' + t.name }, tableBadge(t.state), h('div', { class: 'text' }, tableLink(t.name), h('span', { class: 'sub' }, t.fault || (t.loader && t.loader.fault) || 'See the table.'))));
        else if (t.loader && t.loader.waitingFor)
          attention.push(h('li', { key: 't' + t.name }, h('span', { class: 'badge info' }, 'Waiting'), h('div', { class: 'text' }, tableLink(t.name),
            h('span', { class: 'sub' }, `its loader is waiting for ${t.loader.waitingFor}${t.loader.waitingSince ? `, which began ${ago(t.loader.waitingSince)}` : ''}; the table holds ${plural(t.rows, 'row')}`))));
      }
      for (const reason of st.health.reasons.filter(r => !r.startsWith('pipeline-') && !['loader-faulted', 'table-faulted', 'every-pipeline-failed', 'loader-waiting', 'source-waiting'].includes(r)))
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
            card('Pipelines', lastKnown() ? `${plural(count, 'pipeline')}, as this node last saw them` : plural(count, 'pipeline'),
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
                  h('thead', null, h('tr', null, h('th', null, 'Table'), h('th', null, 'State'), h('th', { class: 'r' }, 'Rows'), h('th', { class: 'r', title: 'Managed heap on this node, estimated: rows, indexes and retained history' }, 'Memory'), h('th', { class: 'r' }, 'Version'), h('th', { class: 'r' }, standby ? 'Behind' : 'Readers'))),
                  h('tbody', null, tables.items.map(t => h('tr', { key: t.name },
                    td('Table', 'id', tableLink(t.name)),
                    td('State', null, tableBadge(t.state)),
                    td('Rows', 'r', fmtNum(t.rows)),
                    td('Version', 'r', fmtInt(t.version)),
                    td(standby ? 'Behind' : 'Readers', 'r', standby ? (t.lagVersions == null ? '–' : plural(t.lagVersions, 'version')) : fmtInt(t.readers))))))),
              { extra: h('a', { href: '#/tables' }, 'All tables') })),

          h('div', { class: 'stack' },
            card('Needs attention', attention.length ? String(attention.length) : null,
              attention.length ? h('ul', { class: 'attn' }, attention)
                : standby ? empty('Nothing on this node needs attention', ['A standby runs no pipelines and does not see how they are doing: whether one has failed is shown on the leader',
                  leaderLink(' (', ')'), '. Here: the tables this node follows are open and it is ready to take over.'])
                : role === 'Leader' ? empty('Nothing needs attention', 'Every pipeline and table is where it was asked to be.')
                : empty('This node runs nothing', ['It no longer leads; what needs attention is shown on the leader', leaderLink(' (', ')'), '.'])),
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
    'state-size': p => (p.stateSize ? p.stateSize.bytes : -1),
  };

  function sortHeader(ctx, key, label, cls) {
    const asked = ctx.query.get('sort');
    const current = asked && Object.hasOwn(SORTS, asked) ? asked : 'state', dir = ctx.query.get('dir') || 'asc';
    const active = current === key;
    return h('th', { class: cls, 'aria-sort': active ? (dir === 'asc' ? 'ascending' : 'descending') : null },
      h('button', { class: 'sort', type: 'button', on: { click: () => ctx.setQuery({ ...Object.fromEntries(ctx.query), sort: key, dir: active && dir === 'asc' ? 'desc' : 'asc' }) } },
        label, h('span', { class: 'arrow', 'aria-hidden': 'true' }, active ? (dir === 'asc' ? '↑' : '↓') : '')));
  }

  views.pipelines = {
    title: () => 'Pipelines',
    kinds: ['pipeline', 'deadletter', 'node', 'catalog'],
    async load() {
      return { pipelines: await all('pipelines') };
    },
    render(ctx) {
      const { pipelines } = ctx.data;
      const q = ctx.query.get('q') || '';
      const chosen = new Set((ctx.query.get('state') || '').split(',').filter(Boolean));
      // A sort key comes from the address bar: one that names no column sorts by state.
      const asked = ctx.query.get('sort');
      const sort = asked && Object.hasOwn(SORTS, asked) ? asked : 'state', dir = ctx.query.get('dir') === 'desc' ? -1 : 1;
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
          actions: canAct() && h('button', { class: 'btn primary', type: 'button', on: { click: () => A.deployDialog({ after: ctx.reload }) } }, icon('plus', 14), 'Deploy'),
        }),
        lastKnown() && notice('warn', h('strong', null, 'These states are the last this node saw. '),
          'It no longer leads and runs no pipeline; what each is doing now is on the leader', leaderLink(' (', ')'), '.'),
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
                sortHeader(ctx, 'step', 'Step', 'r'), sortHeader(ctx, 'lag', 'Lag', 'r'), sortHeader(ctx, 'state-size', 'State size', 'r'), sortHeader(ctx, 'dead', 'Dead letters', 'r'), sortHeader(ctx, 'since', 'In state', 'r'))),
              h('tbody', null, shown.map(p => {
                const r = ratesOf(p.id) || p.rates || {};
                return h('tr', { key: p.id },
                  td('Pipeline', 'id', pipelineLink(p.id)),
                  td('State', null, pipelineBadge(p.state), p.blockedOn && h('span', { class: 'sub' }, blockedText(p.blockedOn)), p.pending && h('span', { class: 'sub' }, `unfinished ${p.pending}`)),
                  td('Definition', null, p.definition, h('span', { class: 'sub mono' }, `${p.plugin}@${short(p.version)}`)),
                  td('In /s', 'r', rate(r.in)),
                  td('Last minute', 'spark', spark(seriesOf(p.id, 'in'), { label: `${p.id}: records in per second`, unit: '/s' })),
                  td('Out /s', 'r', rate(r.out)),
                  td('Step', 'r', r.batchedEveryS ? h('span', { title: `It has stepped about every ${r.batchedEveryS} s lately — a sink’s minimum interval, or an input that is quiet; rates are over the last ${r.rateOverS} s` }, `every ${r.batchedEveryS} s`)
                    : r.stepMs == null ? '–' : fmtMs(r.stepMs)),
                  td('Lag', 'r', r.lag == null ? '–' : fmtNum(r.lag)),
                  td('State size', 'r', !p.stateSize || !p.stateSize.entries ? h('span', { class: 'muted' }, 'none')
                    : [fmtNum(p.stateSize.entries), h('span', { class: 'sub' }, '≈ ' + fmtBytes(p.stateSize.bytes)),
                      p.stateGrowingSince && h('span', { class: 'badge warn', title: `Grown without a pause since ${p.stateGrowingSince}` }, 'growing')]),
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
            `${p.definition} from ${p.plugin}@${short(p.version)} · `,
            h('span', { title: 'The generation of its spec: it goes up with every change to the pipeline’s catalog entry, and is what the audit and the faults name. A restart builds the same generation again.' }, `generation ${p.generation}`),
            p.operation && ` · ${p.operation} in progress`,
            p.blockedOn && ` · ${blockedText(p.blockedOn)}`,
            p.pending && ` · unfinished ${p.pending}`,
          ],
          actions: acting && [
            stopped ? button('start', 'Start', 'play') : button('stop', 'Stop', 'stop'),
            button('restart', 'Restart', 'restart'),
            h('button', { class: 'btn', type: 'button', on: { click: () => A.deployDialog({ existing: p, spec: detail.spec, after: ctx.reload }) } }, 'Edit spec…'),
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
      !running && p.state !== 'Stopped' && p.state !== 'Unbuilt' && p.state !== 'Built' && notice(p.state === 'Failed' || p.state === 'Wedged' || p.state === 'NeedsReset' ? 'error' : 'warn',
        h('strong', null, PIPELINE_STATES[p.state] ? PIPELINE_STATES[p.state][2] + '. ' : ''),
        fault ? `${fault.exceptionType}${fault.message ? ': ' + fault.message : ''} ` : '',
        fault && h('a', { href: `#/pipelines/${enc(id)}/faults` }, 'See the faults')),
      p.rolledBack && notice('warn', h('strong', null, 'The last replace was rolled back. '),
        `Generation ${p.rolledBack.from} (${p.rolledBack.definition} from ${p.rolledBack.plugin}@${short(p.rolledBack.version)}) could not start on the state it was to keep, so the spec of generation ${p.rolledBack.restored} was put back as generation ${p.rolledBack.now}, `,
        timeEl(p.rolledBack.time), '.',
        p.rolledBack.problems && p.rolledBack.problems.length > 0 && h('ul', null, p.rolledBack.problems.map(x => h('li', null, x)))),
      p.replaceUnconfirmed && notice('info', 'A replace that kept state has not finished its first start yet: if it cannot read the state, the previous spec is put back.'),
      p.waiting && !lastKnown() && notice('warn', h('strong', null, 'Waiting for its input. '),
        p.waiting.map((w, i) => [i > 0 && ' ', `Input ${w.slot} has found nothing to read: it is waiting for ${w.waitingFor}`, w.since ? [' (it began ', timeEl(w.since), ')'] : '', '.']),
        ' A file or topic yet to be delivered looks the same as a name with a mistake in it: check the binding.'),
      p.state === 'Built' && notice('info', 'This node is a standby: it has built this pipeline’s circuit on the state it follows and starts it when it takes over. Rates are on the leader.'),
      p.state === 'Unbuilt' && notice('info', 'This node has not built this pipeline’s circuit: it follows its state as bytes and builds the circuit if it takes over. ' + ((detail.history || []).filter(t => t.state === 'Unbuilt' && t.why).map(t => t.why).pop() || 'A standby builds a pipeline once the leader has run it; one that writes a table is built at takeover.')),

      h('div', { class: 'tiles' },
        tile([h('span', { class: 'swatch in' }), 'Records in'], rate(r.in), '/s', r.batchedEveryS ? `a step every ${r.batchedEveryS} s · rate over the last ${r.rateOverS} s` : `${fmtInt(r.steps || 0)} steps in the last second`, spark(seriesOf(id, 'in'), { label: 'Records in per second', unit: '/s' })),
        tile([h('span', { class: 'swatch out' }), 'Records out'], rate(r.out), '/s', 'upserts, deletes and appends', spark(seriesOf(id, 'out'), { cls: 'out', label: 'Records out per second', unit: '/s' })),
        tile('Step latency', r.stepMs == null ? '–' : fmtMs(r.stepMs), '', r.batchedEveryS ? `a step every ${r.batchedEveryS} s; a sink’s minimum interval is part of its step` : r.stepMaxMs == null ? 'no steps in the last second' : `slowest ${fmtMs(r.stepMaxMs)}`, spark(seriesOf(id, 'stepMs'), { cls: 'neutral', label: 'Mean step latency', unit: ' ms' })),
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

      stateCard(detail),

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
    return card('Faults', `the last ${detail.faults.length} on this node, newest first · a restart is a new run of the same generation`,
      h('div', null,
        hidden && h('div', { class: 'record' }, notice('info', 'Messages and stacks are hidden: this admin API does not expose row data, and an exception message can quote a row.')),
        detail.faults.map((f, i) => h('div', { class: 'record', key: f.time + i },
          h('div', { class: 'head' }, h('strong', { class: 'mono' }, f.exceptionType), h('span', { class: 'when' }, timeEl(f.time, `${stamp(f.time)} · ${ago(f.time)}`)), h('span', { class: 'tag', title: 'The generation of the spec that faulted, and which of the circuits this node has built for the pipeline it was' }, `generation ${f.generation}${f.run ? ' · run ' + f.run : ''}`)),
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

  /** Where a pipeline's dropped records go to stay, and how that is going. */
  function deadLetterSink(p) {
    const sink = p.deadLetterSink;
    if (!sink) {
      if (p.errorPolicy !== 'DeadLetter') return null;
      return notice(p.deadLettersBound ? 'info' : 'warn',
        p.deadLettersBound ? ['A durable sink is bound; its counts appear while the pipeline runs on this node.']
          : [h('strong', null, 'No durable sink. '), 'What this pipeline drops is kept only here, in memory, the newest few: a failover or a restart of the leader loses it. Bind one under ',
            h('code', null, 'deadLetters'), ' in its spec (Edit spec, Running it).']);
    }
    const failing = sink.failedWrites > 0 && sink.lastError;
    return h('div', { class: 'sinkstate' },
      h('div', { class: 'head' },
        h('span', { class: ['badge', failing ? 'crit' : sink.writeInFlightSince ? 'info' : 'good'] }, icon(failing ? 'alert' : sink.writeInFlightSince ? 'dots' : 'check', 12),
          failing ? 'Write failing' : sink.writeInFlightSince ? 'Writing' : 'Stored'),
        h('strong', null, 'Durable sink'), h('span', { class: 'mono' }, sink.binding || sink.connector)),
      kv([
        ['Stored', `${plural(sink.delivered, 'record')} in ${plural(sink.batches, 'batch', 'batches')} since this generation started`],
        ['Write', sink.writeInFlightSince ? ['in flight since ', timeEl(sink.writeInFlightSince), ' — the checkpoint waits for it'] : 'none in flight'],
        ['Failed writes', sink.failedWrites ? [fmtInt(sink.failedWrites), sink.lastErrorTime && [', the last ', timeEl(sink.lastErrorTime)]] : 'none'],
        sink.lastError && ['Last error', h('span', { class: 'err' }, sink.lastError)],
      ]),
      h('p', { class: 'help' }, 'Each dropped record is written here before the checkpoint that passes it, with its pipeline, generation, step and source position. A sink that keeps failing stops the pipeline: nothing is dropped unrecorded.'));
  }

  function lettersTab(ctx, id) {
    const { letters, detail } = ctx.data;
    if (!letters) return card('Dead letters', null, A.skeleton());
    const hidden = !letters.rowDataExposed;
    const policy = detail && detail.pipeline.errorPolicy;
    const elsewhere = !ctx.pageSaysSo && store.node && store.node.role !== 'Leader';
    const durable = detail && detail.pipeline.id ? deadLetterSink(detail.pipeline) : null;
    return card('Dead letters', `${fmtInt(letters.total)} dropped; the newest ${fmtInt(letters.kept)} are kept on this node to look at`,
      h('div', null,
        durable && h('div', { class: 'record' }, durable),
        elsewhere && h('div', { class: 'record' }, notice('info', ['This node does not run the pipeline, so it has no record of what the pipeline dropped: the count and the records are kept by the leader',
          leaderLink(' (', ')'), ', in memory.'])),
        hidden && h('div', { class: 'record' }, notice('info', 'Records and messages are hidden: this admin API does not expose row data. The operator, the time and the exception type are shown.')),
        letters.items.length === 0
          ? empty(letters.total ? 'None kept on this node' : 'No dead letters',
            letters.total ? 'What is shown here is per node and in memory: a new leader starts it empty. A durable sink, where one is bound, has them all.'
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
  // What an operator kind does, for the ones where the name does not say: the choice
  // between Join and Lookup is the one a pipeline's author makes per table.
  const KIND_HELP = {
    tableJoin: 'Join against a table (retroactive): it holds its records per key and consumes the table’s changes, so a row that changes later revises what was already emitted.',
    tableIndexJoin: 'Join against a table through an index (retroactive): a row that changes later revises what was already emitted.',
    tableLookup: 'Lookup (enrich on arrival): each record is enriched from the table’s row as of the step it arrives in; a later change to the row does not revise it. It remembers what it emitted, so a retraction cancels exactly that.',
    tableLookupAppendOnly: 'Append-only lookup (enrich on arrival): one probe per record and no state. Its input may only insert.',
    tableRows: 'The table itself as a stream: every change as the old row retracted and the new row asserted.',
    join: 'Join of two streams on a key (retroactive): it holds both sides, and either side changing revises the output.',
    antiJoin: 'The records of the first stream whose key has no match in the second.',
    exchange: 'Repartitions records across workers by key.',
    eventTime: 'Declares the event-time field and the lateness; advances the watermark and drops records older than it.',
    window: 'Assigns records to event-time windows.',
    aggregate: 'One result per key, kept up to date as records arrive and are retracted.',
    aggregateLinear: 'One result per key from a linear aggregate: updated from the delta alone.',
    upsert: 'A keyed input: the latest value per key, with the previous one retracted.',
  };
  const ROLE_TAG = { input: 'input', table: 'table', sink: 'sink', 'table-out': 'writes table', circuit: 'circuit' };
  const clip = (text, max) => (text.length > max ? text.slice(0, max - 1) + '…' : text);
  /** The second line of a node: what kind of edge it is and what it is bound to. */
  const subText = n => clip(n.role === 'operator' || n.role === 'circuit' ? (n.detail || '') : [ROLE_TAG[n.role].toLowerCase(), n.role === 'table' || n.role === 'table-out' ? null : n.detail].filter(Boolean).join(' · '), 28);

  const BAND_GAP = 40, GUTTER = 9;

  /**
   * A layered drawing, left to right — and, where the layers do not fit in `avail`, in
   * bands one below the other, read like lines of text: a circuit of twenty operators is
   * then as legible as one of five instead of being shrunk to fit.
   */
  function layout(nodes, avail) {
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
    const colX = [], colW = [];
    cols.forEach(col => {
      const w = Math.max(...col.map(id => width(byId.get(id))), 30);
      colX.push(x); colW.push(w);
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
    // Cut the layers into bands that fit, and stack the bands.
    const bands = [];
    let band = { cols: [], width: 0 };
    cols.forEach((col, i) => {
      const add = (band.cols.length ? GAP_X : 0) + colW[i];
      if (band.cols.length && PAD * 2 + GUTTER + band.width + add > avail) { bands.push(band); band = { cols: [], width: 0 }; }
      band.width += (band.cols.length ? GAP_X : 0) + colW[i];
      band.cols.push(i);
    });
    bands.push(band);
    if (bands.length === 1) return { pos, width: x - GAP_X + PAD, height, bandOf: () => 0, bands: [{ top: 0, bottom: height }] };

    const bandOf = new Map();
    let top = PAD;
    for (const [b, each] of bands.entries()) {
      const ids = each.cols.flatMap(i => cols[i]);
      const minY = Math.min(...ids.map(id => pos.get(id).y)), maxY = Math.max(...ids.map(id => pos.get(id).y + NODE_H));
      const shiftX = PAD + GUTTER - colX[each.cols[0]], shiftY = top - minY;
      for (const id of ids) { const p = pos.get(id); p.x += shiftX; p.y += shiftY; bandOf.set(id, b); }
      each.top = top; each.bottom = top + (maxY - minY);
      top = each.bottom + BAND_GAP;
    }
    return { pos, width: Math.max(...bands.map(each => each.width)) + PAD * 2 + GUTTER, height: top - BAND_GAP + PAD, bandOf: id => bandOf.get(id), bands };
  }

  /** An edge that leaves one band for another: down into the gap below its own band, along it, and in from the left. */
  function crossBand(a, b, from, to, bands, entry) {
    const x1 = a.x + a.w, y1 = a.y + a.h / 2, x2 = entry ? entry.x : b.x, y2 = entry ? entry.y : b.y + b.h / 2;
    const below = bands[from].bottom + BAND_GAP / 2, above = bands[to].top - BAND_GAP / 2;
    const into = entry ? `V${y2}` : `H${x2 - 12} V${y2} H${x2}`;
    const along = entry ? x2 : x2 - 12;
    if (to === from + 1) return `M${x1},${y1} h10 V${below} H${along} ${entry ? into : `V${y2} H${x2}`}`;
    return `M${x1},${y1} h10 V${below} H${GUTTER} V${above} H${along} ${entry ? into : `V${y2} H${x2}`}`;
  }

  function topologyTab(ctx) {
    const { graph, detail } = ctx.data;
    if (!graph) return card('Topology', null, A.skeleton());
    const ui = ctx.ui;
    // A lookup probes its table rather than consuming its changes: an edge of the
    // picture all the same, so the layout places the table beside what reads it.
    // It opens fitted to the width there is, at a size that can be read: layers that do
    // not fit side by side continue in a band below.
    const avail = Math.max(280, A.$('main').clientWidth - 50);
    const { pos, width, height, bandOf, bands } = layout(graph.nodes.map(n => ({ ...n, inputs: [...n.inputs, ...(n.probes || [])] })), avail);
    const byId = new Map(graph.nodes.map(n => [n.id, n]));
    const fit = Math.max(0.5, Math.min(1, avail / width));
    const zoom = ui.zoom || fit;
    const selected = ui.node && byId.get(ui.node);
    const dead = graph.deadLetters || {};
    const nameOf = i => (byId.get(i) ? byId.get(i).label : i);
    const select = id => { ui.node = ui.node === id ? null : id; ctx.render(); };

    const edges = [];
    for (const n of graph.nodes) {
      for (const from of n.inputs) {
        const a = pos.get(from), b = pos.get(n.id);
        if (!a || !b) continue;
        const x1 = a.x + a.w, y1 = a.y + a.h / 2, x2 = b.x, y2 = b.y + b.h / 2, bend = Math.max(18, (x2 - x1) / 2);
        const within = bandOf(from) === bandOf(n.id);
        edges.push(s('path', { class: ['g-edge', !within && 'cross', selected && (from === selected.id || n.id === selected.id) && 'hl'],
          d: within ? `M${x1},${y1} C${x1 + bend},${y1} ${x2 - bend},${y2} ${x2},${y2}` : crossBand(a, b, bandOf(from), bandOf(n.id), bands), 'marker-end': 'url(#arrow)' }));
      }
      for (const from of n.probes || []) {
        const a = pos.get(from), b = pos.get(n.id);
        if (!a || !b) continue;
        // Into the top or the bottom of the lookup, whichever side the table is on: its
        // left side is where the records it enriches come in.
        const above = a.y < b.y;
        const x1 = a.x + a.w, y1 = a.y + a.h / 2, x2 = b.x + Math.min(22, b.w / 2), y2 = above ? b.y : b.y + b.h;
        const within = bandOf(from) === bandOf(n.id) && x2 > x1;
        edges.push(s('path', { class: ['g-edge', 'probe', !within && 'cross', selected && (from === selected.id || n.id === selected.id) && 'hl'],
          d: within ? `M${x1},${y1} C${x1 + Math.max(14, (x2 - x1) * 0.6)},${y1} ${x2},${y2 + (above ? -26 : 26)} ${x2},${y2}`
            : bandOf(from) < bandOf(n.id) ? crossBand(a, b, bandOf(from), bandOf(n.id), bands, { x: x2, y: b.y })
            : `M${x1},${y1} L${x2},${y2}`, 'marker-end': 'url(#arrow)' }));
      }
    }

    const drawn = graph.nodes.map(n => {
      const p = pos.get(n.id);
      const describe = `${ROLE_TAG[n.role] ? ROLE_TAG[n.role].toLowerCase() : n.role} ${n.label}${n.detail ? ', ' + n.detail : ''}${n.inputs.length ? '. Reads ' + n.inputs.map(nameOf).join(', ') : ''}${(n.probes || []).length ? '. Looks up rows in table ' + n.probes.map(nameOf).join(', ') : ''}`;
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
    const consumers = selected ? graph.nodes.filter(n => n.inputs.includes(selected.id)).map(n => n.label) : [];
    const lookers = selected ? graph.nodes.filter(n => (n.probes || []).includes(selected.id)).map(n => n.label) : [];
    const kind = selected ? (selected.kind || selected.role).split('[')[0] : '';
    const pid = enc(detail.pipeline.id);
    // The numbers that do exist: per input slot, per sink slot, per table.
    const source = selected && selected.role === 'input' ? detail.sources.find(x => x.slot === selected.label) : null;
    const sink = selected && selected.role === 'sink' ? detail.sinks.find(x => x.slot === selected.label) : null;
    const tableUse = selected && (selected.role === 'table' || selected.role === 'table-out') ? detail.tables.find(x => x.name === selected.label) : null;
    return h('div', { class: 'stack' },
      graph.source === 'declared' && notice('info', 'This node has not built this pipeline’s circuit, so this is the declared edges only: its input slots, tables and sinks around one box. The leader shows the operators.'),
      card('Topology', `${graph.nodes.length} nodes · ${graph.source === 'circuit' ? 'the circuit as its definition declares it' : 'declared edges'}`,
        h('div', null,
          h('div', { class: 'graph-wrap', tabindex: 0, 'aria-label': 'Topology drawing; scroll to pan' }, svg),
          h('div', { class: 'graph-side', 'aria-live': 'polite' },
            selected ? kv([
              ['Node', h('strong', null, selected.label)],
              ['Kind', h('span', null, h('span', { class: 'mono' }, selected.kind || selected.role), KIND_HELP[kind] && h('span', { class: 'kind-help' }, KIND_HELP[kind]))],
              selected.detail && (selected.role === 'input' || selected.role === 'sink') && ['Bound to', selected.detail],
              source && ['Read so far', `${plural(source.records, 'record')} · lag ${source.lag == null ? 'not reported' : fmtInt(source.lag)} · ${plural(source.unreadable, 'unreadable record')}`],
              source && source.at && ['At', h('span', { class: 'mono' }, source.at)],
              source && source.waitingFor && ['Waiting for', [source.waitingFor, source.waitingSince ? [' · began ', timeEl(source.waitingSince)] : '']],
              sink && ['Delivered', `${plural(sink.batches, 'batch', 'batches')}: ${fmtInt(sink.upserts)} upserts, ${fmtInt(sink.deletes)} deletes, ${fmtInt(sink.appends)} appends${sink.writeInFlightSince ? ' · a write is in flight' : ''}`],
              (selected.role === 'table' || selected.role === 'table-out') && ['Table', [tableLink(selected.label),
                tableUse && tableUse.committedVersion != null && ` · this pipeline has committed version ${fmtInt(tableUse.committedVersion)} of ${fmtInt(tableUse.currentVersion)}`]],
              selected.stateName && selected.role === 'operator' && ['Known as', h('span', null, h('span', { class: 'mono' }, selected.stateName), ' in its state stores, if it keeps any, and in dead letters', selected.name ? ' — named, so it survives changes to the circuit around it' : ' — positional: an upstream change renumbers it')],
              selected.errorPolicy && ['On a record error', selected.errorPolicy === 'DeadLetter' ? 'drop the record to the dead letters' : 'fault the pipeline'],
              selected.stateName && dead[selected.stateName] && ['Dead letters', h('a', { href: `#/pipelines/${pid}/dead-letters` }, `${dead[selected.stateName]} of those kept came from here`)],
              ['Reads', selected.inputs.length ? selected.inputs.map(nameOf).join(', ') : selected.role === 'table' ? 'the shared table, outside this circuit' : 'nothing: it is a source'],
              (selected.probes || []).length > 0 && ['Looks up', ['rows of ', selected.probes.map((i, k) => [k > 0 && ', ', tableLink(nameOf(i))]), ' at each step’s pinned version; the table’s changes do not flow into it']],
              consumers.length > 0 && ['Feeds', consumers.join(', ')],
              lookers.length > 0 && ['Looked up by', lookers.join(', ')],
              selected.role === 'operator' || selected.role === 'exchange' ? ['Live counts', h('span', { class: 'muted' }, 'not collected per operator')] : null,
            ]) : h('span', { class: 'muted' }, 'Select a node to see its kind, what it reads and feeds, its state stores, and — for an input, a sink or a table — its numbers. Tab moves between nodes.')),
          h('p', { class: 'graph-note' }, icon('info', 13),
            h('span', null, 'This is the circuit as declared, not a live view: the engine measures a step as a whole, so records and time per operator are not collected. The pipeline’s rates are on its ',
              h('a', { href: `#/pipelines/${pid}` }, 'Overview'), '; an input, a sink or a table shows its own numbers when selected.'))),
        { extra: [
          h('ul', { class: 'legend' },
            h('li', null, h('span', { class: 'swatch in' }), 'input'), h('li', null, h('span', { class: 'swatch tbl' }), 'table'),
            h('li', null, h('span', { class: 'swatch out' }), 'sink'), h('li', null, '⇄ exchange'),
            h('li', null, h('span', { class: 'edge-key' }), 'records flow'), h('li', null, h('span', { class: 'edge-key probe' }), 'rows looked up')),
          h('div', { class: 'seg', role: 'group', 'aria-label': 'Zoom' },
            h('button', { type: 'button', 'aria-label': 'Zoom out', on: { click: zoomTo(zoom - 0.15) } }, '−'),
            h('button', { type: 'button', 'aria-label': 'Fit to the view', 'aria-pressed': String(!ui.zoom), on: { click: () => { ui.zoom = null; ctx.render(); } } }, 'Fit'),
            h('button', { type: 'button', 'aria-label': 'Actual size', on: { click: zoomTo(1) } }, '100%'),
            h('button', { type: 'button', 'aria-label': 'Zoom in', on: { click: zoomTo(zoom + 0.15) } }, '+')),
        ] }));
  }

  // ── tables ──────────────────────────────────────────────────────────────────────────

  const writerText = w => (w === 'none' ? 'the host' : w.startsWith('pipeline:') ? pipelineLink(w.slice(9)) : w.startsWith('loader:') ? `${w.slice(7)} loader` : w);

  views.tables = {
    title: () => 'Tables',
    kinds: ['table', 'node', 'catalog'],
    every: 3000,
    load: async () => ({ tables: await all('tables') }),
    render({ data }) {
      const { tables } = data;
      const standby = store.node && store.node.role === 'Standby';
      return h('div', { class: 'stack' },
        pageHead({
          title: 'Tables', lede: 'Shared reference tables: held once per node, read by any number of pipelines.',
          actions: canAct() && h('button', { class: 'btn primary', type: 'button', on: { click: () => A.declareTableDialog({ after: r => { location.hash = '#/tables/' + enc(r.table.name); } }) } }, icon('plus', 14), 'Declare'),
        }),
        h('section', { class: 'card' },
          tables.items.length === 0 ? empty('No tables are declared', canAct() ? 'Use Declare to add the first one; a host can also seed them.' : 'Tables are declared on the leader, or seeded by the host.')
            : h('div', { class: 'table-wrap' }, h('table', { class: 'data cards' },
              h('thead', null, h('tr', null, h('th', null, 'Table'), h('th', null, 'State'), h('th', { class: 'r' }, 'Rows'), h('th', { class: 'r' }, 'Version'),
                h('th', { class: 'r' }, standby ? 'Behind' : 'Versions /s'), h('th', null, 'Written by'), h('th', null, 'Loader'), h('th', { class: 'r' }, 'Readers'), h('th', null, 'Floor held by'))),
              h('tbody', null, tables.items.map(t => h('tr', { key: t.name },
                td('Table', 'id', tableLink(t.name), h('span', { class: 'sub' }, `${t.keyType} → ${t.rowType}`)),
                td('State', null, tableBadge(t.state)),
                td('Rows', 'r', fmtNum(t.rows)),
                td('Memory', 'r', t.memory ? '≈ ' + fmtBytes(t.memory.totalBytes) : '–'),
                td('Version', 'r', fmtInt(t.version)),
                td(standby ? 'Behind' : 'Versions /s', 'r', standby ? (t.lagVersions == null ? '–' : fmtInt(t.lagVersions)) : fmtNum(t.versionsPerSecond)),
                td('Written by', null, t.writer ? writerText(t.writer) : '–'),
                td('Loader', null, t.loader ? [h('span', { class: 'tag' }, t.loader.kind), ' ', t.loader.state,
                  t.loader.waitingFor && h('span', { class: 'sub' }, `for ${t.loader.waitingFor}`), t.loader.unreadable > 0 && h('span', { class: 'sub' }, plural(t.loader.unreadable, 'unreadable record'))] : h('span', { class: 'muted' }, 'none')),
                td('Readers', 'r', fmtInt(t.readers)),
                td('Floor held by', null, t.floorHeldBy ? pipelineLink(t.floorHeldBy) : h('span', { class: 'muted' }, 'nobody')))))))));
    },
  };

  views.table = {
    title: args => args[0],
    kinds: ['table', 'pipeline'],
    // A loader's rows arrive without an event of their own (a declare's first load among
    // them): read again every few seconds, or the page shows the table as it was declared.
    every: 3000,
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
              ['State', h('span', { class: ['badge', t.loader.state === 'Faulted' ? 'crit' : t.loader.state === 'Waiting' ? 'warn' : t.loader.state === 'Streaming' || t.loader.state === 'Idle' ? 'good' : 'info'] }, t.loader.state)],
              t.loader.waitingFor && ['Waiting for', [h('span', { class: 'mono' }, t.loader.waitingFor), t.loader.waitingSince ? [' · began ', timeEl(t.loader.waitingSince)] : '',
                h('p', { class: 'help' }, 'Nothing has failed: what the loader reads is not there. A file yet to be delivered looks the same as a path with a mistake in it.')]],
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

        sizeCard(detail),

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

  /** What the table holds in this node's memory, estimated — and said to be. */
  function sizeCard(detail) {
    const z = detail.size, m = detail.table.memory;
    if (!m) return card('Memory', null, empty('Not estimated', 'The table is not open on this node.'));
    const rows = detail.table.rows;
    const perRow = rows ? m.rowBytes / rows : 0;
    return card('Memory', `about ${fmtBytes(m.totalBytes)} of this node’s managed heap`,
      h('div', { class: 'stack' },
        h('div', { class: 'tiles plain' },
          tile('Rows', '≈ ' + fmtBytes(m.rowBytes), '', `${fmtInt(rows)} rows × ${fmtNum(Math.round(perRow))} B, the key map included`),
          tile('Indexes', '≈ ' + fmtBytes(m.indexBytes), '', detail.table.indexes.length === 0 ? 'none declared' : `${plural(detail.table.indexes.length, 'index', 'indexes')}: their own maps and groups`),
          tile('Retained history', '≈ ' + fmtBytes(m.historyBytes), '', `${plural(m.historyRows, 'superseded row')} in ${plural(m.historyChanges, 'recent change')}`),
          tile('One row', fmtNum(m.meanRowBytes), 'B', m.sampledRows ? `its objects, over ${plural(m.sampledRows, 'row')} sized as written` : 'no row sized yet')),
        h('p', { class: 'help' },
          'How this is estimated: rows are sized as they are written or restored — each of the first 256, then one in 512 — by reading their fields: the object, its strings and whatever else only it refers to, with headers and rounding as the runtime allocates them. ',
          'The mean is multiplied by the row count, and the key map’s cost per row added. An index is its own map per group and one key per row; history is the rows the recent versions replaced or removed, which only those versions keep alive. ',
          'A string instance shared by many rows is counted in each, so a table of such rows is overstated. ',
          z ? ['As data the rows are smaller: about ', h('strong', { class: 'num' }, fmtBytes(Math.round(z.rowsBytes))), ` (${fmtNum(z.meanRowBytes)} B a row as JSON, from ${plural(z.sampledRows, 'sampled row')}`,
            z.historyBytes == null ? `; ${fmtInt(z.retainedVersions)} versions of history are held by the log’s brokers, not here` : `; the log holds ${fmtInt(z.historyChanges)} row changes of history in this process`, '). '] : '',
          'The process as a whole is on the ', h('a', { href: '#/cluster' }, 'cluster page'), '.')),
      { flush: false });
  }

  const STORE_KINDS = {
    value: 'one value per key', indexed: 'weighted values under a key', weight: 'weighted values', deadline: 'a deadline per key',
    bytes: 'as bytes: no circuit built on this node has given it types',
  };

  /** What the pipeline's state holds in this node's memory, store by store. */
  function stateCard(detail) {
    const z = detail.pipeline.stateSize, stores = detail.stateStores || [];
    if (!z || (!z.entries && stores.length === 0))
      return card('State', null, empty('No state', 'This pipeline keeps nothing between steps on this node, or has not committed yet.'));
    return card('State', `${plural(z.entries, 'entry', 'entries')} · about ${fmtBytes(z.bytes)} of this node’s managed heap`,
      h('div', { class: 'stack' },
        z.entriesHeldTwice > 0 && notice('info', h('strong', null, `${fmtNum(z.entriesHeldTwice)} entries are held twice. `),
          `The circuit was built on restored state and has not committed since, so the bytes it was built from are still kept beside it — about ${fmtBytes(z.bytesHeldTwice)}. They go at its first commit after a record arrives`,
          detail.pipeline.state === 'Built' ? ' on the leader.' : '.'),
        z.entriesAsBytes > 0 && z.entriesAsBytes === z.entries && notice('info', 'This node follows the state as bytes: it has no circuit built for this pipeline.'),
        detail.pipeline.stateGrowingSince && notice('warn', h('strong', null, 'The state has grown without a pause since ', timeEl(detail.pipeline.stateGrowingSince), '. '),
          'A pipeline that keeps something per record — a retroactive join, a remembered lookup, a keyed input — holds it until something closes it. ',
          'Over a stream of facts, give the input event time and forget each record once it is final (EvictWhenFinal); see the guide, “Bounding a join over a stream of facts”.'),
        h('div', { class: 'table-wrap' }, h('table', { class: 'data' },
          h('thead', null, h('tr', null, h('th', null, 'Store'), h('th', null, 'Kind'), h('th', { class: 'r' }, 'Entries'), h('th', { class: 'r' }, 'Memory'),
            h('th', { class: 'r', title: 'The objects of one entry’s key and value, from entries sized as they were written or read' }, 'One entry'),
            h('th', { class: 'r', title: 'Mean size of the store’s records in the state changelog, before compression' }, 'One record'))),
          h('tbody', null, stores.map(st => h('tr', { key: st.name },
            h('td', { class: 'id wrap' }, st.name),
            h('td', null, h('span', { class: 'tag', title: STORE_KINDS[st.kind] || '' }, st.kind)),
            h('td', { class: 'r' }, fmtInt(st.entries)),
            h('td', { class: 'r' }, '≈ ' + fmtBytes(st.estimatedBytes)),
            h('td', { class: 'r' }, st.sampledEntries ? fmtNum(st.meanEntryBytes) + ' B' : '–'),
            h('td', { class: 'r' }, st.meanRecordBytes ? fmtNum(st.meanRecordBytes) + ' B' : '–')))))),
        z.stores > stores.length && h('p', { class: 'help' }, `The ${fmtInt(stores.length)} largest of ${fmtInt(z.stores)} stores.`),
        h('p', { class: 'help' },
          'Entries are counted. Memory is estimated: the objects of an entry — sized as entries are written to the changelog or read from it, each of a store’s first 256 and then one in 512 — times the entries, plus the store’s own maps. ',
          'An object two stores share is counted in both. A state that grows without a pause for half an hour (ServiceOptions.StateGrowthAlarmAfter) is flagged here, on the overview and as the gauge pipeline.state_growing.')),
      { flush: false });
  }

  function rowBrowser(ctx, name, t, exposing) {
    const ui = ctx.ui;
    if (!exposing)
      return card('Rows', null, empty('Row data is not exposed', 'This admin API is configured without row data (ServiceAdminOptions.ExposeRowData), so rows, records and exception messages are not returned.'));
    ui.mode = ui.mode || 'browse';
    ui.limit = ui.limit || 15;
    // A page is read at a cursor the server gave with the page before it: a position in
    // the table's scan (from), or an offset under an index key (skip). The trail is the
    // cursors of the pages passed, so Previous is a step back along it.
    ui.trail = ui.trail || [0];
    const run = async (trail = [0]) => {
      ui.trail = trail; ui.busy = true; ui.rowError = null; ctx.render();
      const at = trail[trail.length - 1];
      const params = new URLSearchParams({ limit: ui.limit });
      if (ui.mode === 'browse') { params.set('from', at); if (ui.contains) params.set('contains', ui.contains); }
      else {
        params.set('key', ui.key || '');
        if (ui.mode.startsWith('index:')) { params.set('index', ui.mode.slice(6)); params.set('skip', at); }
      }
      try {
        ui.rows = await api(`tables/${enc(name)}/rows?${params}`);
        ui.rowsFor = ui.mode;
      } catch (e) { ui.rowError = e; ui.rows = null; }
      ui.busy = false; ctx.render();
    };
    if (!ui.rows && !ui.busy && !ui.rowError && !ui.started && t.state !== 'Opening') { ui.started = true; setTimeout(() => run()); }
    const rows = ui.rows;
    const columns = rows ? [...new Set(rows.rows.flatMap(r => (r.row && typeof r.row === 'object' ? Object.keys(r.row) : ['value'])))].slice(0, 12) : [];
    const cell = v => (v == null ? '' : typeof v === 'object' ? JSON.stringify(v) : String(v));
    const lookup = ui.mode !== 'browse';
    const paged = ui.mode !== 'key';
    const searching = ui.mode === 'browse' && !!ui.contains;

    return card('Rows', 'read from this node’s copy, at one version',
      h('div', null,
        h('form', { class: 'toolbar', on: { submit: e => { e.preventDefault(); run(); } } },
          h('select', { class: 'input', 'aria-label': 'How to read', on: { change: e => { ui.mode = e.target.value; ui.rows = null; ui.rowError = null; ui.trail = [0]; if (e.target.value === 'browse') run(); else ctx.render(); } } },
            h('option', { value: 'browse', selected: ui.mode === 'browse' }, 'Browse'),
            h('option', { value: 'key', selected: ui.mode === 'key' }, `By key (${t.keyType})`),
            t.indexes.map(i => h('option', { value: 'index:' + i.name, selected: ui.mode === 'index:' + i.name }, `By index ${i.name} (${i.keyType || '?'})`))),
          lookup
            ? h('input', { class: 'input grow', id: 'row-key', placeholder: ui.mode === 'key' ? 'A key, e.g. 42 or CP0003' : 'An index key', 'aria-label': 'Key', value: ui.key || '', on: { input: e => { const had = !!ui.key; ui.key = e.target.value; if (had !== !!ui.key) ctx.render(); } } })
            : h('input', { class: 'input grow', type: 'search', placeholder: 'Contains text (scans rows)', 'aria-label': 'Rows containing', value: ui.contains || '', on: { input: e => { ui.contains = e.target.value; } } }),
          h('button', { class: 'btn', type: 'submit', disabled: ui.busy || (lookup && !ui.key) }, ui.busy ? 'Reading…' : lookup ? 'Look up' : 'Read')),
        ui.rowError && h('div', { class: 'record' }, errorNotice(ui.rowError, 'The rows could not be read. ')),
        !rows && !ui.rowError && (lookup ? empty('Enter a key', 'A lookup reads one row by key, or the rows under one index key.') : A.skeleton()),
        rows && rows.rows.length === 0 && empty(lookup ? 'No row under that key' : rows.truncated ? 'No match yet' : 'No rows',
          rows.truncated ? `This read looked at ${fmtInt(rows.scanned)} rows and stopped at its bound without a match. Keep scanning to look at the rows after them.`
            : rows.lagVersions ? `This node is ${plural(rows.lagVersions, 'version')} behind the table’s log: the row may not have arrived here yet.`
            : lookup ? `Not in version ${fmtInt(rows.version)} of the table.` : ui.contains ? 'No row contains that text.' : 'The table is empty.'),
        rows && rows.rows.length > 0 && h('div', { class: 'table-wrap' }, h('table', { class: 'data rowgrid' },
          h('thead', null, h('tr', null, h('th', null, 'Key'), columns.map(c => h('th', null, c)))),
          h('tbody', null, rows.rows.map((r, i) => h('tr', { key: 'r' + i + cell(r.key) },
            h('td', { class: 'cell id', title: cell(r.key) }, cell(r.key)),
            columns.map(c => { const v = r.row && typeof r.row === 'object' ? r.row[c] : r.row; return h('td', { class: 'cell', title: cell(v) }, cell(v)); })))))),
        rows && h('div', { class: 'pager' },
          h('span', { class: 'info' },
            `Version ${fmtInt(rows.version)} · ${fmtInt(rows.rows.length)} shown · `,
            ui.mode === 'browse' ? (searching ? `looked at ${fmtInt(rows.scanned)} of ${fmtInt(rows.tableRows)} rows` : `of ${fmtInt(rows.tableRows)} rows`)
              : ui.mode === 'key' ? 'read by key' : `${fmtInt(rows.scanned)} under this key`,
            ui.trail.length > 1 && ` · page ${ui.trail.length}`,
            rows.truncated && ' · the scan stopped at its bound', rows.lagVersions ? ` · this node is ${plural(rows.lagVersions, 'version')} behind` : ''),
          paged && h('button', { class: 'btn small', type: 'button', disabled: ui.trail.length < 2 || ui.busy, on: { click: () => run(ui.trail.slice(0, -1)) } }, 'Previous'),
          paged && h('button', { class: 'btn small', type: 'button', disabled: rows.next == null || ui.busy, on: { click: () => run([...ui.trail, rows.next]) } },
            rows.truncated ? 'Keep scanning' : 'Next'),
          h('button', { class: 'btn small ghost', type: 'button', disabled: ui.busy, on: { click: () => run(ui.trail) } }, 'Refresh'))),
      { flush: true });
  }

  // ── plugins ─────────────────────────────────────────────────────────────────────────

  function definitionsTable(p) {
    if (p.definitions.length === 0) return null;
    const settings = d => (d.settings.length === 0 ? h('span', { class: 'muted' }, 'none')
      : d.settings.map(st => h('div', { key: st.name, title: st.description || null },
        h('span', { class: 'mono' }, st.name), h('span', { class: 'muted' }, `: ${st.jsonType}${st.default != null ? ' = ' + (typeof st.default === 'string' ? st.default : JSON.stringify(st.default)) : ''}${st.values && st.values.length ? ' (' + st.values.join(' | ') + ')' : ''}${st.minimum != null || st.maximum != null ? ` [${st.minimum ?? ''}..${st.maximum ?? ''}]` : ''}`),
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

  const diffMark = { added: ['good', '+ added'], removed: ['crit', '− removed'], changed: ['warn', '~ changed'], unchanged: ['idle', 'unchanged'] };
  const changeBadge = change => h('span', { class: ['badge', (diffMark[change] || diffMark.unchanged)[0]] }, (diffMark[change] || [null, change])[1]);

  /**
   * One side of a difference. A slot's text is its kind and then a line per type its
   * record is made of, `Name { member: Type; … }`: the lines both sides share are shown
   * quietly, and in a line that differs the members the other side lacks are marked.
   */
  function shapeCell(text, other) {
    if (!text) return h('span', { class: 'muted' }, '–');
    const lines = text.split('\n'), theirs = (other || '').split('\n');
    if (lines.length === 1) return text;
    const membersOf = line => { const m = /^(.*?) \{ (.*) \}$/.exec(line); return m ? [m[1], m[2].split('; ')] : [line, null]; };
    const byType = new Map(theirs.map(membersOf));
    return h('div', { class: 'shape' }, lines.map((line, i) => {
      if (theirs.includes(line)) return h('div', { key: 'l' + i, class: i === 0 ? null : 'same' }, line);
      const [type, members] = membersOf(line);
      if (!members) return h('div', { key: 'l' + i }, h('mark', null, line));
      const had = byType.get(type);
      if (!had) return h('div', { key: 'l' + i }, h('mark', null, line));
      return h('div', { key: 'l' + i }, type, ' { ', members.map((m, k) => [k > 0 && '; ', had.includes(m) ? m : h('mark', null, m)]), ' }');
    }));
  }

  /** Two versions of one plugin, side by side in words: what was added, removed, changed. */
  function diffView(ctx, name, versions) {
    const ui = ctx.ui;
    const cmp = (ui.diff = ui.diff || {})[name] = ui.diff[name] || { from: versions[1].version, to: versions[0].version };
    const run = async () => {
      cmp.busy = true; cmp.error = null; ctx.render();
      try { cmp.result = await api(`plugins/${enc(name)}/diff?from=${enc(cmp.from)}&to=${enc(cmp.to)}`); }
      catch (e) { cmp.error = e; cmp.result = null; }
      cmp.busy = false; ctx.render();
    };
    const pick = (key, label) => h('label', { class: 'field inline' }, h('span', { class: 'label' }, label),
      h('select', { class: 'input', 'aria-label': `${label} version of ${name}`, on: { change: e => { cmp[key] = e.target.value; cmp.result = null; ctx.render(); } } },
        versions.map(p => h('option', { value: p.version, selected: p.version === cmp[key] }, A.versionLabel(p)))));
    const r = cmp.result;
    const changedDefs = r ? r.definitions.filter(d => d.change !== 'unchanged') : [];
    return h('details', { class: 'more diff', key: 'diff' },
      h('summary', null, 'Compare two versions'),
      h('div', { class: 'stack' },
        h('div', { class: 'toolbar bare' }, pick('from', 'From'), pick('to', 'To'),
          h('button', { class: 'btn', type: 'button', disabled: cmp.busy || cmp.from === cmp.to, on: { click: run } }, cmp.busy ? 'Comparing…' : 'Compare')),
        cmp.from === cmp.to && h('p', { class: 'help' }, 'Choose two different versions.'),
        cmp.error && errorNotice(cmp.error, 'The versions could not be compared. '),
        r && h('div', { class: 'stack' },
          r.notes.map(n => notice('info', n)),
          changedDefs.length === 0 && r.tableDefinitions.length === 0 && notice('info', h('strong', null, 'Nothing declared differs. '),
            r.shapesCompared ? 'The definitions, their settings, their input, sink and table slots and the types those carry are the same.' : 'The definitions and their settings are the same.',
            ' What a definition computes is not compared: two versions can declare the same and do different things.'),
          (changedDefs.length > 0 || r.tableDefinitions.length > 0) && h('div', { class: 'table-wrap' }, h('table', { class: 'data difftable' },
            h('thead', null, h('tr', null, h('th', null, 'What'), h('th', null, 'Change'), h('th', null, short(r.from, 8)), h('th', null, short(r.to, 8)))),
            h('tbody', null,
              r.definitions.filter(d => d.change !== 'unchanged').flatMap(d => [
                h('tr', { key: 'd' + d.definition, class: 'group' }, h('td', { class: 'id' }, 'definition ', h('strong', null, d.definition)), h('td', null, changeBadge(d.change)), h('td', null), h('td', null)),
                ...d.members.map(m => h('tr', { key: 'm' + d.definition + m.kind + m.name },
                  h('td', { class: 'member' }, h('span', { class: 'tag' }, m.kind), ' ', h('span', { class: 'mono' }, m.name)), h('td', null, changeBadge(m.change)),
                  h('td', { class: 'wrap' }, shapeCell(m.from, m.to)), h('td', { class: 'wrap' }, shapeCell(m.to, m.from)))),
                ...d.notes.map((n, i) => h('tr', { key: 'n' + d.definition + i }, h('td', { colspan: 4, class: 'muted wrap' }, n))),
              ]),
              r.tableDefinitions.map(m => h('tr', { key: 't' + m.name }, h('td', { class: 'member' }, h('span', { class: 'tag' }, 'table definition'), ' ', h('span', { class: 'mono' }, m.name.split('.').pop())),
                h('td', null, changeBadge(m.change)), h('td', null, m.from || h('span', { class: 'muted' }, '–')), h('td', null, m.to || h('span', { class: 'muted' }, '–'))))))),
          (changedDefs.length > 0) && h('p', { class: 'help' },
            `${plural(r.definitions.length - changedDefs.length, 'other definition')} unchanged. `,
            r.shapesCompared ? 'Settings, slots and the types they carry are compared; ' : 'Settings are compared; ',
            'what a definition computes, and the shape of its state, are not — a replace compares the state when it is planned.'))));
  }

  views.plugins = {
    title: () => 'Plugins',
    kinds: ['plugin', 'pipeline', 'catalog', 'table'],
    every: 5000,
    load: async () => ({ plugins: await all('plugins'), node: await api('node') }),
    render(ctx) {
      const { plugins, node } = ctx.data;
      const ui = ctx.ui, info = store.info || {};
      const groups = new Map();
      for (const p of plugins.items) groups.set(p.name, [...(groups.get(p.name) || []), p]);
      // Newest first: the import sequence is the order on every node.
      for (const versions of groups.values()) versions.sort((a, b) => (b.sequence || 0) - (a.sequence || 0) || a.version.localeCompare(b.version));

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

      const usedBy = p => {
        const parts = [];
        if (p.pipelines.length) parts.push(['runs ', p.pipelines.map((x, i) => [i > 0 && ', ', pipelineLink(x.id)])]);
        if (p.tables.length) parts.push([`${p.pipelines.length ? 'defines' : 'Defines'} ${p.tables.length === 1 ? 'table' : 'tables'} `, p.tables.map((x, i) => [i > 0 && ', ', tableLink(x)])]);
        if (!parts.length) return h('span', { class: 'muted' }, p.kind === 'Contract' ? 'No declared table is defined by it.' : 'No pipeline names this version.');
        return h('span', null, p.pipelines.length ? 'It ' : '', parts.map((x, i) => [i > 0 && '; ', x]), '.');
      };
      const inUse = p => p.users > 0 || p.tables.length > 0;

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
          : [...groups].map(([name, versions]) => card(h('span', { class: 'mono' }, name), `${versions[0].kind === 'Contract' ? 'contract' : 'pipeline plugin'} · ${plural(versions.length, 'version')}${versions.length > 1 ? ', newest first' : ''}`,
            h('div', null,
              versions.length > 1 && h('div', { class: 'record' }, diffView(ctx, name, versions)),
              versions.map((p, i) => h('div', { class: 'record', key: p.version },
                h('div', { class: 'head' },
                  h('strong', { class: 'mono', title: `Version ${p.version}: the hash of the plugin’s files` }, short(p.version, 16)),
                  versions.length > 1 && i === 0 && h('span', { class: 'tag' }, 'newest'),
                  h('span', { class: ['badge', p.loadState === 'loaded' || p.loadState === 'built-in' ? 'good' : p.loadState === 'pending' ? 'warn' : 'idle'],
                    title: { 'built-in': 'Compiled into the host', loaded: 'Loaded on this node', stored: 'Stored; loaded when a pipeline needs it', pending: 'A newer contract version: loaded when the process restarts', superseded: 'An older contract version: a newer one is loaded, and a restart loads the newest' }[p.loadState] }, p.loadState),
                  h('span', { class: 'spacer' }),
                  canAct() && !p.builtIn && h('button', { class: 'btn small danger', type: 'button', disabled: inUse(p), title: p.users > 0 ? 'A pipeline names this version' : p.tables.length ? 'A declared table is defined by it' : null, on: { click: () => A.pluginDelete(p, ctx.reload) } }, 'Delete…')),
                kv([
                  ['Imported', p.builtIn ? h('span', { class: 'muted' }, 'never: it is compiled into the host, and is whatever build the host is')
                    : p.importedAt ? [timeEl(p.importedAt, stamp(p.importedAt)), h('span', { class: 'muted' }, ` · ${ago(p.importedAt)}`), p.importedBy && [' by ', h('strong', null, p.importedBy)]]
                    : h('span', { class: 'muted' }, 'before import times were recorded')],
                  p.sequence != null && ['Sequence', [h('span', { class: 'num' }, '#' + p.sequence), h('span', { class: 'muted' }, ' · its place among everything the catalog has recorded; a later import has a higher one on every node')]],
                  ['Build', p.informationalVersion ? h('span', { class: 'mono', title: p.informationalVersion }, A.buildName(p.informationalVersion))
                    : h('span', { class: 'muted' }, p.builtIn ? 'the host’s' : 'the assembly carries no informational version')],
                  p.files != null && ['Files', `${plural(p.files, 'file')} · ${fmtBytes(p.bytes)} · main assembly ${p.mainAssembly}`],
                  ['Used by', usedBy(p)],
                ]),
                definitionsTable(p),
                p.tableDefinitions.length > 0 && h('div', null, h('span', { class: 'muted' }, 'Table definitions: '), h('span', { class: 'mono' }, p.tableDefinitions.map(x => x.split('.').pop()).join(', ')))))),
            { flush: true })));
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
        pageHead({ title: 'Dead letters', lede: 'Records a pipeline dropped instead of faulting on them: the record, the error and where it came from. A pipeline with a dead-letter sink stores every one durably; the newest are also kept here, per node and in memory, to look at.' }),
        store.node && store.node.role !== 'Leader' && notice('info', ['This node runs no pipelines. Dropped records are counted and kept by the node that runs them — the leader',
          leaderLink(' (', ')'), ' — so the zeroes here say nothing about what was dropped there.']),
        pipelines.length === 0
          ? h('section', { class: 'card' }, empty('No dead letters', store.node && store.node.role !== 'Leader' ? 'None is recorded on this node.' : 'No pipeline has dropped a record, and none is deployed with the DeadLetter error policy.'))
          : h('div', { class: 'split' },
            h('div', null, letters ? lettersTab({ ...ctx, pageSaysSo: true, data: { letters, detail: { pipeline: pipelines.find(p => p.id === id) || {} } } }, id) : h('section', { class: 'card' }, empty('Choose a pipeline', 'Its dropped records appear here.'))),
            card('Pipelines', 'with dropped records, or able to drop them',
              h('table', { class: 'data' },
                h('thead', null, h('tr', null, h('th', null, 'Pipeline'), h('th', null, 'Stored in'), h('th', { class: 'r' }, 'Dropped'))),
                h('tbody', null, pipelines.map(p => h('tr', { key: p.id, 'aria-current': p.id === id ? 'true' : null },
                  h('td', { class: 'id' }, h('a', { href: '#/dead-letters/' + enc(p.id), 'aria-current': p.id === id ? 'page' : null }, p.id), p.id === id && h('span', { class: 'sub' }, 'shown')),
                  h('td', null, p.deadLetterSink ? [h('span', { class: 'tag' }, p.deadLetterSink.connector), p.deadLetterSink.failedWrites > 0 && p.deadLetterSink.lastError && h('span', { class: 'sub err' }, 'a write failed')]
                    : p.deadLettersBound ? h('span', { class: 'tag' }, 'bound') : h('span', { class: 'muted', title: 'No durable sink: kept in memory on the leader only' }, 'memory only')),
                  h('td', { class: 'r' }, fmtNum(p.deadLetters)))))))));
    },
  };

  // ── cluster ─────────────────────────────────────────────────────────────────────────

  views.cluster = {
    title: () => 'Cluster',
    kinds: ['node', 'catalog', 'table'],
    every: 2000,
    async load() {
      const [node, tables, pipelines] = await Promise.all([api('node'), all('tables'), all('pipelines')]);
      store.leader = node.leader;
      return { node, tables, pipelines };
    },
    render(ctx) {
      const { node, tables, pipelines } = ctx.data;
      const st = node.status, lead = st.leadership, role = lead.role, kafka = lead.kafka, peer = st.peer, standby = st.standby;
      const acting = store.info && !store.info.readOnly;
      const take = st.lastTakeover;
      // Every stage is measured on one clock, each ending where the next begins: they add
      // up to the service's part of the takeover with nothing left over.
      const stages = take ? [
        ['Catalog', take.catalog, 'promote or open the catalog, load the stored contracts'],
        ['Tables', take.tables, `promote or open ${plural(take.tableCount, 'table')}, concurrently`],
        ['Leader record', take.leaderRecord, 'restore the catalog from its mirror where there is one; write who leads'],
        ['Plugin directory', take.pluginDirectory, 'import what the directory holds and the catalog lacks'],
        ['Seed tables', take.tableSeeds, 'declare the host’s seed tables, the first time only'],
        ['Pipelines', take.pipelines, `start ${plural(take.pipelineCount, 'pipeline')} at their committed step — those built while standing by as they are, the rest built first`],
        ['Seed pipelines', take.pipelineSeeds, 'deploy the host’s seed pipelines, the first time only: each is built and started'],
        ['Finishing', take.finish, 'record the seeds, prune the plugin cache, start the loaders'],
      ].filter(([, v]) => v != null) : [];
      const stageTotal = stages.reduce((a, [, v]) => a + v, 0) || 1;
      const gcText = g => [g.server ? 'server' : 'workstation', g.server && (g.dynamic ? 'heaps adapt' : g.heaps > 0 ? plural(g.heaps, 'heap') : 'a heap per core'),
        g.concurrent ? 'concurrent' : 'not concurrent', g.conserveMemory > 0 && `conserve ${g.conserveMemory}`, g.heapHardLimitBytes > 0 && `limit ${fmtBytes(g.heapHardLimitBytes)}`].filter(Boolean).join(' · ');
      const memoryRows = m => (m ? [
        ['Working set', [h('strong', { class: 'num' }, fmtBytes(m.workingSetBytes)), h('span', { class: 'muted' }, ' · what the operating system counts for the process')]],
        ['GC heap', [h('strong', { class: 'num' }, fmtBytes(m.gcLiveBytes || m.gcHeapBytes)), h('span', { class: 'muted' },
          m.gcLiveBytes ? ` live at the last collection · ${fmtBytes(m.gcCommittedBytes)} committed` : ` in use · ${fmtBytes(m.gcCommittedBytes)} committed`)]],
        m.otherBytes != null && ['Not the collector’s', [h('strong', { class: 'num' }, fmtBytes(m.otherBytes)), h('span', { class: 'muted' }, ' · the Kafka client’s buffers, the runtime, thread stacks')]],
        ['GC pauses', [h('strong', { class: 'num' }, fmtNum(m.gcPauseRecentPercent != null ? m.gcPauseRecentPercent : m.gcPausePercent) + ' %'), h('span', { class: 'muted' },
          m.gcPauseRecentPercent != null ? ` of the last minute · ${fmtNum(m.gcPausePercent)} % of the process’s life · ${plural(m.gen2Collections, 'full collection')}` : ` of the process’s life · ${plural(m.gen2Collections, 'full collection')}`)]],
        m.gc && ['Collector', h('span', { class: 'muted' }, gcText(m.gc))],
      ] : []);

      const selfCard = h('section', { class: 'card nodecard self', 'aria-label': 'This node' },
        h('div', { class: 'who' }, roleBadge(role, true), h('span', { class: 'node-name' }, st.nodeId), h('span', { class: 'tag' }, 'this node')),
        kv([
          ['Term', role === 'Standby' ? `${fmtInt(lead.leaderTerm)} (the leader’s)` : fmtInt(lead.term)],
          ['Health', h('span', null, healthBadge(st.health.level), st.health.reasons.length > 0 && h('ul', { class: 'muted' }, st.health.reasons.map(r => h('li', null, reasonText(r)))))],
          ['Ready', st.health.ready ? 'yes' : 'no'],
          ['Admin address', st.advertisedAdminUrl ? h('a', { href: st.advertisedAdminUrl }, st.advertisedAdminUrl) : h('span', { class: 'muted' }, 'not advertised')],
          ...memoryRows(node.memory),
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
            peer.adminUrl ? ['Admin address', h('a', { href: peerHref(peer.adminUrl) }, peer.adminUrl)]
              : leaderElsewhere && node.leader.adminUrl && ['Admin address', h('a', { href: peerHref(node.leader.adminUrl) }, node.leader.adminUrl)],
            ...memoryRows(peer.memory),
            !peer.memory && ['Memory', h('span', { class: 'muted' }, 'it did not say')],
          ]),
          node.processShared && peer.memory && h('p', { class: 'help' }, 'In memory the two nodes are one process, so these are the same process’s figures as this node’s, read a moment apart.'))
        : h('section', { class: 'card nodecard', 'aria-label': 'The other node' },
          h('div', { class: 'who' }, h('span', { class: 'role unknown big' }, icon('circle', 16), node.peerExpected ? 'No answer' : 'No peer'), h('span', { class: 'tag' }, 'peer')),
          h('p', { class: 'muted' }, node.peerExpected
            ? 'A second node is configured and does not answer. If this node fails, nothing takes over.'
            : 'This node was not told of another. A lone node is a valid deployment: there is just nothing to fail over to.'),
          leaderElsewhere && kv([['Leader on record', node.leader.adminUrl ? h('a', { href: peerHref(node.leader.adminUrl) }, node.leader.nodeId) : node.leader.nodeId]]));

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
              ['Pipelines', (() => {
                const of = s => pipelines.items.filter(p => p.desired === 'Running' && p.state === s).length;
                const built = of('Built'), unbuilt = of('Unbuilt');
                return [`${fmtInt(built)} built and ready to start`, ` · ${fmtInt(standby.pipelinesToBuild || 0)} still being built`,
                  unbuilt > 0 && h('span', null, ` · ${fmtInt(unbuilt)} to be built at a takeover (`, h('a', { href: '#/pipelines' }, 'see why'), ')')];
              })()],
            ])), { flush: false }),

        holdsCard(node),

        (() => {
          // How each pipeline's last start went: a takeover's cost is in here, pipeline by pipeline.
          const started = pipelines.items.filter(p => p.lastStart).sort((a, b) => b.lastStart.total - a.lastStart.total);
          if (started.length === 0) return null;
          return card('Last start of each pipeline', `on this node · slowest first`,
            h('div', { class: 'table-wrap' }, h('table', { class: 'data cards' },
              h('thead', null, h('tr', null, ['Pipeline', 'When', 'How', 'Plugin', 'Build', 'Start', 'Edges', 'Total'].map((x, i) => h('th', { class: i >= 3 ? 'r' : null }, x)))),
              h('tbody', null, started.map(p => h('tr', { key: p.id },
                td('Pipeline', 'id', h('a', { href: '#/pipelines/' + encodeURIComponent(p.id) }, p.id)),
                td('When', null, timeEl(p.lastStart.at)),
                td('How', null, p.lastStart.warm ? h('span', { class: 'badge good', title: 'Built while standing by; the start ran it' }, 'warm') : h('span', { class: 'badge info', title: 'Built at the start' }, 'built at start')),
                ...[['Plugin', 'plugin'], ['Build', 'build'], ['Start', 'start'], ['Edges', 'edges'], ['Total', 'total']].map(([l, k]) => td(l, 'r', fmtMs(p.lastStart[k])))))))),
            { flush: true });
        })(),

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
          card('Last takeover', take ? `term ${take.term}, ${ago(take.at)} · ${fmtMs(stageTotal)} in the service` : null,
            take ? h('div', { class: 'stack' },
              h('div', { class: 'stages', role: 'img', 'aria-label': stages.map(([k, v]) => `${k} ${fmtMs(v)}`).join(', ') },
                stages.map(([k, v], i) => { const el = h('span', { class: 'stage-' + (i + 1), title: `${k}: ${fmtMs(v)}` }); el.style.flex = Math.max(v, stageTotal / 200); return el; })),
              h('table', { class: 'data' }, h('tbody', null,
                stages.map(([k, v, what], i) => h('tr', { key: k }, h('td', null, h('span', { class: 'swatch stage-' + (i + 1) }), ' ', k, h('span', { class: 'sub' }, what)), h('td', { class: 'r' }, fmtMs(v)))),
                h('tr', { class: 'total' }, h('td', null, h('strong', null, 'The service’s part'), h('span', { class: 'sub' }, 'the stages above, measured end to end on one clock')), h('td', { class: 'r' }, h('strong', null, fmtMs(stageTotal)))),
                kafka && kafka.lastTakeover && h('tr', null, h('td', null, 'Whole takeover, as the leadership timed it',
                  h('span', { class: 'sub' }, `the service’s part, and around it fencing the old writer (${fmtMs(kafka.lastTakeover.fence ?? null)}) and promoting the storage (${fmtMs(kafka.lastTakeover.storage ?? null)})`)), h('td', { class: 'r' }, fmtMs(kafka.lastTakeover.total ?? null))),
                kafka && kafka.lastTakeover && kafka.lastTakeover.silence != null && h('tr', null, h('td', null, 'Before it began',
                  h('span', { class: 'sub' }, kafka.lastTakeover.reason === 'silence' ? 'how long the leader had been silent when this node moved' : `why this node moved: ${kafka.lastTakeover.reason || 'not recorded'}`)), h('td', { class: 'r' }, fmtMs(kafka.lastTakeover.silence))))))
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

  /** Who made a change, and how far that is known. */
  /** Where this node's memory is: what it holds by name, set against the process's figures. */
  function holdsCard(node) {
    const m = node.memory, x = node.holds;
    if (!m || !x) return null;
    const named = x.tableBytes + x.stateBytes;
    const live = m.gcLiveBytes || m.gcHeapBytes;
    const rest = Math.max(0, live - named);
    const slack = Math.max(0, m.gcCommittedBytes - live);
    const parts = [
      ['Tables', x.tableBytes, 'hold-1', `${plural(x.tableCount, 'table')}: rows, indexes and retained history, estimated`],
      ['Pipeline state', x.stateBytes, 'hold-2', `${plural(x.stateEntries, 'entry', 'entries')} in ${plural(x.pipelineCount, 'pipeline')}, estimated`],
      ['Everything else on the heap', rest, 'hold-3', 'circuits, buffers in flight, the plugins’ and the runtime’s own objects — and whatever the two estimates miss'],
      ['Committed and not in use', slack, 'hold-4', 'what the collector keeps ready beyond what was live at its last collection'],
      m.otherBytes != null && ['Not the collector’s', m.otherBytes, 'hold-5', 'the Kafka client’s buffers, the runtime, thread stacks'],
      m.otherBytes != null && ['The program itself', Math.max(0, m.workingSetBytes - m.gcCommittedBytes - m.otherBytes), 'hold-6', 'the runtime’s and the assemblies’ code, and other files mapped into the process'],
    ].filter(Boolean);
    const total = Math.max(1, parts.reduce((a, [, v]) => a + v, 0));
    const bar = h('div', { class: 'holdbar', role: 'img', 'aria-label': 'This node’s memory by what holds it: ' + parts.map(([l, v]) => `${l} ${fmtBytes(v)}`).join(', ') },
      parts.map(([label, value, fill]) => {
        const piece = h('span', { class: fill, title: `${label}: ${fmtBytes(value)}` });
        piece.style.width = (100 * value / total).toFixed(2) + '%';
        return piece;
      }));
    return card('Memory', `${fmtBytes(m.workingSetBytes)} working set · ${fmtBytes(named)} of it is tables and state by name`,
      h('div', { class: 'stack' },
        bar,
        h('table', { class: 'data' },
          h('tbody', null, parts.map(([label, value, fill, help]) => h('tr', { key: label },
            h('td', null, h('span', { class: ['swatch', fill] }), ' ', label, h('span', { class: 'sub' }, help)),
            h('td', { class: 'r num' }, (label === 'Tables' || label === 'Pipeline state' ? '≈ ' : '') + fmtBytes(value)))))),
        h('div', { class: 'grid two' },
          x.tables.length > 0 && h('div', null, h('h3', { class: 'minor' }, 'Largest tables'),
            h('table', { class: 'data' }, h('tbody', null, x.tables.map(t => h('tr', { key: t.name },
              h('td', { class: 'id' }, tableLink(t.name)), h('td', { class: 'r' }, fmtNum(t.rows) + ' rows'), h('td', { class: 'r' }, '≈ ' + fmtBytes(t.bytes))))))),
          x.pipelines.some(p => p.bytes > 0) && h('div', null, h('h3', { class: 'minor' }, 'Largest state'),
            h('table', { class: 'data' }, h('tbody', null, x.pipelines.filter(p => p.bytes > 0).map(p => h('tr', { key: p.id },
              h('td', { class: 'id' }, pipelineLink(p.id)), h('td', { class: 'r' }, fmtNum(p.entries) + ' entries'), h('td', { class: 'r' }, '≈ ' + fmtBytes(p.bytes)))))))),
        node.memoryRelease && h('p', { class: 'help' },
          'Given back to the operating system since this process started: ', h('strong', { class: 'num' }, fmtBytes(node.memoryRelease.nativeReturnedBytesTotal)),
          ' the Kafka client had freed and the C allocator kept',
          node.memoryRelease.collectionsInducedTotal > 0 ? `; ${plural(node.memoryRelease.collectionsInducedTotal, 'young collection')} asked for so that the collector would let go of what a burst left it holding` : '',
          '. Last looked at ', timeEl(node.memoryRelease.at), '.'),
        h('p', { class: 'help' },
          'The working set is what a machine or a container has to give this node. Tables and state are estimates made from sampled rows and entries (each table’s and pipeline’s page says how); the rest are the runtime’s own figures, read a moment ago. ',
          'The guide’s “Sizing a node” has the arithmetic for a deployment that does not exist yet.')),
      { flush: false });
  }

  function callerCell(caller) {
    const stated = ' (self-declared)';
    if (caller === 'local') return h('span', { class: 'muted', title: 'An unauthenticated loopback caller who gave no name: nothing more is known. Set your name with the button in the header, or have the host authenticate operators.' }, 'local · unnamed');
    if (caller === 'seed' || caller === 'service' || caller === 'plugin-directory')
      return h('span', { class: 'muted', title: 'Done by the service itself, not by a person' }, caller);
    if (caller.endsWith(stated)) return [h('strong', null, caller.slice(0, -stated.length)), ' ', h('span', { class: 'tag', title: 'The name the caller gave: nothing verified it' }, 'self-declared')];
    return h('strong', { title: 'The identity the host authenticated' }, caller);
  }

  views.audit = {
    title: () => 'Audit',
    kinds: ['catalog', 'pipeline', 'table', 'plugin', 'node'],
    every: 5000,
    load: async ctx => ({ audit: await api(`audit?limit=50${ctx.ui.before ? '&before=' + ctx.ui.before : ''}`) }),
    render(ctx) {
      const { audit } = ctx.data;
      return h('div', { class: 'stack' },
        pageHead({ title: 'Audit', lede: [
          'Every change made to this service, kept in the catalog: it survives a failover. Newest first. ',
          store.who && !store.who.verified && 'Who is the name the host authenticated, where it does; here nobody is signed in, so it is the name a caller gave — marked self-declared — or “local”.'] }),
        h('section', { class: 'card' },
          audit.items.length === 0 ? empty('No entries', 'Deploys, resets, removals and role changes are recorded here with who asked for them.')
            : h('div', { class: 'table-wrap' }, h('table', { class: 'data cards' },
              h('thead', null, h('tr', null, h('th', { class: 'r' }, '#'), h('th', null, 'When'), h('th', null, 'Who'), h('th', null, 'Action'), h('th', null, 'Target'), h('th', null, 'Generation'), h('th', null, 'Node · term'), h('th', null, 'What it did'))),
              h('tbody', null, audit.items.slice().reverse().map(a => h('tr', { key: 'a' + a.sequence },
                td('#', 'r', a.sequence),
                td('When', 'nowrap', timeEl(a.time, stamp(a.time))),
                td('Who', null, callerCell(a.caller)),
                td('Action', null, h('span', { class: 'tag' }, a.action)),
                td('Target', 'id wrap', a.action.startsWith('pipeline.') && a.action !== 'pipeline.remove' ? pipelineLink(a.target) : a.target),
                td('Generation', 'nowrap', a.generationBefore == null && a.generationAfter == null ? ''
                  : a.generationBefore === a.generationAfter ? h('span', { title: 'The action did not change the generation' }, `${a.generationAfter}, unchanged`)
                  : `${a.generationBefore ?? '–'} → ${a.generationAfter ?? '–'}`),
                td('Node · term', 'nowrap', `${a.node} · ${a.term}`),
                td('What it did', 'wrap', a.plan && a.plan.length ? h('details', { class: 'more' }, h('summary', null, plural(a.plan.length, 'step')), A.planList({ steps: a.plan })) : '')))))),
          (audit.hasOlder || ctx.ui.before) && h('div', { class: 'pager' },
            h('span', { class: 'info' }, `${fmtInt(audit.total)} entries kept`),
            ctx.ui.before && h('button', { class: 'btn small', type: 'button', on: { click: () => { ctx.ui.before = null; ctx.reload(); } } }, 'Newest'),
            audit.hasOlder && h('button', { class: 'btn small', type: 'button', on: { click: () => { ctx.ui.before = audit.oldest; ctx.reload(); } } }, 'Older'))));
    },
  };
})();
