/* Dialogs for everything that changes something. Each one asks the server what the action
   will do (a dry run), shows that plan, and only then offers the button; a destructive
   action also asks for the id to be typed. */
(() => {
  'use strict';
  const A = window.__admin;
  const { h, morph, icon, api, notice, errorNotice, toast } = A;

  function modal(title, { wide } = {}) {
    const content = h('div', { class: 'content' });
    const footer = h('footer');
    const heading = h('h2', { id: 'dialog-title' }, title);
    const form = h('form', { method: 'dialog', novalidate: true }, h('header', null, heading), content, footer);
    const el = h('dialog', { class: ['modal', wide && 'wide'], 'aria-labelledby': 'dialog-title' }, form);
    document.body.append(el);
    el.addEventListener('close', () => el.remove());
    form.addEventListener('submit', e => e.preventDefault());
    el.showModal();
    return {
      el,
      set(contentKids, footerKids) {
        morph(content, h('div', { class: 'content' }, contentKids));
        morph(footer, h('footer', null, footerKids));
      },
      close: () => el.open && el.close(),
    };
  }

  const planList = plan => h('ol', { class: 'plan' }, plan.steps.map(step => h('li', null, h('span', null, step))));

  /**
   * One guarded action.
   *   dry(force)  → Promise<{ plan }>    the dry run
   *   run(force)  → Promise              the action itself
   *   typed       the text that must be typed, or null
   *   force       whether a refusal for readiness may be overridden
   */
  function confirmAction({ title, intro, verb, danger, typed, typedLabel, dry, run, force: mayForce, after, done }) {
    const dlg = modal(title);
    const st = { phase: 'planning', plan: null, error: null, text: '', force: false, runError: null };

    const plan = async () => {
      st.phase = 'planning';
      st.error = null;
      draw();
      try {
        st.plan = (await dry(st.force)).plan;
        st.phase = 'ready';
      } catch (e) {
        st.plan = null;
        st.error = e;
        st.phase = 'refused';
      }
      draw();
    };

    const go = async () => {
      st.phase = 'running';
      st.runError = null;
      draw();
      try {
        const result = await run(st.force);
        dlg.close();
        toast(done || `${title}: done.`);
        if (after) after(result);
      } catch (e) {
        st.runError = e;
        st.phase = 'ready';
        draw();
      }
    };

    function draw() {
      const typedOk = !typed || st.text === typed;
      const kids = [
        intro && h('p', null, intro),
        st.phase === 'planning' && h('p', { class: 'muted', 'aria-busy': 'true' }, 'Asking the node what this will do…'),
        st.plan && h('div', { class: 'field' }, h('div', { class: 'label' }, 'What will happen'), planList(st.plan)),
        st.error && errorNotice(st.error, 'The node refuses this. '),
        mayForce && (st.error && st.error.code === 'not-ready' || st.force) && h('label', { class: 'check' },
          h('input', { type: 'checkbox', checked: st.force, on: { change: e => { st.force = e.target.checked; plan(); } } }),
          h('span', null, h('strong', null, 'Force. '), 'Do it although the node says it is not safe.')),
        typed && st.plan && h('div', { class: 'field' },
          h('label', { for: 'confirm-text' }, typedLabel || 'Type ', !typedLabel && h('code', null, typed), !typedLabel && ' to confirm'),
          h('input', {
            class: 'input', id: 'confirm-text', autocomplete: 'off', spellcheck: 'false', value: st.text,
            'aria-describedby': 'confirm-hint',
            on: { input: e => { st.text = e.target.value; draw(); }, keydown: e => { if (e.key === 'Enter' && st.text === typed && st.phase === 'ready') go(); } },
          }),
          h('span', { class: 'muted', id: 'confirm-hint' }, typedOk ? 'Matches.' : 'The button unlocks when this matches exactly.')),
        st.runError && errorNotice(st.runError, 'It did not happen. '),
      ];
      dlg.set(kids, [
        h('button', { class: 'btn', type: 'button', on: { click: () => dlg.close() } }, 'Cancel'),
        h('button', {
          class: ['btn', danger ? 'danger solid' : 'primary'], type: 'button',
          disabled: st.phase !== 'ready' || !typedOk,
          on: { click: go },
        }, st.phase === 'running' ? 'Working…' : verb),
      ]);
    }
    plan();
  }

  const q = params => {
    const usp = new URLSearchParams();
    for (const [k, v] of Object.entries(params)) if (v != null && v !== false) usp.set(k, v === true ? 'true' : v);
    const text = usp.toString();
    return text ? '?' + text : '';
  };
  const ifMatch = generation => (generation == null ? {} : { 'If-Match': `"${generation}"` });

  const PIPELINE_ACTIONS = {
    start: { title: 'Start pipeline', verb: 'Start', method: 'POST', done: 'Start requested.' },
    stop: { title: 'Stop pipeline', verb: 'Stop', method: 'POST', done: 'Stopped.' },
    restart: { title: 'Restart pipeline', verb: 'Restart', method: 'POST', done: 'Restarted.',
      intro: 'The generation is torn down and a new one is built on the same state. This also clears Failed.' },
    reset: { title: 'Reset pipeline', verb: 'Reset state', method: 'POST', typed: true, danger: true, done: 'Reset.',
      intro: 'A reset discards the pipeline’s state and its input positions. It cannot be undone.' },
    remove: { title: 'Remove pipeline', verb: 'Remove', method: 'DELETE', typed: true, danger: true, done: 'Removed.',
      intro: 'The pipeline, its state and its input positions are removed. It cannot be undone.' },
  };

  function pipelineAction(action, pipeline, after) {
    const def = PIPELINE_ACTIONS[action];
    const id = pipeline.id;
    const path = '/pipelines/' + encodeURIComponent(id) + (action === 'remove' ? '' : '/' + action);
    const call = extra => api(path.slice(1) + q(extra), { method: def.method, headers: ifMatch(pipeline.generation) });
    confirmAction({
      title: `${def.title} ${id}`, intro: def.intro, verb: def.verb, danger: def.danger, typed: def.typed ? id : null, done: def.done, after,
      dry: () => call({ dryRun: true }),
      run: () => call(def.typed ? { confirm: id } : {}),
    });
  }

  function tableAction(action, table, after) {
    const name = table.name, enc = encodeURIComponent(name);
    const defs = {
      'loader-restart': { title: 'Restart loader of', verb: 'Restart loader', method: 'POST', path: `tables/${enc}/loader/restart`, done: 'Loader restarted.' },
      reopen: { title: 'Reopen table', verb: 'Reopen', method: 'POST', path: `tables/${enc}/reopen`, done: 'Reopened.',
        intro: 'The table is closed on this node and read again from its log. Pipelines built on it restart.' },
      remove: { title: 'Remove table', verb: 'Remove', method: 'DELETE', path: `tables/${enc}`, typed: true, danger: true, done: 'Removed.' },
    };
    const def = defs[action];
    confirmAction({
      title: `${def.title} ${name}`, intro: def.intro, verb: def.verb, danger: def.danger, typed: def.typed ? name : null, done: def.done, after,
      dry: () => api(def.path + q({ dryRun: true }), { method: def.method }),
      run: () => api(def.path + q({ confirm: name }), { method: def.method }),
    });
  }

  function pluginDelete(plugin, after) {
    const target = `${plugin.name}/${plugin.version}`;
    const path = `plugins/${encodeURIComponent(plugin.name)}/${encodeURIComponent(plugin.version)}`;
    confirmAction({
      title: `Delete plugin ${plugin.name}`, verb: 'Delete version', danger: true, typed: plugin.name, done: 'Plugin version deleted.', after,
      intro: `Version ${plugin.version} is deleted from the catalog and its files from storage.`,
      dry: () => api(path + q({ dryRun: true }), { method: 'DELETE' }),
      run: () => api(path + q({ confirm: target }), { method: 'DELETE' }),
    });
  }

  function nodeAction(action, nodeId, after) {
    const takeover = action === 'takeover';
    confirmAction({
      title: takeover ? `Take over with ${nodeId}` : `Step ${nodeId} down`,
      verb: takeover ? 'Take over' : 'Step down', danger: true, typed: nodeId, force: true, after,
      done: takeover ? 'Takeover started.' : 'Step-down started. This node’s process ends when it has resigned.',
      intro: takeover
        ? 'This node fences the current leader and starts leading. Pipelines stop on the old leader and are rebuilt here from their committed state.'
        : 'This node stops its pipelines and resigns. Resigning is final for this process: it ends, and its supervisor restarts it as a standby.',
      dry: force => (takeover ? api('node/takeover' + q({ dryRun: true, force }), { method: 'POST' }) : api('node/stepdown' + q({ dryRun: true, force }), { method: 'POST' })),
      run: force => (takeover ? api('node/takeover' + q({ confirm: nodeId, force }), { method: 'POST' }) : api('node/stepdown' + q({ confirm: nodeId, force }), { method: 'POST' })),
    });
  }

  /**
   * Deploy or replace: a spec as JSON, checked by a dry run before the button unlocks. A
   * change that can alter what the pipeline's state means is refused until the operator
   * says keep or reset; the dialog shows what changed and asks.
   */
  function specDialog({ title, spec, existing, plugins, after }) {
    const dlg = modal(title, { wide: true });
    const st = {
      text: JSON.stringify(spec, null, 2), choice: null, needsChoice: false, changes: [],
      checked: null, result: null, error: null, busy: false, template: '',
    };
    const fingerprint = () => st.text + '|' + st.choice;

    const parse = () => {
      let value;
      const local = message => Object.assign(new A.ApiError(400, { error: { code: 'invalid', message } }), { local: true });
      try { value = JSON.parse(st.text); } catch (e) { throw local('The spec is not valid JSON: ' + e.message); }
      if (!value || typeof value !== 'object' || !value.id) throw local('The spec needs an "id".');
      // This dialog replaces one pipeline. The id in the JSON is where the request goes,
      // and the generation sent with it is that pipeline's: another id would replace
      // another pipeline under this one's title.
      if (existing && value.id !== existing.id)
        throw local(`This dialog replaces pipeline "${existing.id}"; its id cannot be changed here. To deploy "${value.id}", use Deploy on the pipelines page.`);
      return value;
    };
    const send = async dryRun => {
      const value = parse();
      delete value.generation;
      return api('pipelines/' + encodeURIComponent(value.id) + q({ dryRun, state: st.choice }), {
        method: 'PUT', body: value, headers: existing ? ifMatch(existing.generation) : {},
      });
    };
    const check = async () => {
      st.busy = true; st.error = null; st.result = null; draw();
      const mark = fingerprint();
      try {
        st.result = await send(true);
        st.checked = mark;
      } catch (e) {
        st.error = e;
        st.checked = null;
        if (e.code === 'state-choice-required') { st.needsChoice = true; st.changes = e.details; st.error = null; }
      }
      st.busy = false; draw();
    };
    const deploy = async () => {
      st.busy = true; st.error = null; draw();
      try {
        const result = await send(false);
        dlg.close();
        toast(existing ? 'Pipeline replaced.' : 'Pipeline deployed.');
        if (after) after(result);
      } catch (e) {
        st.error = e; st.busy = false; st.checked = null; draw();
      }
    };
    const useTemplate = value => {
      st.template = value;
      const [plugin, version, definition] = value.split('\u0000');
      const found = (plugins || []).find(p => p.name === plugin && p.version === version);
      const def = found && found.definitions.find(d => d.name === definition);
      if (!def) return;
      const settings = {};
      for (const setting of def.settings || []) settings[setting.name] = setting.jsonType === 'number' ? 0 : setting.jsonType === 'boolean' ? false : setting.jsonType === 'array' ? [] : '';
      st.text = JSON.stringify({
        id: definition, plugin, version, definition, desired: 'Running',
        settings: Object.keys(settings).length ? settings : undefined,
        circuit: { workers: 1 },
        tables: {},
        inputs: { '<slot>': { connector: 'memory', name: '<name>' } },
        sinks: { '<slot>': { connector: 'memory', name: '<name>' } },
      }, null, 2);
      st.checked = null; st.result = null; st.error = null;
      draw();
    };

    function draw() {
      const ready = st.checked === fingerprint() && st.result;
      dlg.set([
        !existing && plugins && plugins.length > 0 && h('div', { class: 'field' },
          h('label', { for: 'spec-template' }, 'Start from a definition'),
          h('select', { class: 'input', id: 'spec-template', on: { change: e => useTemplate(e.target.value) } },
            h('option', { value: '', selected: st.template === '' }, 'Choose…'),
            plugins.filter(p => p.kind === 'Pipeline').flatMap(p => p.definitions.map(d => {
              const value = [p.name, p.version, d.name].join('\u0000');
              return h('option', { value, selected: st.template === value }, `${d.name} — ${p.name}@${p.version.slice(0, 8)}`);
            })))),
        h('div', { class: 'field' },
          h('label', { for: 'spec-text' }, 'Spec (JSON)'),
          h('textarea', {
            class: 'input', id: 'spec-text', rows: 16, spellcheck: 'false', value: st.text,
            on: { input: e => { st.text = e.target.value; if (st.checked) { st.checked = null; draw(); } } },
          }, st.text),
          existing && st.text.includes('"***"') && h('p', { class: 'muted' },
            'A secret is shown as "***". Left as it is, the stored value is kept; type a value to change it.')),
        st.needsChoice && h('fieldset', { class: 'field' },
          h('legend', { class: 'label' }, 'This change can alter what the pipeline’s state means. Keep the state or reset it?'),
          st.changes.length > 0 && h('ul', { class: 'muted' }, st.changes.map(c => h('li', null, 'changes ' + c))),
          h('div', { class: 'radio-row' },
            ['keep', 'reset'].map(choice => h('label', null,
              h('input', { type: 'radio', name: 'state-choice', value: choice, checked: st.choice === choice, on: { change: () => { st.choice = choice; st.checked = null; draw(); } } }),
              h('span', null, h('strong', null, choice === 'keep' ? 'Keep the state' : 'Reset the state'),
                h('small', null, choice === 'keep'
                  ? 'The new generation reads the old one’s entries. Right only while the shapes of the types in its state are unchanged.'
                  : 'State and input positions are dropped; sources start again. Upsert sinks keep rows the new run does not produce.')))))),
        st.result && h('div', { class: 'field' }, h('div', { class: 'label' }, 'What will happen'), planList(st.result.plan)),
        st.result && h('p', { class: 'muted' },
          `Declares ${st.result.shape.inputs.length} input(s), ${st.result.shape.sinks.length} sink(s); reads ${st.result.shape.tablesRead.join(', ') || 'no table'}; writes ${st.result.shape.tablesWritten.join(', ') || 'no table'}.`),
        // What this dialog itself refuses (JSON that does not parse, an id that is not this
        // pipeline's) was never sent, so it is not the node's refusal.
        st.error && errorNotice(st.error, st.error.local ? 'This was not sent. ' : 'The node refuses this spec. '),
      ], [
        h('button', { class: 'btn', type: 'button', on: { click: () => dlg.close() } }, 'Cancel'),
        h('button', { class: 'btn', type: 'button', disabled: st.busy || (st.needsChoice && !st.choice), on: { click: check } }, st.busy && !ready ? 'Checking…' : 'Check (dry run)'),
        h('button', { class: 'btn primary', type: 'button', disabled: !ready || st.busy, on: { click: deploy } }, existing ? 'Replace' : 'Deploy'),
      ]);
    }
    draw();
  }

  Object.assign(A, { modal, confirmAction, pipelineAction, tableAction, pluginDelete, nodeAction, specDialog, planList });
})();
