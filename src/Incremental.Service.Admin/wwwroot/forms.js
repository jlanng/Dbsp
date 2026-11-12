/* The two forms that write a spec: deploy or replace a pipeline, and declare a table.
   Both are built from what the node says exists — plugin versions and their definitions,
   a definition's settings and slots, the registered connectors and connections, the table
   definitions and loader kinds — so nothing has to be known from the host's source. The
   spec itself is the one thing edited: the form writes into it, the JSON view shows it,
   and either can be used at any moment. */
(() => {
  'use strict';
  const A = window.__admin;
  const { h, icon, api, all, notice, errorNotice, toast, modal, planList, short, stamp, pipelineBadge } = A;
  const enc = encodeURIComponent;
  const clone = value => JSON.parse(JSON.stringify(value));
  const NAME = /^[a-z0-9][a-z0-9._-]{0,62}$/;

  const field = (id, label, control, help, tags) => h('div', { class: 'field' },
    h('label', { for: id }, label, tags),
    control,
    help && h('span', { class: 'help' }, help));

  /** `1.0.0+d7c6215f6371…` as `1.0.0+d7c6215f`: the build's own name, short enough to read. */
  const buildName = text => {
    if (!text) return '';
    const plus = text.indexOf('+');
    return plus < 0 ? text : text.slice(0, plus + 9);
  };

  /** One line that tells two versions of a plugin apart: when, by whom, what the build calls itself. */
  function versionLabel(p) {
    if (p.builtIn) return `${short(p.version, 8)} · compiled into the host`;
    return [
      p.importedAt ? `imported ${stamp(p.importedAt)}` : 'import time not recorded',
      p.importedBy && `by ${p.importedBy}`,
      buildName(p.informationalVersion),
      short(p.version, 8),
    ].filter(Boolean).join(' · ');
  }

  // ── inputs for one property of a schema ─────────────────────────────────────────────

  /**
   * Text typed into an array or object field that is not that kind of JSON, by the field's
   * id. Such a field has no value to give, and silently keeping its last one would send a
   * spec the form does not show: a form with a draft on screen is not checked or sent.
   */
  const drafts = new Map();
  const draftProblem = () => {
    for (const [id, draft] of drafts)
      if (document.getElementById(id)) return `“${draft.name}” is not valid JSON (${draft.problem}). Correct it, or clear the field.`;
    return null;
  };

  /**
   * `p`: { name, jsonType, required, description, values, default (as text), minimum,
   * maximum, secret, tag }. `set(undefined)` removes the property from what is edited.
   */
  function propertyField(id, p, value, set) {
    const tags = [
      p.required && h('span', { class: 'req' }, 'required'),
      p.tag && h('span', { class: 'tag', title: p.tagTitle || null }, p.tag),
    ];
    const hasDefault = p.default != null && p.default !== '';
    const range = p.minimum != null && p.maximum != null ? `From ${p.minimum} to ${p.maximum}.`
      : p.minimum != null ? `At least ${p.minimum}.` : p.maximum != null ? `At most ${p.maximum}.` : '';
    const help = [p.description, hasDefault && `Default ${p.default}.`, range].filter(Boolean).join(' ');
    const unset = value == null || value === '';
    let control;
    if (p.values) {
      control = h('select', { class: 'input', id, on: { change: e => set(e.target.value === '' ? undefined : e.target.value) } },
        (!p.required || unset) && h('option', { value: '', selected: unset }, p.required ? 'Choose…' : hasDefault ? `Default (${p.default})` : 'Not set'),
        !unset && !p.values.includes(value) && h('option', { value, selected: true }, `${value} (not offered by this node)`),
        p.values.map(v => h('option', { value: v, selected: v === value }, v)));
      if (p.values.length === 0 && unset)
        return field(id, p.name, control, [help, help && ' ', h('strong', null, 'This node has none registered.')], tags);
    } else if (p.jsonType === 'boolean') {
      const on = value != null ? value === true : p.default === 'true';
      control = h('label', { class: 'check' },
        h('input', { type: 'checkbox', id, checked: on, on: { change: e => set(e.target.checked) } }), h('span', null, on ? 'Yes' : 'No'));
    } else if (p.jsonType === 'number') {
      control = h('input', {
        class: 'input', id, type: 'number', step: 'any', min: p.minimum, max: p.maximum, value: unset ? '' : value, placeholder: hasDefault ? p.default : null,
        on: { input: e => set(e.target.value === '' || Number.isNaN(Number(e.target.value)) ? undefined : Number(e.target.value)) },
      });
    } else if (p.jsonType === 'array' || p.jsonType === 'object') {
      const draft = drafts.get(id);
      const edit = text => {
        if (text.trim() === '') { drafts.delete(id); set(undefined); return; }
        let parsed, problem = null;
        try {
          parsed = JSON.parse(text);
          const isArray = Array.isArray(parsed), isObject = parsed !== null && typeof parsed === 'object' && !isArray;
          if (p.jsonType === 'array' ? !isArray : !isObject) problem = `it is not ${p.jsonType === 'array' ? 'an array: [ … ]' : 'an object: { … }'}`;
        } catch (e) { problem = e.message; }
        if (problem == null) { drafts.delete(id); set(parsed); return; }
        // Not a value yet. What is edited keeps the last one, so the text is held here
        // and the form is not sent while it stands: see draftProblem.
        drafts.set(id, { name: p.name, text, problem });
        set(value);
      };
      control = h('input', {
        class: 'input mono', id, spellcheck: 'false', autocomplete: 'off', value: draft ? draft.text : unset ? '' : JSON.stringify(value), placeholder: p.jsonType === 'array' ? '[ ]  as JSON' : '{ }  as JSON',
        'aria-invalid': draft ? 'true' : null, 'aria-describedby': draft ? `${id}-problem` : null,
        on: { input: e => edit(e.target.value) },
      });
      if (draft)
        return field(id, p.name, control, [h('span', { class: 'err', id: `${id}-problem` }, `Not valid JSON, so not used: ${draft.problem}. `), help], tags);
    } else {
      const kept = p.secret && value === '***';
      control = h('input', {
        class: 'input', id, type: p.secret ? 'password' : 'text', spellcheck: 'false', autocomplete: 'off', value: kept ? '' : unset ? '' : String(value),
        placeholder: kept ? 'the stored value is kept' : hasDefault ? p.default : null,
        on: { input: e => set(e.target.value === '' ? (kept ? '***' : undefined) : e.target.value) },
      });
    }
    return field(id, p.name, control, help, tags);
  }

  /**
   * The inputs for a connector's or a loader's config. `values` is edited in place. `kind`,
   * for a slot: a property the schema says is for other kinds of slot (`appliesTo`) is not
   * offered — unless the spec sets it, and then it is shown, marked, so that it can be
   * cleared: the connector refuses it.
   */
  function schemaFields(prefix, schema, values, changed, kind) {
    if (!schema)
      return h('p', { class: 'help' }, 'This kind does not describe its config. Whatever it needs is given in the JSON view.');
    const shown = schema.properties
      .filter(p => !kind || !p.appliesTo || p.appliesTo.includes(kind) || values[p.name] != null)
      .map(p => !kind || !p.appliesTo || p.appliesTo.includes(kind) ? p
        : { ...p, tag: `not for a ${kind}`, tagTitle: `For ${p.appliesTo.join(', ')} only: the connector refuses it here. Clear it.` });
    return [
      schema.description && h('p', { class: 'help' }, schema.description),
      shown.length > 0 && h('div', { class: 'fields' }, shown.map(p =>
        propertyField(`${prefix}-${p.name}`, p, values[p.name], v => { if (v === undefined) delete values[p.name]; else values[p.name] = v; changed(); }))),
      schema.open && h('p', { class: 'help' }, 'It accepts properties not listed here; add those in the JSON view.'),
    ];
  }

  /** A setting of a definition, as a property: its default becomes text, its attribute a tag. */
  const settingProperty = s => ({
    name: s.name, jsonType: s.jsonType, required: s.required, description: s.description, values: s.values || null,
    default: s.default == null ? null : typeof s.default === 'string' ? s.default : JSON.stringify(s.default),
    minimum: s.minimum, maximum: s.maximum,
    tag: s.stateNeutral ? 'state-neutral' : null, tagTitle: 'Changing it keeps the pipeline’s state without being asked',
  });

  /**
   * One slot's binding: which connector, then that connector's config. `carriers` are
   * the connectors the node says can carry this slot's types (a connector may carry only
   * some); null when the node was not asked, and then every connector of the kind is offered.
   */
  function bindingEditor(prefix, kind, binding, connectors, replace, changed, carriers) {
    const ofKind = connectors.filter(c => c.kinds.includes(kind));
    const able = carriers ? ofKind.filter(c => carriers.includes(c.name)) : ofKind;
    const current = connectors.find(c => c.name === binding.connector);
    return [
      field(`${prefix}-connector`, 'Connector',
        h('select', { class: 'input', id: `${prefix}-connector`, on: { change: e => replace({ connector: e.target.value }) } },
          !binding.connector && h('option', { value: '', selected: true }, 'Choose…'),
          binding.connector && !able.includes(current) && h('option', { value: binding.connector, selected: true },
            !current ? `${binding.connector} (not registered on this node)`
              : ofKind.includes(current) ? `${binding.connector} (does not carry this slot’s type)` : `${binding.connector} (cannot be bound to a ${kind})`),
          able.map(c => h('option', { value: c.name, selected: c.name === binding.connector }, c.name))),
        able.length === 0 ? `No registered connector can be bound to a ${kind}.` : null),
      current && able.includes(current) && schemaFields(prefix, current.config, binding, changed, kind),
    ];
  }

  const viewSwitch = (st, toForm, toJson) => h('div', { class: 'seg', role: 'group', 'aria-label': 'How to edit the spec' },
    h('button', { type: 'button', 'aria-pressed': String(st.view === 'form'), disabled: st.textError != null, title: st.textError != null ? 'The JSON does not parse yet' : null, on: { click: toForm } }, 'Form'),
    h('button', { type: 'button', 'aria-pressed': String(st.view === 'json'), on: { click: toJson } }, 'JSON (advanced)'));

  /** What a check answered is below the form: bring it into view, or it was answered unseen. */
  const showOutcome = () => { const el = document.getElementById('outcome'); if (el && el.firstChild) el.scrollIntoView({ block: 'nearest' }); };

  const local = message => Object.assign(new A.ApiError(400, { error: { code: 'invalid', message } }), { local: true });

  // ── deploy or replace a pipeline ────────────────────────────────────────────────────

  /**
   * `existing`: the pipeline being edited, when the dialog was opened from its page — its
   * id is then fixed. Opened from the list it starts with an empty id; typing the id of a
   * deployed pipeline turns the dialog into that pipeline's replace, visibly, and the
   * request then carries that pipeline's generation. A new id is sent as a plain deploy
   * that the server refuses if a pipeline of that id has appeared meanwhile.
   */
  async function deployDialog({ existing, spec, after }) {
    drafts.clear();
    const dlg = modal(existing ? `Replace pipeline ${existing.id}` : 'Deploy a pipeline', { wide: true });
    const st = {
      loading: true, loadError: null, view: 'form', text: '', textError: null,
      spec: spec ? clone(spec) : { id: '', plugin: '', version: '', definition: '', desired: 'Running', circuit: { workers: 1, checkpointInterval: 1, errorPolicy: 'Fault' }, tables: {}, inputs: {}, sinks: {} },
      plugins: [], connectors: [], tables: [], pipelines: [],
      described: null, describedFor: '', describing: false, describeError: null, notes: [], carriers: null, seq: 0,
      choice: null, needsChoice: false, evidence: [], checked: null, result: null, error: null, busy: false,
    };
    const s = () => st.spec;
    const target = () => existing || st.pipelines.find(p => p.id === s().id) || null;
    const fingerprint = () => JSON.stringify(s()) + '|' + st.choice;
    const keyOf = sp => `${sp.plugin}/${sp.version}/${sp.definition}`;

    /** Anything that changes the spec makes what the last dry run said stale. */
    const changed = ({ answer = false } = {}) => {
      st.checked = null; st.result = null; st.error = null;
      if (!answer) { st.needsChoice = false; st.evidence = []; st.choice = null; }
      draw();
    };

    const versionsOf = plugin => st.plugins.filter(p => p.name === plugin)
      .sort((a, b) => (b.sequence || 0) - (a.sequence || 0) || a.version.localeCompare(b.version));
    const pluginOf = () => st.plugins.find(p => p.name === s().plugin && p.version === s().version);

    /**
     * Asks the node what the chosen definition declares. `adopt` says what to do with the
     * spec's own settings and bindings: 'fresh' takes the node's starting spec for the
     * definition; 'merge' (another version of the same definition) keeps every setting
     * and binding whose name still exists and fills in what is new; 'keep' changes nothing.
     */
    async function describe(adopt) {
      const sp = s();
      st.described = null; st.describeError = null; st.notes = []; st.carriers = null;
      if (!sp.plugin || !sp.version || !sp.definition) { st.describedFor = ''; draw(); return; }
      const seq = ++st.seq;
      st.describing = true; draw();
      try {
        const under = adopt === 'keep' && sp.settings && Object.keys(sp.settings).length ? '?settings=' + enc(JSON.stringify(sp.settings)) : '';
        const described = await api(`plugins/${enc(sp.plugin)}/${enc(sp.version)}/definitions/${enc(sp.definition)}` + under);
        // The node's starting spec, and with it which connectors can carry each slot.
        // Editing a spec as it is ('keep') wants only the second, and does without it
        // if the node will not say.
        let template = null;
        try { template = await api(`plugins/${enc(sp.plugin)}/${enc(sp.version)}/definitions/${enc(sp.definition)}/template`); }
        catch (e) { if (adopt !== 'keep') throw e; }
        if (seq !== st.seq) return;
        st.carriers = template ? template.connectorsForSlot || null : null;
        if (template && adopt !== 'keep') {
          const from = template.spec, merge = adopt === 'merge';
          const pick = (mine, theirs, names) => Object.fromEntries(names.map(n => [n, merge && mine && n in mine ? mine[n] : (theirs || {})[n]]).filter(([, v]) => v !== undefined));
          sp.settings = pick(sp.settings, from.settings, described.settings.map(x => x.name));
          sp.inputs = pick(sp.inputs, from.inputs, described.inputs.map(x => x.name));
          sp.sinks = pick(sp.sinks, from.sinks, described.sinks.map(x => x.name));
          sp.tables = pick(sp.tables, from.tables, [...new Set(described.tables.map(x => x.slot))]);
          // The template names a memory log after the id it was made for, which is not
          // this pipeline's: left out, the connector names it after the pipeline itself.
          for (const [mine, theirs] of [[sp.inputs, from.inputs || {}], [sp.sinks, from.sinks || {}]])
            for (const [slot, binding] of Object.entries(mine || {}))
              if (binding === theirs[slot] && binding.connector === 'memory' && binding.name === `${from.id}/${slot}`) delete binding.name;
          st.notes = template.notes || [];
        }
        st.described = described;
        st.describedFor = keyOf(sp);
      } catch (e) {
        if (seq !== st.seq) return;
        st.describeError = e;
      }
      st.describing = false;
      changed();
    }

    const choosePlugin = name => {
      const versions = versionsOf(name);
      Object.assign(s(), { plugin: name, version: versions.length ? versions[0].version : '', definition: '' });
      const defs = versions.length ? versions[0].definitions : [];
      if (defs.length === 1) s().definition = defs[0].name;
      describe('fresh');
    };
    const chooseVersion = version => {
      const before = s().definition;
      s().version = version;
      const found = pluginOf();
      const still = found && found.definitions.some(d => d.name === before);
      if (!still) s().definition = '';
      describe(still ? 'merge' : 'fresh');
    };
    const chooseDefinition = name => { s().definition = name; describe('fresh'); };

    const toJson = () => { drafts.clear(); st.text = JSON.stringify(s(), null, 2); st.textError = null; st.view = 'json'; draw(); };
    const toForm = () => {
      if (st.textError != null) return;
      st.view = 'form';
      if (keyOf(s()) !== st.describedFor) describe('keep'); else draw();
    };
    const editText = text => {
      st.text = text;
      try {
        const value = JSON.parse(text);
        if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('a spec is a JSON object');
        if (existing) value.id = existing.id;
        st.spec = value; st.textError = null;
      } catch (e) { st.textError = e.message; }
      changed();
    };

    const body = () => {
      if (st.textError != null) throw local('The spec is not valid JSON: ' + st.textError);
      if (st.view === 'form' && draftProblem()) throw local(draftProblem());
      const value = clone(s());
      if (!value.id) throw local('Give the pipeline an id.');
      if (!NAME.test(value.id)) throw local(`"${value.id}" cannot be a pipeline id: lower-case letters, digits, dots, dashes and underscores, starting with a letter or digit, at most 63.`);
      if (!value.plugin || !value.definition) throw local('Choose a plugin and a definition.');
      delete value.generation;
      return value;
    };
    const send = dryRun => {
      const value = body(), replaced = target();
      const query = A.query({ dryRun, state: replaced ? st.choice : null });
      // A plain deploy says "generation 0": the server refuses it if a pipeline of this
      // id exists after all. A replace says which generation it replaces.
      return api('pipelines/' + enc(value.id) + query, { method: 'PUT', body: value, headers: { 'If-Match': `"${replaced ? replaced.generation : 0}"` } });
    };
    const check = async () => {
      st.busy = true; st.error = null; st.result = null; draw();
      const mark = fingerprint();
      try {
        // What the spec itself gets wrong — a slot left unbound, a setting out of range —
        // comes first: the dry run of a replace stops at keep-or-reset before it looks.
        if (target()) await api('pipelines/validate', { method: 'POST', body: body() });
        st.result = await send(true);
        if (mark === fingerprint()) st.checked = mark;
      } catch (e) {
        st.checked = null;
        if (e.code === 'state-choice-required') { st.needsChoice = true; st.evidence = e.details; }
        else st.error = e;
      }
      st.busy = false; draw(); showOutcome();
    };
    const deploy = async () => {
      st.busy = true; st.error = null; draw();
      const replaced = target();
      try {
        const result = await send(false);
        dlg.close();
        toast(replaced ? `Pipeline ${replaced.id} replaced.` : `Pipeline ${s().id} deployed.`);
        if (after) after(result);
      } catch (e) {
        st.error = e; st.busy = false; st.checked = null; draw(); showOutcome();
      }
    };

    function formView() {
      const sp = s(), replaced = target();
      const pipelinePlugins = [...new Set(st.plugins.filter(p => p.kind === 'Pipeline').map(p => p.name))];
      const versions = versionsOf(sp.plugin), found = pluginOf();
      const definitions = found ? found.definitions : [];
      const def = definitions.find(d => d.name === sp.definition);
      const d = st.described;
      sp.circuit = sp.circuit || {};
      const set = (object, key) => v => { if (v === undefined) delete object[key]; else object[key] = v; changed(); };

      const slot = (group, kindOf, x) => {
        sp[group] = sp[group] || {};
        const binding = sp[group][x.name] = sp[group][x.name] || { connector: '' };
        return h('div', { class: 'slot', key: group + x.name },
          h('div', { class: 'slot-head' }, h('strong', { class: 'mono' }, x.name),
            h('span', { class: 'tag' }, `${kindOf}<${x.keyType ? x.keyType + ', ' : ''}${x.type}>`)),
          x.members && x.members.length > 0 && h('details', { class: 'more' }, h('summary', null, group === 'inputs' ? 'What a record must carry' : 'What a record carries'),
            h('pre', { class: 'code wrap' }, x.members.join('\n'))),
          bindingEditor(`${group}-${x.name}`, x.kind, binding, st.connectors, next => { sp[group][x.name] = next; changed(); }, changed,
            st.carriers && st.carriers[x.name]));
      };

      const tableSlots = d ? [...new Map(d.tables.map(t => [t.slot, t])).values()] : [];
      const deadLetterAble = st.connectors.filter(c => c.kinds.includes('AppendSink'));

      return [
        h('div', { class: 'fields' },
          field('spec-id', 'Pipeline id',
            h('input', {
              class: 'input mono', id: 'spec-id', spellcheck: 'false', autocomplete: 'off', value: sp.id || '', readonly: !!existing, placeholder: 'a new id, e.g. large-trades',
              'aria-invalid': sp.id && !NAME.test(sp.id) ? 'true' : null,
              on: { input: e => { sp.id = e.target.value.trim(); changed(); } },
            }),
            existing ? 'An id is for life: to deploy this spec under another id, use Deploy on the pipelines page.'
              : sp.id && !NAME.test(sp.id) ? 'Lower-case letters, digits, dots, dashes and underscores; it starts with a letter or digit.'
              : 'Names the pipeline, its state and its input positions.'),
          field('spec-plugin', 'Plugin',
            h('select', { class: 'input', id: 'spec-plugin', disabled: !!existing, on: { change: e => choosePlugin(e.target.value) } },
              !sp.plugin && h('option', { value: '', selected: true }, pipelinePlugins.length ? 'Choose…' : 'No pipeline plugin is stored'),
              sp.plugin && !pipelinePlugins.includes(sp.plugin) && h('option', { value: sp.plugin, selected: true }, `${sp.plugin} (not stored)`),
              pipelinePlugins.map(n => h('option', { value: n, selected: n === sp.plugin }, n))),
            existing ? 'The plugin stays; its version and the definition can change.' : null)),
        sp.plugin && field('spec-version', 'Version',
          h('select', { class: 'input', id: 'spec-version', on: { change: e => chooseVersion(e.target.value) } },
            sp.version && !found && h('option', { value: sp.version, selected: true }, `${short(sp.version, 8)} (not stored)`),
            versions.map((p, i) => h('option', { value: p.version, selected: p.version === sp.version }, versionLabel(p) + (i === 0 && versions.length > 1 ? ' · newest' : '')))),
          found && !found.builtIn ? `Full version ${found.version}. A version is the hash of the plugin’s files.` : null),
        sp.plugin && field('spec-definition', 'Definition',
          h('select', { class: 'input', id: 'spec-definition', on: { change: e => chooseDefinition(e.target.value) } },
            !sp.definition && h('option', { value: '', selected: true }, definitions.length ? 'Choose…' : 'This version has no definitions'),
            sp.definition && !def && h('option', { value: sp.definition, selected: true }, `${sp.definition} (not in this version)`),
            definitions.map(x => h('option', { value: x.name, selected: x.name === sp.definition }, x.name))),
          def && def.description),

        st.describing && h('p', { class: 'muted', 'aria-busy': 'true' }, 'Asking the node what this definition declares…'),
        st.describeError && errorNotice(st.describeError, 'The definition could not be described, so its form cannot be drawn; the JSON view still works. '),
        d && d.notes.length > 0 && notice('warn', h('ul', null, d.notes.map(n => h('li', null, n)))),
        st.notes.length > 0 && notice('info', h('strong', null, 'To fill in: '), h('ul', null, st.notes.map(n => h('li', null, n)))),

        d && d.settings.length > 0 && h('fieldset', { class: 'section' }, h('legend', null, 'Settings'),
          h('div', { class: 'fields' }, d.settings.map(x => {
            sp.settings = sp.settings || {};
            return propertyField(`setting-${x.name}`, settingProperty(x), sp.settings[x.name], set(sp.settings, x.name));
          }))),

        d && d.inputs.length > 0 && h('fieldset', { class: 'section' }, h('legend', null, d.inputs.length === 1 ? 'Input' : `Inputs (${d.inputs.length})`),
          d.inputs.map(x => slot('inputs', x.kind, x))),
        d && d.sinks.length > 0 && h('fieldset', { class: 'section' }, h('legend', null, d.sinks.length === 1 ? 'Sink' : `Sinks (${d.sinks.length})`),
          d.sinks.map(x => slot('sinks', x.kind, x))),

        tableSlots.length > 0 && h('fieldset', { class: 'section' }, h('legend', null, 'Tables'),
          h('div', { class: 'fields' }, tableSlots.map(t => {
            sp.tables = sp.tables || {};
            const fits = st.tables.filter(x => x.definition === t.definition).map(x => x.name);
            const bound = sp.tables[t.slot] || (fits.includes(t.slot) ? t.slot : '');
            const id = `table-${t.slot}`;
            return field(id, [h('span', { class: 'mono' }, t.slot), ` · ${t.access === 'write' ? 'writes' : 'reads'} ${t.keyType} → ${t.rowType}`],
              h('select', { class: 'input', id, on: { change: e => { if (e.target.value === t.slot || e.target.value === '') delete sp.tables[t.slot]; else sp.tables[t.slot] = e.target.value; changed(); } } },
                !bound && h('option', { value: '', selected: true }, fits.length ? 'Choose…' : 'No table is declared from this definition'),
                bound && !fits.includes(bound) && h('option', { value: bound, selected: true }, `${bound} (not declared from ${t.definition.split('.').pop()})`),
                fits.map(n => h('option', { value: n, selected: n === bound }, n === t.slot ? `${n} (the slot’s own name)` : n))),
              fits.length === 0 ? ['Declare one on the ', h('a', { href: '#/tables' }, 'tables page'), ' first.']
                : fits.length > 1 ? 'Several tables are declared from this definition: this pipeline reads the one chosen.' : null);
          }))),

        (d || sp.definition) && h('fieldset', { class: 'section' }, h('legend', null, 'Running it'),
          h('div', { class: 'fields' },
            propertyField('circuit-workers', { name: 'workers', jsonType: 'number', minimum: 1, description: 'Threads the circuit runs on. Changing it on a deployed pipeline needs a reset.', default: '1' }, sp.circuit.workers, set(sp.circuit, 'workers')),
            propertyField('circuit-checkpoint', { name: 'checkpointInterval', jsonType: 'number', minimum: 1, description: 'Steps between checkpoints.', default: '1' }, sp.circuit.checkpointInterval, set(sp.circuit, 'checkpointInterval')),
            propertyField('circuit-errors', { name: 'errorPolicy', jsonType: 'string', values: ['Fault', 'DeadLetter'], required: true, description: 'A record an operator throws on faults the pipeline, or is dropped to the dead letters.' }, sp.circuit.errorPolicy || 'Fault', set(sp.circuit, 'errorPolicy')),
            propertyField('spec-desired', { name: 'desired', jsonType: 'string', values: ['Running', 'Stopped'], required: true, description: 'Whether it starts once deployed.' }, sp.desired || 'Running', set(sp, 'desired'))),
          (sp.circuit.errorPolicy === 'DeadLetter' || sp.deadLetters) && h('div', { class: 'slot' },
            h('div', { class: 'slot-head' }, h('strong', null, 'Dead-letter sink'), h('span', { class: 'tag' }, 'AppendSink<DeadLetterEnvelope>')),
            h('label', { class: 'check' },
              h('input', { type: 'checkbox', id: 'dead-letters-on', checked: !!sp.deadLetters, on: { change: e => { if (e.target.checked) sp.deadLetters = { connector: deadLetterAble.length === 1 ? deadLetterAble[0].name : '' }; else delete sp.deadLetters; changed(); } } }),
              h('span', null, h('strong', null, 'Store dropped records durably. '), 'Each is written to this sink before the checkpoint that passes it. Without one they are kept in memory on the leader only, the newest few, and lost at a failover.')),
            sp.deadLetters && bindingEditor('dead-letters', 'AppendSink', sp.deadLetters, st.connectors, next => { sp.deadLetters = next; changed(); }, changed))),
      ];
    }

    function draw() {
      const sp = s(), replaced = target();
      const ready = st.checked === fingerprint() && st.result;
      dlg.title(replaced ? `Replace pipeline ${replaced.id}` : 'Deploy a pipeline');
      if (st.loading || st.loadError) {
        dlg.set([st.loadError ? errorNotice(st.loadError, 'What there is to deploy with could not be read. ') : h('p', { class: 'muted', 'aria-busy': 'true' }, 'Reading the plugins, connectors and tables of this node…')],
          [h('button', { class: 'btn', type: 'button', on: { click: () => dlg.close() } }, 'Close')]);
        return;
      }
      const keepClosed = st.evidence.find(x => x.startsWith('keep is not possible'));
      dlg.set([
        h('div', { class: 'dialog-bar' },
          h('span', { class: 'muted' }, st.view === 'form' ? 'The form writes the spec; the JSON view shows it.' : 'The spec as it will be sent.'),
          viewSwitch(st, toForm, toJson)),

        replaced && !existing && h('div', { class: 'notice warn replace-note', role: 'alert' }, icon('alert', 14), h('div', null,
          h('strong', null, `A pipeline with the id “${replaced.id}” is deployed. `),
          'Going on replaces it: it is stopped and built again from this spec. ',
          h('span', { class: 'replace-what' }, pipelineBadge(replaced.state), ` ${replaced.definition} from ${replaced.plugin}@${short(replaced.version)} · generation ${replaced.generation}`),
          h('div', null, 'To add a pipeline instead, give it an id that is not in use.'))),

        st.view === 'form' ? formView() : h('div', { class: 'field' },
          h('label', { for: 'spec-text' }, 'Spec (JSON)'),
          h('textarea', { class: 'input', id: 'spec-text', rows: 22, spellcheck: 'false', value: st.text, 'aria-invalid': st.textError != null ? 'true' : null, on: { input: e => editText(e.target.value) } }, st.text),
          st.textError != null ? h('span', { class: 'help err' }, 'Not valid JSON yet: ' + st.textError)
            : h('span', { class: 'help' }, 'Every change here is in the form too.')),
        JSON.stringify(sp).includes('"***"') && h('p', { class: 'help' },
          'A secret is shown as "***". Left as it is, the stored value is kept; type a value to change it.'),

        h('div', { class: 'outcome', id: 'outcome' },
        replaced && st.needsChoice && h('fieldset', { class: 'section choice' },
          h('legend', null, 'Keep the pipeline’s state, or reset it?'),
          h('p', { class: 'help' }, 'This change can alter what the state means. What the node compared:'),
          st.evidence.length > 0 && h('ul', { class: 'evidence' }, st.evidence.map(c => h('li', null, c))),
          h('div', { class: 'radio-row' },
            ['keep', 'reset'].map(choice => h('label', null,
              h('input', { type: 'radio', name: 'state-choice', value: choice, checked: st.choice === choice, disabled: choice === 'keep' && !!keepClosed,
                on: { change: () => { st.choice = choice; st.checked = null; st.result = null; st.error = null; check(); } } }),
              h('span', null, h('strong', null, choice === 'keep' ? 'Keep the state' : 'Reset the state'),
                h('small', null, choice === 'keep'
                  ? 'The new generation reads the old one’s entries. Right while the stores and the types in them are unchanged; if it cannot start on them, the previous spec is put back.'
                  : 'State and input positions are dropped; sources start again. Upsert sinks keep rows the new run does not produce.')))))),

        st.result && h('div', { class: 'field' }, h('div', { class: 'label' }, 'What will happen'), planList(st.result.plan)),
        st.result && h('p', { class: 'muted' },
          `Declares ${st.result.shape.inputs.length} input(s), ${st.result.shape.sinks.length} sink(s); reads ${st.result.shape.tablesRead.join(', ') || 'no table'}; writes ${st.result.shape.tablesWritten.join(', ') || 'no table'}.`),
        // What this dialog itself refuses (JSON that does not parse, no id) was never
        // sent, so it is not the node's refusal.
        st.error && errorNotice(st.error, st.error.local ? 'This was not sent. '
          : st.error.code === 'state-incompatible' ? 'Not replaced: the state cannot be kept. '
          : st.error.code === 'precondition-failed' ? 'The pipeline changed while this dialog was open. Close it and start again. '
          : 'The node refuses this spec. ')),
      ], [
        h('button', { class: 'btn', type: 'button', on: { click: () => dlg.close() } }, 'Cancel'),
        // Busy is said, not enforced by disabling: a button that disables itself under the
        // finger drops the keyboard's focus out of the dialog.
        h('button', { class: 'btn', type: 'button', 'aria-busy': st.busy ? 'true' : null, disabled: st.describing || (replaced && st.needsChoice && !st.choice), on: { click: () => { if (!st.busy) check(); } } }, st.busy && !ready ? 'Checking…' : 'Check (dry run)'),
        h('button', { class: ['btn', 'primary'], type: 'button', disabled: !ready || st.busy, title: ready ? null : 'Unlocks when a dry run of exactly this spec has passed', on: { click: deploy } },
          replaced ? `Replace ${replaced.id}` : 'Deploy'),
      ]);
    }

    draw();
    try {
      const [plugins, connectors, tables, pipelines] = await Promise.all([all('plugins'), all('connectors'), all('tables'), all('pipelines')]);
      Object.assign(st, { plugins: plugins.items, connectors: connectors.items, tables: tables.items, pipelines: pipelines.items, loading: false });
    } catch (e) {
      st.loadError = e; st.loading = false; draw();
      return;
    }
    if (existing) await describe('keep');
    else {
      const names = [...new Set(st.plugins.filter(p => p.kind === 'Pipeline').map(p => p.name))];
      if (names.length === 1) choosePlugin(names[0]); else draw();
    }
  }

  // ── declare a table ─────────────────────────────────────────────────────────────────

  async function declareTableDialog({ after }) {
    drafts.clear();
    const dlg = modal('Declare a table', { wide: true });
    const st = {
      loading: true, loadError: null, view: 'form', text: '', textError: null,
      spec: { name: '', definition: '', onUnreadable: 'Fault' },
      definitions: [], sources: [], connections: [], tables: [],
      checked: null, plan: null, error: null, busy: false,
    };
    const s = () => st.spec;
    const fingerprint = () => JSON.stringify(s());
    const changed = () => { st.checked = null; st.plan = null; st.error = null; draw(); };

    const body = () => {
      if (st.view === 'form' && draftProblem()) throw local(draftProblem());
      if (st.textError != null) throw local('The spec is not valid JSON: ' + st.textError);
      const value = clone(s());
      if (!value.name) throw local('Give the table a name.');
      if (!NAME.test(value.name)) throw local(`"${value.name}" cannot be a table name: lower-case letters, digits, dots, dashes and underscores, starting with a letter or digit, at most 63.`);
      if (!value.definition) throw local('Choose the table’s definition.');
      return value;
    };
    const check = async () => {
      st.busy = true; st.error = null; st.plan = null; draw();
      const mark = fingerprint();
      try {
        st.plan = (await api('tables?dryRun=true', { method: 'POST', body: body() })).plan;
        if (mark === fingerprint()) st.checked = mark;
      } catch (e) { st.error = e; st.checked = null; }
      st.busy = false; draw(); showOutcome();
    };
    const declare = async () => {
      st.busy = true; st.error = null; draw();
      try {
        const result = await api('tables', { method: 'POST', body: body() });
        dlg.close();
        toast(`Table ${s().name} declared.`);
        if (after) after(result);
      } catch (e) { st.error = e; st.busy = false; st.checked = null; draw(); showOutcome(); }
    };
    const toJson = () => { drafts.clear(); st.text = JSON.stringify(s(), null, 2); st.textError = null; st.view = 'json'; draw(); };
    const toForm = () => { if (st.textError == null) { st.view = 'form'; draw(); } };
    const editText = text => {
      st.text = text;
      try {
        const value = JSON.parse(text);
        if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('a spec is a JSON object');
        st.spec = value; st.textError = null;
      } catch (e) { st.textError = e.message; }
      changed();
    };

    function formView() {
      const sp = s();
      const def = st.definitions.find(d => d.definition === sp.definition && (!sp.contract || d.contract === sp.contract));
      const source = sp.loader ? st.sources.find(x => x.kind === sp.loader.kind) : null;
      const fits = source && source.connection ? st.connections.filter(c => c.kind === source.connection).map(c => c.name) : [];
      const taken = st.tables.some(t => t.name === sp.name);
      return [
        field('table-definition', 'Definition',
          h('select', { class: 'input', id: 'table-definition', on: { change: e => {
            const chosen = st.definitions[Number(e.target.value)];
            if (!chosen) return;
            const wasDefault = !sp.name || (def && sp.name === def.defaultName);
            sp.definition = chosen.definition; sp.contract = chosen.contract;
            if (wasDefault) sp.name = st.tables.some(t => t.name === chosen.defaultName) ? '' : chosen.defaultName;
            changed();
          } } },
            !def && h('option', { value: '', selected: true }, st.definitions.length ? 'Choose…' : 'No contract with a table definition is loaded'),
            st.definitions.map((d, i) => h('option', { value: i, selected: d === def }, `${d.definition.split('.').pop()} — ${d.keyType} → ${d.rowType} · ${d.contract}`))),
          def ? [`${def.definition}: rows of ${def.rowType} keyed by ${def.keyType}. `, def.declaredAs.length ? `Declared so far as ${def.declaredAs.join(', ')}.` : 'Not declared yet.']
            : 'A table’s shape — its row type, its key, its indexes — comes from a definition class in a contract assembly.'),
        field('table-name', 'Table name',
          h('input', { class: 'input mono', id: 'table-name', spellcheck: 'false', autocomplete: 'off', value: sp.name || '', placeholder: def ? `e.g. ${def.defaultName}-eu` : 'a new name',
            'aria-invalid': (sp.name && !NAME.test(sp.name)) || taken ? 'true' : null, on: { input: e => { sp.name = e.target.value.trim(); changed(); } } }),
          taken ? h('span', { class: 'err' }, `A table named ${sp.name} is declared already.`)
            : sp.name && !NAME.test(sp.name) ? 'Lower-case letters, digits, dots, dashes and underscores; it starts with a letter or digit.'
            : def ? `A pipeline’s slot “${def.defaultName}” reads the table of that name unless its spec binds another.` : null),
        h('fieldset', { class: 'section' }, h('legend', null, 'What fills it'),
          field('table-loader', 'Loader',
            h('select', { class: 'input', id: 'table-loader', on: { change: e => { if (e.target.value === '') delete sp.loader; else sp.loader = { kind: e.target.value }; changed(); } } },
              h('option', { value: '', selected: !sp.loader }, 'None — a pipeline (ToTable) or the host writes it'),
              sp.loader && !source && h('option', { value: sp.loader.kind, selected: true }, `${sp.loader.kind} (not a kind this node has)`),
              st.sources.map(x => h('option', { value: x.kind, selected: !!sp.loader && x.kind === sp.loader.kind }, x.builtIn ? x.kind : `${x.kind} (this host’s)`))),
            source && source.config && source.config.description),
          source && source.connection && propertyField('table-connection',
            { name: 'connection', jsonType: 'string', required: true, values: fits, description: `${source.connection === 'kafka' ? 'A Kafka connection' : 'An object store'} the host registered. Where it points and how it authenticates stay with the host.` },
            sp.loader.connection, v => { if (v === undefined) delete sp.loader.connection; else sp.loader.connection = v; changed(); }),
          source && (source.config
            ? source.config.properties.length > 0 && h('div', { class: 'fields' }, source.config.properties.map(p => {
              sp.loader.config = sp.loader.config || {};
              return propertyField(`loader-${p.name}`, p, sp.loader.config[p.name], v => { if (v === undefined) delete sp.loader.config[p.name]; else sp.loader.config[p.name] = v; changed(); });
            }))
            : h('p', { class: 'help' }, 'This kind does not describe its config. If it needs one, give it as "config" in the JSON view.')),
          sp.loader && propertyField('table-unreadable',
            { name: 'onUnreadable', jsonType: 'string', required: true, values: ['Fault', 'Skip'], description: 'A record the loader cannot read stops the loader at it (Fault), or is counted, kept among the last few and passed over (Skip).' },
            sp.onUnreadable || 'Fault', v => { sp.onUnreadable = v || 'Fault'; changed(); })),
      ];
    }

    function draw() {
      if (st.loading || st.loadError) {
        dlg.set([st.loadError ? errorNotice(st.loadError, 'What a table can be declared from could not be read. ') : h('p', { class: 'muted', 'aria-busy': 'true' }, 'Reading the table definitions and loader kinds of this node…')],
          [h('button', { class: 'btn', type: 'button', on: { click: () => dlg.close() } }, 'Close')]);
        return;
      }
      const ready = st.checked === fingerprint() && st.plan;
      dlg.set([
        h('div', { class: 'dialog-bar' },
          h('span', { class: 'muted' }, 'A table is held once per node and read by any number of pipelines.'),
          viewSwitch(st, toForm, toJson)),
        st.view === 'form' ? formView() : h('div', { class: 'field' },
          h('label', { for: 'table-text' }, 'Table spec (JSON)'),
          h('textarea', { class: 'input', id: 'table-text', rows: 14, spellcheck: 'false', value: st.text, 'aria-invalid': st.textError != null ? 'true' : null, on: { input: e => editText(e.target.value) } }, st.text),
          st.textError != null ? h('span', { class: 'help err' }, 'Not valid JSON yet: ' + st.textError) : h('span', { class: 'help' }, 'Every change here is in the form too.')),
        h('div', { class: 'outcome', id: 'outcome' },
          st.plan && h('div', { class: 'field' }, h('div', { class: 'label' }, 'What will happen'), planList(st.plan)),
          st.error && errorNotice(st.error, st.error.local ? 'This was not sent. ' : 'The node refuses this table. ')),
      ], [
        h('button', { class: 'btn', type: 'button', on: { click: () => dlg.close() } }, 'Cancel'),
        h('button', { class: 'btn', type: 'button', 'aria-busy': st.busy ? 'true' : null, on: { click: () => { if (!st.busy) check(); } } }, st.busy && !ready ? 'Checking…' : 'Check (dry run)'),
        h('button', { class: 'btn primary', type: 'button', disabled: !ready || st.busy, title: ready ? null : 'Unlocks when a dry run of exactly this spec has passed', on: { click: declare } }, 'Declare'),
      ]);
    }

    draw();
    try {
      const [definitions, sources, connections, tables] = await Promise.all([all('table-definitions'), all('table-sources'), all('connections'), all('tables')]);
      Object.assign(st, { definitions: definitions.items, sources: sources.items, connections: connections.items, tables: tables.items, loading: false });
    } catch (e) { st.loadError = e; st.loading = false; }
    draw();
  }

  Object.assign(A, { deployDialog, declareTableDialog, versionLabel, buildName });
})();
