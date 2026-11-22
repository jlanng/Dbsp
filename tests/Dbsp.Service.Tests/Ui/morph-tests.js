/* What the admin UI's `morph` must do for form controls, checked in a real browser. A view
   draws a new tree from its model and `morph` patches the page to it; a control that has
   been used no longer follows its markup (`selected`, `checked`, `value` attributes), so
   unless `morph` sets the properties the page shows one thing and the model holds another
   — the deploy form once showed one definition and deployed another. Each case uses the
   control as a person would (sets its property), then redraws from a model that says
   something else, and looks at what the control shows. */
(() => {
  'use strict';
  const { h, morph } = window.__admin;
  const lines = [];
  const test = (name, fn) => {
    try { fn(); lines.push('ok ' + name); } catch (e) { lines.push('FAIL ' + name + ': ' + e.message); }
  };
  const eq = (actual, expected, what) => {
    if (actual !== expected) throw new Error(`${what}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  };
  const host = document.getElementById('main');
  const mount = tree => {
    const root = h('div');
    host.replaceChildren(root);
    return morph(root, tree);
  };
  const $ = id => document.getElementById(id);
  const select = (id, values, chosen) => h('select', { id }, values.map(v => h('option', { value: v, selected: v === chosen }, v)));

  test('a select that was chosen from shows the model’s choice', () => {
    const root = mount(h('div', null, select('d', ['a', 'b', 'c'], 'b')));
    $('d').value = 'c';
    morph(root, h('div', null, select('d', ['a', 'b', 'c'], 'a')));
    eq($('d').value, 'a', 'shown');
  });

  test('selects matched to other selects by a shift in the tree show their own model', () => {
    // The deploy form's fields are unkeyed: a notice appearing above them makes each
    // field's select be patched from the next one's, and back when it goes.
    const model = { plugin: 'enrichment', version: 'v2', definition: 'watchlist-alerts' };
    const form = notice => h('div', null,
      notice && h('div', { class: 'notice' }, 'A pipeline with this id is deployed.'),
      h('div', { class: 'field' }, select('plugin', ['enrichment', 'other'], model.plugin)),
      h('div', { class: 'field' }, select('version', ['v1', 'v2'], model.version)),
      h('div', { class: 'field' }, select('definition', ['desk-exposure', 'trade-enrichment', 'watchlist-alerts'], model.definition)));
    const root = mount(form(false));
    for (const id of ['plugin', 'version', 'definition']) $(id).value = $(id).value; // touched
    $('definition').value = 'watchlist-alerts';
    morph(root, form(true));
    morph(root, form(false));
    eq($('plugin').value, 'enrichment', 'plugin');
    eq($('version').value, 'v2', 'version');
    eq($('definition').value, 'watchlist-alerts', 'definition');
    eq($('definition').selectedOptions[0].textContent, 'watchlist-alerts', 'definition’s text');
  });

  test('a select whose options are replaced shows the new selected one', () => {
    const root = mount(h('div', null, select('s', ['a', 'b'], 'a')));
    $('s').value = 'b';
    morph(root, h('div', null, select('s', ['x', 'y', 'z'], 'z')));
    eq($('s').value, 'z', 'shown');
    eq($('s').options.length, 3, 'options');
  });

  test('a select with no selected option in the model shows its first', () => {
    const root = mount(h('div', null, select('s', ['a', 'b'], 'b')));
    $('s').value = 'b';
    morph(root, h('div', null, select('s', ['a', 'b'], null)));
    eq($('s').value, 'a', 'shown');
  });

  test('a multiple select shows the model’s set', () => {
    const multi = chosen => h('select', { id: 'm', multiple: true }, ['a', 'b', 'c'].map(v => h('option', { value: v, selected: chosen.includes(v) }, v)));
    const root = mount(h('div', null, multi(['a'])));
    $('m').options[1].selected = true;
    $('m').options[2].selected = true;
    morph(root, h('div', null, multi(['c'])));
    eq([...$('m').selectedOptions].map(o => o.value).join(','), 'c', 'shown');
  });

  test('a ticked box shows the model', () => {
    const box = on => h('input', { type: 'checkbox', id: 'c', checked: on });
    const root = mount(h('div', null, box(false)));
    $('c').checked = true;
    morph(root, h('div', null, box(false)));
    eq($('c').checked, false, 'after the model said no');
    morph(root, h('div', null, box(true)));
    eq($('c').checked, true, 'after the model said yes');
  });

  test('a radio group shows the model', () => {
    const radios = chosen => h('div', null, ['keep', 'reset'].map(v => h('input', { type: 'radio', name: 'r', id: 'r-' + v, value: v, checked: v === chosen })));
    const root = mount(h('div', null, radios('keep')));
    $('r-reset').checked = true;
    morph(root, h('div', null, radios('keep')));
    eq($('r-keep').checked, true, 'keep');
    eq($('r-reset').checked, false, 'reset');
  });

  test('a field typed in and left shows the model', () => {
    const input = v => h('input', { id: 'i', value: v });
    const root = mount(h('div', null, input('a')));
    $('i').value = 'typed';
    morph(root, h('div', null, input('model')));
    eq($('i').value, 'model', 'shown');
  });

  test('a field being typed in is left as it is', () => {
    const input = v => h('input', { id: 'i', value: v });
    const root = mount(h('div', null, input('a')));
    $('i').focus();
    $('i').value = 'half-typ';
    morph(root, h('div', null, input('a')));
    eq($('i').value, 'half-typ', 'shown');
    $('i').blur();
  });

  test('a text area edited and left shows the model', () => {
    const area = v => h('textarea', { id: 't', value: v }, v);
    const root = mount(h('div', null, area('{ }')));
    $('t').value = 'edited';
    morph(root, h('div', null, area('{ "id": 1 }')));
    eq($('t').value, '{ "id": 1 }', 'shown');
  });

  document.getElementById('result').textContent = lines.join('\n');
})();
