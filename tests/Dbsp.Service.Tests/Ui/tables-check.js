/* Every list the admin UI draws, drawn from a live node and checked: each body row has as
   many cells as its table has headers (a colspan counts for what it spans), and each cell's
   data-label — what the phone layout shows beside its value — is its header's text. A
   table whose header and cells disagree shows every value after the gap under the wrong
   name; that happened twice before this check existed (the overview's and the tables
   list's Memory column). Loaded after the UI's own scripts by a copy of index.html; it
   visits each page in turn, waits for its data, and writes what it found to
   <pre id="result">: one line per table, "ok …" or "FAIL …", and "page …" lines for a
   page that failed to draw or showed no row where rows were expected. */
(() => {
  'use strict';
  const A = window.__admin;
  const lines = [];
  const errors = [];
  window.addEventListener('error', e => errors.push(String(e.message)));
  const consoleError = console.error.bind(console);
  console.error = (...args) => { errors.push(args.map(String).join(' ')); consoleError(...args); };

  const sleep = ms => new Promise(r => setTimeout(r, ms));
  const text = el => { const c = el.cloneNode(true); c.querySelectorAll('[aria-hidden="true"]').forEach(x => x.remove()); return c.textContent.replace(/\s+/g, ' ').trim(); };

  async function visit(hash) {
    location.hash = hash;
    for (let i = 0; i < 200; i++) {
      await sleep(50);
      const v = A.store.view;
      if (v && !v.loading && (v.data != null || v.error)) break;
    }
    await sleep(300); // a live tick or two, and the redraw they cause
    const v = A.store.view;
    if (!v || v.error) { lines.push(`page ${hash}: FAIL it did not load: ${v && v.error ? v.error.message : 'no view'}`); return 0; }
    if (/This view failed to draw/.test(document.getElementById('main').textContent))
      lines.push(`page ${hash}: FAIL the view failed to draw`);
    let rows = 0;
    document.querySelectorAll('#main table').forEach((table, n) => {
      const heads = [...table.querySelectorAll(':scope > thead > tr > th')];
      if (heads.length === 0) return; // a key/value list, not a list of things
      const names = heads.map(text);
      const name = `${hash} table ${n + 1} (${names.join(' | ')})`;
      const bad = [];
      for (const tr of table.querySelectorAll(':scope > tbody > tr')) {
        rows++;
        const cells = [...tr.children];
        const width = cells.reduce((a, c) => a + (Number(c.getAttribute('colspan')) || 1), 0);
        if (width !== heads.length) { bad.push(`a row has ${width} cells for ${heads.length} headers`); continue; }
        if (cells.length !== heads.length) continue; // a row that spans: grouped, nothing to label
        cells.forEach((c, i) => {
          const label = c.getAttribute('data-label');
          if (label == null) { if (table.classList.contains('cards')) bad.push(`cell ${i + 1} has no label`); }
          else if (label !== names[i]) bad.push(`cell ${i + 1} is labelled “${label}” under “${names[i]}”`);
        });
      }
      lines.push(bad.length ? `FAIL ${name}: ${[...new Set(bad)].join('; ')}` : `ok ${name}`);
    });
    return rows;
  }

  async function run() {
    for (let i = 0; i < 200 && !(A.store.node && A.store.info); i++) await sleep(50);
    const pipelines = await A.api('pipelines');
    const tables = await A.api('tables');
    const p = (pipelines.items.find(x => x.state === 'Running' || x.state === 'Built') || pipelines.items[0] || {}).id;
    const dead = (pipelines.items.find(x => x.deadLetters > 0) || {}).id;
    const t = (tables.items.find(x => x.readers > 0) || tables.items[0] || {}).name;
    const enc = encodeURIComponent;
    // The pages that list things, each of which has rows on a node with data.
    const pages = [
      ['#/', true], ['#/pipelines', true], ['#/tables', true], ['#/cluster', true], ['#/audit', A.store.node.role === 'Leader'],
      ['#/plugins', false], ['#/dead-letters', false],
      p && ['#/pipelines/' + enc(p), true], p && ['#/pipelines/' + enc(p) + '/topology', false], p && ['#/pipelines/' + enc(p) + '/faults', false],
      dead && ['#/dead-letters/' + enc(dead), true], dead && ['#/pipelines/' + enc(dead) + '/dead-letters', false],
      t && ['#/tables/' + enc(t), true],
    ].filter(Boolean);
    for (const [hash, needsRows] of pages) {
      const rows = await visit(hash);
      if (needsRows && rows === 0) lines.push(`page ${hash}: FAIL no table row on a page that lists things`);
    }
    for (const e of errors) lines.push(`page console: FAIL ${e}`);
    lines.push(`done ${A.store.node.role} ${pages.length} pages`);
    const pre = document.createElement('pre');
    pre.id = 'result';
    pre.textContent = lines.join('\n');
    document.body.append(pre);
  }

  run().catch(e => {
    const pre = document.createElement('pre');
    pre.id = 'result';
    pre.textContent = lines.concat('FAIL the check itself: ' + e.message).join('\n');
    document.body.append(pre);
  });
})();
