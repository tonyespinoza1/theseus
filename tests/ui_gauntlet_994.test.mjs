// Real Core answers through the actual page load/render path. This is a
// content oracle; it makes no native layout, gesture or timing claim.
import test from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { dom } from './fixtures/ui_dom.mjs';
import { PipelinePage } from '../web/pipeline.mjs';

const root = fileURLToPath(new URL('../', import.meta.url));
globalThis.HTMLInputElement = class {};
globalThis.HTMLTextAreaElement = class {};
const records = JSON.parse(execFileSync('/usr/bin/python3', ['-I', '-B',
  root + 'tests/fixtures/gauntlet_994_records.py', root], { encoding: 'utf8' }));
function all(node) { return [node, ...node.children.flatMap(all)]; }

for (const [state, pages] of Object.entries(records)) {
  test(`the real page keeps the Core classification on ${state}`, async () => {
    globalThis.document = dom();
    document.addEventListener = () => {};
    const host = document.createElement('main');
    const header = Object.fromEntries(['eyebrow', 'description', 'title'].map(key => [key, document.createElement('div')]));
    const page = new PipelinePage({ host, header,
      reader: 'agent-b', instanceId: 'test-instance', workspace: 'w1',
      read: async (view, args) => {
        assert.equal(view, 'pipeline');
        return structuredClone(pages[args.section]);
      }, send: () => { throw Error('a render never writes'); } });
    assert.equal(await page.load(), true);
    const owner = pages.now.release_piece ?? pages.now.pieces[0];
    const card = all(host).find(node => node['data-key'] === `piece:${owner.id}`);
    assert.ok(card, 'the real page paints the source or release owner');
    const notes = all(card).filter(node => node.className?.split(' ').includes('pl-judged'));
    assert.equal(notes.length, owner.freeze?.judged === true ? 1 : 0);
    if (notes.length) {
      assert.equal(notes[0].role, 'note');
      assert.match(notes[0].text, /Judged/);
    }
    if (pages.now.release_piece) {
      assert.equal(card['aria-label'], `Release ${owner.release_number}`);
      assert.ok(card.text.includes('Synthetic work 0'));
    }
  });
}
