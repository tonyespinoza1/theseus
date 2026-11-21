import { literalLinkNodes } from './message-format.mjs';
// The pipeline page, an item's record, the steer menu, the one-line add at the top
// of each section with words and size set on the item in place, Change stages,
// and the rail's Pipeline row (item 84; docs/pipeline.md). Every word comes
// from pipeline-words.mjs; every fact from the `pipeline`, `pipeline_item` and
// `pipeline_row` views. The page reads its views again on every commit and
// keeps the reader's open groups, open menu and focus.
import { agentColor } from './author-colors.mjs';
import { isBusy, unconfirmed } from './api.mjs';
import { shell } from './in-place-navigation.mjs';
import { clock, datedTime, shownTime } from './times.mjs';
import { workNote } from './topic-state.mjs';
import { glyph as menuGlyph } from './glyphs.mjs';
import {
  BUSY_WORDS, EMPTY_WORDS, SIZE_WORDS, checkPipeline, duration, eventRuns, factBoxes, gatheringWords, glyphWords, heldWords, historyLine, listWords, missedWords,
  pieceTopicWords, placedWords, raisedWords, sentenceName, sourceRuns, stageLine, typicalCell, typicalWords, waitingWords, waitsWords,
} from './pipeline-words.mjs';

export const pipelineHref = (workspace, item) => '?' + new URLSearchParams(item === undefined ? { pipeline: workspace } : { pipeline: workspace, item: String(item) });
const topicHref = id => '?' + new URLSearchParams({ topic: id });
const messageHref = (topic, message) => '?' + new URLSearchParams({ topic, message });

// What is given to be shown: nothing for null, undefined or false, which a
// browser would otherwise print as the words "null" and "undefined".
const present = kid => kid !== null && kid !== undefined && kid !== false;

function el(tag, attrs, ...kids) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs ?? {})) {
    if (value === undefined || value === null || value === false) continue;
    if (key === 'class') node.className = value;
    else if (key === 'text') node.textContent = value;
    else if (key.startsWith('on')) node[key] = value;
    else node.setAttribute(key, value === true ? '' : value);
  }
  node.append(...kids.flat().filter(present));
  return node;
}
const time = (ms, now) => el('time', { datetime: new Date(ms).toISOString(), title: datedTime(ms), 'data-ms': String(ms), text: shownTime(ms, now) });
const judgedNote = piece => piece.freeze?.judged === true
  ? el('p', { class: 'pl-meta pl-judged', role: 'note', text: 'Judged · this change pays its own gauntlet' }) : null;

// A person or agent as the page names them: "you" in black, an agent in its colour.
function who(id, reader) {
  if (id === reader) return el('span', { class: 'pl-who you', text: 'you' });
  return el('span', { class: `pl-who agent-${safeColor(id)}`, text: id });
}
function safeColor(id) { try { return agentColor(id); } catch { return 'olive'; } }

const SECTION_ORDER = ['now', 'next', 'later', 'candidates'];

export class PipelinePage {
  constructor({ host, header, reader, instanceId, workspace, item = null, read, send }) {
    Object.assign(this, { host, header, reader, instanceId, workspace, itemNumber: item, read, send });
    this.open = new Set(); this.closed = new Set(); this.menu = null; this.confirming = null; this.asiding = null; this.errors = new Map();
    this.answer = null; this.serial = 0; this.editing = false; this.chain = Promise.resolve();
    // A busy Theseus (item 192): the Try again per place whose act it was too busy to
    // take; the read that did not go through, kept until a read is shown ({ key, busy }:
    // the place whose act that read followed, or null, and whether Theseus was too busy
    // for it or it failed another way); the act running; the load whose read is
    // shown (shownAt), and the load running when the page's last act saved (savedAt), so
    // a change saved after the read shown began is said to be saved and not shown yet.
    this.agains = new Map(); this.missed = null; this.busyPlaced = false; this.acting = null; this.shownAt = 0; this.savedAt = null;
    // Words saved from a field that no read has shown yet ("<stage>:<field>", "words:<number>",
    // "title"): the field shows them, never the words they replaced, until a read does.
    this.sent = new Map();
    // The titles sent from a one-line add that Theseus does not have yet, per line (lineAct).
    this.held = new Map(); this.heldSerial = 0;
    // The one-line add's text per section, the words being written per item, the
    // item whose words and size are open, what was just added, and the why being typed per piece being set aside.
    this.addDrafts = {}; this.wordDrafts = new Map(); this.details = null; this.added = null; this.savedWords = null; this.asideDrafts = new Map();
    // Change stages in place: what is being typed per stage line ("<stage>:<field>"),
    // the new stage's line, the quiet note per stage, and the stage just removed (for Undo).
    this.stageDrafts = new Map(); this.newStage = ''; this.stageNote = null; this.removed = null;
    // True while a repaint replaces the page: a field it takes away is not the person leaving it,
    // so its blur saves nothing (Chrome fires one; WebKit does not).
    this.painting = false;
    // The pipeline's own settings in place (the owner's): the title being typed, and the quiet note.
    this.titleDraft = null; this.titleField = null; this.settingsNote = null;
    host.addEventListener('toggle', event => {
      const key = event.target?.dataset?.key; if (!key) return;
      if (event.target.open) { this.open.add(key); this.closed.delete(key); } else { this.open.delete(key); this.closed.add(key); }
    }, true);
    host.addEventListener('keydown', event => this.keydown(event));
    document.addEventListener('click', event => { if (this.menu !== null && !event.target.closest?.('.pl-menu,.pl-steer')) { this.menu = null; this.paint(); } });
  }

  async readAll(section) {
    let cursor = 0, items = [], groups = [], first = null;
    for (;;) {
      const answer = checkPipeline(await this.read('pipeline', { workspace_id: this.workspace, section, ...(cursor ? { cursor } : {}), limit: 100 }), this.instanceId);
      first ??= answer;
      items = items.concat(answer.items ?? []); groups = groups.concat(answer.groups ?? []);
      if (answer.next_cursor === null || answer.next_cursor === undefined) break;
      cursor = answer.next_cursor;
    }
    return { ...first, items, groups };
  }

  // A load overtaken by a newer one (a live change arriving mid-read) settles when the newer one
  // has painted, so whoever awaits it (an act, then its focus) sees the page as it now stands.
  load() {
    this.latest = this.loadOnce(++this.serial);
    return this.latest;
  }

  // A read overtaken by a newer one, however it ends, settles as the newer one does and
  // says nothing of its own. A read that does not go through keeps the page as it was last
  // read, says so with a Try again (missedLine), in words for how it failed, and settles
  // false, so the commit follower reads again on the next commit: a read Theseus was too
  // busy for, a read made for a place (after an act there, or its Try again), and any read
  // while the page does not show a change it saved. Its own failure is never said under a
  // place, where it would read as the change's. Any other failure throws, as before.
  async loadOnce(serial) {
    let answer, failed = null;
    try { answer = this.itemNumber !== null ? await this.readRecord() : await this.readPage(); } catch (error) { failed = { error }; }
    if (serial !== this.serial) return this.latest;
    if (failed) {
      const busy = isBusy(failed.error);
      if (!busy && this.acting === null && !this.unshown()) throw failed.error;
      // It is said where the act it followed was made; a later read that fails, with no act of its own, leaves it where it was.
      this.missed = { key: this.acting ?? this.missed?.key ?? null, busy }; this.paint(); return false;
    }
    this.missed = null; this.shownAt = serial;
    if (!this.unshown()) this.sent.clear();
    if (this.itemNumber !== null) this.record = answer; else this.answer = answer;
    this.paint();
    return true;
  }

  async readRecord() {
    const answer = await this.read('pipeline_item', { workspace_id: this.workspace, number: this.itemNumber });
    if (answer?.error) throw Object.assign(Error(answer.error.message), { details: answer });
    return answer;
  }

  async readPage() {
    const head = await this.readAll('now');
    if (head.pipeline === null) return head;
    const [next, later, candidates, dropped, shipped] = await Promise.all(['next', 'later', 'candidates', 'dropped', 'shipped'].map(section => this.readAll(section)));
    return { ...head, sections: { now: head.items, next: next.items, later: later.items, candidates: candidates.items, dropped: dropped.items }, groups: shipped.groups };
  }

  // The page's last act saved after the read it shows began.
  unshown() { return this.savedAt !== null && this.savedAt >= this.shownAt; }

  // ---- painting ----------------------------------------------------------------

  paint() {
    const active = document.activeElement;
    const focused = active?.closest?.('[data-key]')?.dataset.key;
    const focusedRole = active?.dataset?.role;
    const caret = isField(active) ? [active.selectionStart, active.selectionEnd] : null;
    const now = clock.now();
    this.busyPlaced = false;
    this.painting = true;
    try {
      if (!this.shownAt) this.show([]);
      else if (this.itemNumber !== null) this.paintRecord(now);
      else this.paintPage(now);
    } finally { this.painting = false; }
    // The 98 rule: what had focus keeps it, so a reload never moves the reader.
    if (focused) {
      const row = this.host.querySelector(`[data-key="${CSS.escape(focused)}"]`);
      const target = row && (focusedRole ? row.querySelector(`[data-role="${CSS.escape(focusedRole)}"]`) : null) || row?.querySelector('a,button');
      target?.focus({ preventScroll: true });
      // What is being typed keeps its caret: a repaint never moves it.
      if (caret && isField(target)) try { target.setSelectionRange(...caret); } catch { /* a field with no caret */ }
    }
  }

  // With `inPlace` (the owner's pipeline), the title is a line edited where it stands.
  paintHeader(eyebrow, title, description, inPlace = null) {
    this.header.eyebrow.textContent = eyebrow; this.header.description.textContent = description;
    if (String(description ?? '').includes('](')) {
      const links = literalLinkNodes(description, { doc: this.header.description.ownerDocument ?? document });
      if (links.some(node => node.tagName === 'A')) this.header.description.replaceChildren(...links);
    }
    if (inPlace) this.titleInPlace(inPlace); else this.header.title.textContent = title;
    document.title = `${title} · Theseus`;
  }

  paintPage(now) {
    const answer = this.answer, pipeline = answer.pipeline;
    const page = [];
    if (!pipeline || !pipeline.enabled) {
      this.paintHeader(answer.workspace_title ?? '', pipeline?.title ?? 'Pipeline', 'This outcome’s pipeline is off.');
      page.push(this.turnOn(pipeline));
      this.show(page);
      return;
    }
    const sized = pipeline.sized;
    this.paintHeader(answer.workspace_title, pipeline.title, sized
      ? `Every feature and fix by number. As of ${shownTime(now, now)}, from what Theseus has seen happen.`
      : `Every ${pipeline.unit_one} being prepared, by stage. As of ${shownTime(now, now)}, from what Theseus has seen happen.`, pipeline.you_own ? pipeline : null);
    if (pipeline.you_own) page.push(this.settingsLine(pipeline));
    const sections = answer.sections, any = SECTION_ORDER.some(name => sections[name].length) || answer.groups.some(group => group.items.length);
    if (answer.waiting?.length) {
      page.push(el('section', { class: 'waiting-on-you', 'aria-label': 'Waiting on you' }, el('h2', { text: `Waiting on you (${answer.waiting.length})` }),
        el('ul', { class: 'waiting-list' }, ...answer.waiting.map(row => el('li', {}, el('a', { href: topicHref(row.topic_id), text: row.topic_title }),
          el('span', { class: 'muted', text: waitingWords(row, now) }))))));
    }
    if (answer.banner) {
      const banner = answer.banner;
      page.push(el('div', { class: 'pl-banner', role: 'status' }, el('strong', { text: `Release ${banner.release} is live, deployed ${shownTime(banner.deployed_ms, now)}.` }),
        el('span', { class: 'muted', text: banner.items.length ? `It shipped ${listWords(banner.items)}. They moved to Shipped from the deploy’s release record.` : 'Its record names no items.' })));
    }
    page.push(el('div', { class: 'pl-facts' }, ...factBoxes(answer, now).map(([label, value, sub]) =>
      el('div', { class: 'pl-fact-box' }, el('span', { class: 'label', text: label }), el('span', { class: 'value', text: value }), sub ? el('span', { class: 'muted', text: sub }) : null))));
    if (!any) page.push(el('div', { class: 'pl-empty' }, el('p', {}, el('strong', { text: EMPTY_WORDS[0] })), el('p', { text: EMPTY_WORDS[1] }), el('p', { class: 'muted', text: EMPTY_WORDS[2] })));
    const words = pipeline.section_words ?? {};
    const byNumber = new Map(SECTION_ORDER.flatMap(name => sections[name]).map(item => [item.number, item]));
    if (answer.release_piece) {
      const piece = answer.release_piece;
      page.push(section('Next release', null, el('article', { class: 'pl-item', 'aria-label': `Release ${piece.release_number}`, 'data-key': `piece:${piece.id}` },
        judgedNote(piece),
        el('div', { class: 'pl-head' }, glyph('building'), el('span', { class: 'pl-title', text: `Release ${piece.release_number}, being put together` })),
        el('p', { class: 'pl-meta', text: `${piece.items.length} item${piece.items.length === 1 ? '' : 's'}, combined from their approved pieces:` }),
        el('ul', { class: 'pl-meta pl-release-items' }, ...piece.items.map(number => el('li', {}, el('span', { class: 'pl-num', text: String(number) }), ' ',
          el('a', { href: pipelineHref(this.workspace, number), text: piece.titles?.[String(number)] ?? byNumber.get(number)?.title ?? '' }),
          ...(byNumber.get(number) ? this.origin(byNumber.get(number)) : [])))),
        this.stageBox(piece, pipeline, now, false))));
    }
    // Now: the pieces being built, then what is planned and not started.
    const pieces = answer.pieces ?? [];
    const inPiece = new Set(pieces.flatMap(piece => piece.items).concat(answer.release_piece?.items ?? []));
    const nowRows = sections.now.filter(item => !inPiece.has(item.number));
    // Each section opens with its one line: a title and Return adds an item there.
    page.push(section(words.now ?? 'Now', sized ? 'Being built, and what is planned for this release.' : 'Being prepared, soonest first.',
      this.addLine(pipeline, 'now'),
      ...pieces.map((piece, index) => this.pieceCard(piece, byNumber, pipeline, now, index === 0)),
      nowRows.length && pieces.length ? el('p', { class: 'muted pl-planned', text: 'Planned for this release, not started yet' }) : null,
      nowRows.length ? this.rows(nowRows, pipeline, now) : null));
    const coordinator = pipeline.coordinator ? sentenceName(pipeline.coordinator === this.reader ? 'you' : pipeline.coordinator) : 'The owner';
    page.push(section(words.next ?? 'Next', sized ? `The release after Now. ${coordinator} order${pipeline.coordinator === this.reader ? '' : 's'} it; steer with ⋯.` : 'Not started; steer with ⋯.',
      this.addLine(pipeline, 'next'), sections.next.length ? this.rows(sections.next, pipeline, now) : null));
    page.push(section(words.later ?? 'Later', null, this.addLine(pipeline, 'later'), sections.later.length ? this.rows(sections.later, pipeline, now) : null));
    const groups = [...new Set(sections.candidates.map(item => item.candidate_group))];
    page.push(section(words.candidates ?? 'Candidates', 'Not ordered yet.', this.addLine(pipeline, 'candidates'), ...groups.map(group => {
      const items = sections.candidates.filter(item => item.candidate_group === group);
      const key = `group:${group}`;
      const asked = items.some(item => item.glyph === 'decide');
      return el('details', { class: 'pl-group', 'data-key': key, open: this.open.has(key) || (asked && !this.closed.has(key)) },
        el('summary', {}, sentenceName(group || 'Ungrouped'), el('span', { class: 'muted', text: ` · ${items.length}` })), this.rows(items, pipeline, now));
    })));
    page.push(this.stagesSection(pipeline));
    page.push(this.timesSection(answer, pipeline));
    page.push(this.shippedSection(answer, pipeline, now));
    if (sections.dropped.length) page.push(section('Dropped', 'Kept with its number, so it can come back.', this.rows(sections.dropped, pipeline, now, { dropped: true })));
    this.show(page);
  }

  // The page as built, leaving out each part with nothing to show (a section with no
  // rows is null), and first, a busy read's line when no place on the page said it.
  show(page) {
    const kids = page.filter(present);
    if (this.missed && !this.busyPlaced) kids.unshift(this.missedLine(null));
    this.host.replaceChildren(...kids);
  }

  pieceCard(piece, byNumber, pipeline, now, first) {
    const card = el('article', { class: 'pl-item', 'data-key': `piece:${piece.id}`, 'aria-label': `Item${piece.items.length > 1 ? 's' : ''} ${listWords(piece.items)}` }, judgedNote(piece));
    piece.items.forEach((number, index) => {
      const known = byNumber.get(number);
      const item = known ?? { number, title: piece.titles?.[String(number)] ?? '', glyph: 'building', sources: null, raised_by: null, raised_note: '', size: null, asks: [] };
      const ring = index === 0 && piece.topic?.work?.observed
        ? el('span', { class: 'topic-work', 'aria-hidden': 'true', title: workNote(piece.topic.work, this.reader) }) : null;
      // Each item being built keeps its own ⋯ (9.1.3, D3): Open its record, Set aside to Later… (183) when the server would
      // take it from this reader, and Drop…, which asks first.
      const key = `item:${number}`;
      const lines = el('div', { class: 'pl-piece-item', 'data-key': key, 'data-number': String(number) },
        this.head(item, item.glyph === 'decide' ? 'decide' : 'building', ring), this.meta(item, pipeline, now), ...this.origin(item));
      if (this.confirming === number) lines.append(this.dropConfirm(item));
      if (this.asiding === number && piece.you_set_aside) lines.append(this.asideLine(item, piece));
      lines.append(...this.said(key));
      if (known && pipeline.you_steer) lines.append(...this.steer(item, [item], 0, pipeline, { underway: true, piece }));
      card.append(lines);
    });
    if (piece.topic) {
      const words = pieceTopicWords(piece, now);
      card.append(el('p', { class: 'pl-meta' }, 'Topic: ', el('a', { href: topicHref(piece.topic.id), text: piece.topic.title }), words.together, words.activity));
    }
    card.append(this.stageBox(piece, pipeline, now, first));
    return card;
  }

  stageBox(piece, pipeline, now, openHistory) {
    const box = el('div', { class: 'pl-stagebox' });
    const stage = piece.stage;
    if (!stage) return box;
    // An ask that holds this piece or release, until it is answered (item 259): who it waits on and its first line, linking to it.
    for (const held of Array.isArray(piece.held) ? piece.held : []) {
      const words = heldWords(held, this.reader);
      box.append(el('p', { class: 'pl-held', 'data-message': held.message_id }, el('strong', { text: words.lead }), ' — ',
        el('a', { href: messageHref(held.topic_id, held.message_id), text: words.line })));
    }
    const chips = [
      ...piece.stages_so_far.map(entry => ['done', `${entry.name} · ${duration(entry.left_ms - entry.entered_ms)}`, `${datedTime(entry.entered_ms)} to ${datedTime(entry.left_ms)}`]),
      ['now', stage.name, `Now, since ${datedTime(stage.entered_ms)}`],
      ...piece.ahead.map(name => ['ahead', name, 'Not reached yet']),
    ];
    box.append(el('ol', { class: 'pl-stages', 'aria-label': 'Stages' }, ...chips.map(([kind, words, title], index) => el('li', {},
      el('span', { class: `pl-chip ${kind}`, title, 'aria-current': kind === 'now' ? 'step' : null, text: words }),
      index < chips.length - 1 ? el('span', { class: 'pl-sep', 'aria-hidden': 'true', text: '›' }) : null))));
    const line = stageLine(stage, now);
    const current = el('p', { class: 'pl-current' }, el('strong', { text: line.name }), ` ${line.since} · ${line.soFar}`);
    if (stage.note) current.append(el('span', { class: 'muted', text: stage.note }));
    if (stage.owner_gate) {
      if (stage.you_leave_it) {
        current.append(el('span', { class: 'pl-gate-you', text: `Waiting on you: ${stage.gate_words || 'say when this stage is done.'}` }));
        const next = piece.ahead_ids?.[0];
        current.append(el('span', {}, el('button', { type: 'button', 'data-role': 'leave', text: 'Done with this stage',
          onclick: () => this.act(`piece:${piece.id}`, () => next
            ? this.command('pipeline.stage_enter', { piece_id: piece.id }, { stage_id: next, round: 1, left_outcome: 'passed' }, { piece_revision: piece.version })
            : this.command('pipeline.piece_finish', { piece_id: piece.id }, {}, { piece_revision: piece.version })) })));
      } else current.append(el('span', { class: 'muted', text: `Waiting on ${pipeline.owner}` }));
    } else if (stage.gate_words) current.append(el('span', { class: 'muted', text: stage.gate_words }));
    if (stage.gate_checks?.length) current.append(el('span', { class: 'muted', text: `Checked by: ${listWords(stage.gate_checks)}. Checks become records with the outcome's checks; until then the gate is shown, not enforced.` }));
    const typical = typicalWords(stage.typical, { size: piece.size, unitMany: pipeline.unit_many });
    if (typical) current.append(el('span', { class: 'muted', text: typical }));
    box.append(current);
    if (piece.stages_so_far.length) {
      const key = `history:${piece.id}`;
      box.append(el('details', { class: 'pl-history', 'data-key': key, open: this.open.has(key) || (openHistory && !this.closed.has(key)) },
        el('summary', { text: 'Stages so far' }),
        el('ol', {}, ...piece.stages_so_far.map(entry => el('li', { text: historyLine(entry, now) })), el('li', { text: `${stage.name}: since ${shownTime(stage.entered_ms, now)}` }))));
    }
    box.append(...this.said(`piece:${piece.id}`));
    return box;
  }

  head(item, glyphKind, extra) {
    const words = el('span', { class: 'pl-words' }, el('a', { class: 'pl-title', href: pipelineHref(this.workspace, item.number), title: item.description || null, text: item.title }));
    if (glyphKind === 'decide' || item.glyph === 'decide') words.append(el('span', { class: 'topic-state mine', 'data-state': 'waiting', text: 'Waiting on you' }));
    if (extra) words.append('⁠', extra);
    return el('div', { class: 'pl-head' }, glyph(glyphKind ?? item.glyph), el('span', { class: 'pl-num', text: String(item.number) }), words);
  }

  meta(item, pipeline, now, more = []) {
    const raised = raisedWords(item);
    const parts = [];
    if (pipeline.sized && item.size) parts.push(`${SIZE_WORDS[item.size]} · `);
    parts.push(raised.prefix);
    if (raised.who) parts.push(who(raised.who, this.reader));
    return el('p', { class: 'pl-meta' }, ...parts, waitsWords(item.needs), ...more);
  }

  origin(item) {
    if (!item.sources) return [];
    const runs = sourceRuns(item.sources, this.reader);
    const linked = runs.parts.map(part => part.message ? el('a', { href: topicHref(part.message.topic_id), title: `Open ${part.message.topic_title}`, text: part.text }) : part.text);
    const nodes = [];
    if (!runs.parts.length) return nodes;
    if (!runs.folds) nodes.push(el('p', { class: 'pl-origin' }, el('span', { class: 'sr-only', text: 'Where this came from: ' }), 'From ', ...linked));
    else {
      const key = `origin:${item.number}`;
      nodes.push(el('details', { class: 'pl-origin', 'data-key': key, open: this.open.has(key) },
        el('summary', {}, el('span', { class: 'sr-only', text: 'Where this came from: ' }), runs.summary),
        el('p', {}, el('strong', { text: 'Where this came from: ' }), ...linked, runs.more > 0 ? ` and ${runs.more} more in its record` : '')));
    }
    const gathering = gatheringWords(item.gathering, this.reader);
    if (gathering) nodes.push(el('p', { class: 'pl-origin pl-maturing' }, gathering.lead,
      gathering.link ? (item.gathering.topic_id ? el('a', { href: topicHref(item.gathering.topic_id), text: gathering.link }) : gathering.link) : null, gathering.mine ? ' (you)' : '', gathering.after));
    return nodes;
  }

  rows(items, pipeline, now, { dropped = false } = {}) {
    return el('ul', { class: 'pl-rows' + (dropped ? ' pl-dropped' : '') }, ...items.map((item, index) => this.row(item, items, index, pipeline, now)));
  }

  row(item, list, index, pipeline, now) {
    const key = `item:${item.number}`;
    const placed = placedWords(item.placed, this.reader, pipeline.section_words, now, item.number);
    const moved = item.placed?.kind === 'moved' && !item.placed.undo_of && item.section !== 'dropped' && (now - item.placed.ms) < 24 * 3600 * 1000;
    const li = el('li', { class: 'pl-row' + (moved ? ' moved' : ''), 'data-key': key, 'data-number': String(item.number) });
    const more = [];
    for (const ask of item.asks ?? []) more.push(` · ${ask.by} asked you, since ${shownTime(ask.since_ms, now)}`);
    const stageWords = item.stage ? [' · ', `${item.stage.name} since ${shownTime(item.stage.entered_ms, now)}`] : [];
    const body = el('div', { class: 'pl-body' }, this.head(item), this.meta(item, pipeline, now, [...more, ...stageWords]), ...this.origin(item));
    for (const ask of item.asks ?? []) body.append(el('p', { class: 'pl-note' }, `“${ask.excerpt}” In `, el('a', { href: topicHref(ask.topic_id), text: ask.topic_title }), '.'));
    // A move shows for a week; why a piece was set aside shows for as long as the item stays where it was put.
    if (placed && ((now - item.placed.ms) < 7 * 24 * 3600 * 1000 || item.placed.set_aside)) {
      const undo = item.placed.by === this.reader && !item.placed.undo_of
        ? el('button', { type: 'button', 'data-role': 'undo', text: item.section === 'dropped' ? 'Restore' : 'Undo',
          onclick: () => this.act(key, () => this.command('pipeline.item_move', { workspace_id: this.workspace, number: item.number }, { undo_of: item.placed.event }, { item_revision: item.version })) })
        : null;
      body.append(el('p', { class: 'pl-note' }, placed, undo));
    } else if (item.section === 'dropped' && item.dropped && pipeline.you_steer) {
      body.append(el('p', { class: 'pl-note' }, el('button', { type: 'button', 'data-role': 'undo', text: 'Restore',
        onclick: () => this.act(key, () => this.command('pipeline.item_move', { workspace_id: this.workspace, number: item.number }, { undo_of: item.dropped.event }, { item_revision: item.version })) })));
    }
    if (this.confirming === item.number) body.append(this.dropConfirm(item));
    const editable = item.section !== 'dropped' && item.glyph !== 'shipped';
    if (editable && this.details === item.number) body.append(this.detailsPanel(item, pipeline));
    body.append(...this.said(key));
    li.append(body);
    if (editable) li.append(el('button', { type: 'button', class: 'pl-edit', 'data-role': 'details', 'aria-expanded': this.details === item.number ? 'true' : 'false',
      'aria-label': `${this.details === item.number ? 'Close details' : 'Details'} of ${item.number}`, text: this.details === item.number ? 'Close' : 'Details', onclick: () => this.toggleDetails(item.number) }));
    if (pipeline.you_steer && item.section !== 'dropped' && item.glyph !== 'shipped') li.append(...this.steer(item, list, index, pipeline));
    return li;
  }

  // Drop… on an item being built asks first (D3): Drop it, or Keep it, which changes nothing.
  dropConfirm(item) {
    const key = `item:${item.number}`;
    return el('p', { class: 'pl-confirm', role: 'alert' }, `Drop ${item.number}? It is being built now, so this stops work under way. `,
      el('button', { type: 'button', 'data-role': 'drop-confirm', text: 'Drop it', onclick: () => { this.confirming = null; this.act(key, () => this.move(item, { section: 'dropped', underway_confirmed: true })); } }),
      el('button', { type: 'button', 'data-role': 'drop-keep', text: 'Keep it', onclick: () => { this.confirming = null; this.paint(); this.host.querySelector(`[data-key="${key}"] [data-role="steer"]`)?.focus(); } }));
  }

  // Set aside to Later… asks why in one line (the form rule): Return sets the whole piece aside, Escape keeps it as it is.
  asideLine(item, piece) {
    const key = `item:${item.number}`, one = piece.items.length === 1, later = this.answer?.pipeline?.section_words?.later ?? 'Later';
    // The why being typed is kept per piece, as the add lines keep theirs, so a live repaint leaves its words alone;
    // a piece set aside is closed for good, so its words are never offered for another.
    const close = () => { this.asiding = null; this.paint(); this.host.querySelector(`[data-key="${key}"] [data-role="steer"]`)?.focus(); };
    return titleLine({ key: `aside:${item.number}`, role: 'aside-note', label: `Why set ${listWords(piece.items)} aside to ${later}`, placeholder: 'Why set it aside?',
      hint: `Return sets ${listWords(piece.items)} aside to ${later}, with ${one ? 'its' : 'their'} stages so far · Escape keeps ${one ? 'it' : 'them'}`,
      value: this.asideDrafts.get(piece.id) ?? '', input: value => { this.asideDrafts.set(piece.id, value); },
      submit: note => { this.asiding = null; this.act(key, () => this.move(item, { section: 'later', note })); }, cancel: close });
  }

  // The ⋯ button and, while it is open, its menu.
  steer(item, list, index, pipeline, { underway = false, piece = null } = {}) {
    const key = `item:${item.number}`;
    const button = el('button', { type: 'button', class: 'pl-steer', 'data-role': 'steer', 'aria-label': `Steer ${item.number}`, 'aria-haspopup': 'menu',
      'aria-expanded': this.menu === item.number ? 'true' : 'false', text: '⋯',
      onclick: () => { this.menu = this.menu === item.number ? null : item.number; this.paint(); this.host.querySelector(`[data-key="${key}"] .pl-menu button`)?.focus(); } });
    return this.menu === item.number ? [button, this.steerMenu(item, list, index, pipeline, underway, piece)] : [button];
  }

  // An item being built (`underway`) is not ordered or moved from here: its menu is Open its record, Set aside to Later…
  // when its piece's view says this reader may (`you_set_aside`, the server's own rule), and Drop….
  steerMenu(item, list, index, pipeline, underway = false, piece = null) {
    const words = pipeline.section_words ?? {};
    // Each item's glyph (glyphs.mjs, item 602) follows its words and is drawn before them (aim.css).
    const button = (label, keys, name, action) => el('button', { type: 'button', role: 'menuitem', onclick: () => { this.menu = null; action(); } },
      el('span', { text: label }), menuGlyph(name), keys ? el('kbd', { text: keys }) : null);
    const key = `item:${item.number}`;
    const up = index > 0 ? list[index - 1] : null, down = index < list.length - 1 ? list[index + 1] : null;
    const menu = el('div', { class: 'pl-menu', role: 'menu', 'aria-label': `Steer ${item.number}` });
    if (!underway) {
      if (up) menu.append(button('Move up', '⌘↑', 'up', () => this.act(key, () => this.move(item, { above: up.number }))));
      if (down) menu.append(button('Move down', '⌘↓', 'down', () => this.act(key, () => this.move(item, { below: down.number }))));
      menu.append(el('hr'));
      for (const name of SECTION_ORDER) if (name !== item.section) menu.append(button(`Move to ${words[name] ?? name}`, null, 'moveto', () => this.act(key, () => this.move(item, { section: name }))));
      menu.append(el('hr'));
    }
    menu.append(button('Open its record', null, 'openpage', () => shell.leave(pipelineHref(this.workspace, item.number))));
    if (underway && piece?.you_set_aside) menu.append(button(`Set aside to ${words.later ?? 'Later'}…`, null, 'moveto', () => {
      this.asiding = item.number; this.paint(); this.host.querySelector(`[data-key="item:${item.number}"] [data-role="aside-note"]`)?.focus();
    }));
    menu.append(button('Drop…', null, 'drop', () => {
      if (item.stage) { this.confirming = item.number; this.paint(); this.host.querySelector(`[data-key="${key}"] [data-role="drop-keep"]`)?.focus(); }
      else this.act(key, () => this.move(item, { section: 'dropped' }));
    }));
    menu.addEventListener('keydown', event => {
      if (event.key === 'Escape') { this.menu = null; this.paint(); this.host.querySelector(`[data-key="${key}"] .pl-steer`)?.focus(); }
      if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
        const buttons = [...menu.querySelectorAll('button')], at = buttons.indexOf(document.activeElement);
        buttons[(at + (event.key === 'ArrowDown' ? 1 : buttons.length - 1)) % buttons.length]?.focus(); event.preventDefault();
      }
    });
    return menu;
  }

  keydown(event) {
    if (!(event.metaKey || event.ctrlKey) || (event.key !== 'ArrowUp' && event.key !== 'ArrowDown')) return;
    const row = event.target.closest?.('.pl-row'); if (!row || !this.answer?.pipeline?.you_steer) return;
    const list = [...row.parentElement.children], index = list.indexOf(row);
    const other = list[index + (event.key === 'ArrowUp' ? -1 : 1)];
    if (!other) return;
    event.preventDefault();
    const number = Number(row.dataset.number), item = this.findItem(number);
    if (!item) return;
    this.act(`item:${number}`, () => this.move(item, event.key === 'ArrowUp' ? { above: Number(other.dataset.number) } : { below: Number(other.dataset.number) }));
  }

  findItem(number) {
    for (const items of Object.values(this.answer?.sections ?? {})) { const found = items.find(item => item.number === number); if (found) return found; }
    return null;
  }

  move(item, args) {
    if (args.section === 'candidates' && item.section !== 'candidates') args.candidate_group = '';
    return this.command('pipeline.item_move', { workspace_id: this.workspace, number: item.number }, args, { item_revision: item.version });
  }

  // The item as this page last read it: a row of the page, or the record's own item.
  current(number) {
    return this.itemNumber !== null ? (this.record?.item?.number === number ? this.record.item : null) : this.findItem(number);
  }

  // An edit on the item as last read; the acts run one after another, so each reads the revision the last one left.
  edit(number, args) {
    const item = this.current(number);
    if (!item) throw Error(`${number} moved since this page read it. It is shown where it is now.`);
    return this.command('pipeline.item_edit', { workspace_id: this.workspace, number }, args, { item_revision: item.version });
  }

  command(kind, target, args, expected = {}) { return this.sendChange(this.change(kind, target, args, expected)); }

  // A change, made once with its own operation; `saved` hears its result once it is saved.
  change(kind, target, args, expected = {}, saved = () => {}) {
    return { kind, target, args, expected, operation_id: crypto.randomUUID(), unconfirmed: false, saved };
  }

  // Sends a change as it was made. An attempt that ends with no word on whether Core
  // recorded it (api.mjs unconfirmed) leaves it unconfirmed until Core's recorded
  // answer to it comes; until then, what sends it again sends it so, and Core answers
  // with what it recorded, or makes it then ("A retry keeps the same operation identity").
  async sendChange(change) {
    let receipt;
    try { receipt = await this.send(change.kind, change.target, change.args, change.expected, change.operation_id); }
    catch (error) { if (unconfirmed(error)) change.unconfirmed = true; throw error; }
    if (receipt?.status === 'committed' || receipt?.recorded === true) change.unconfirmed = false;
    if (receipt?.status !== 'committed') throw Object.assign(Error(receipt?.error?.message ?? 'Theseus did not confirm this change.'), { details: receipt });
    this.savedAt = this.serial;
    return receipt.result;
  }

  // An act from the page: its refusal shows in plain words under the row and keeps focus there.
  // Acts run one at a time. `role` names the control that takes focus after; false leaves
  // focus where the reader put it (the repaint keeps it). An act Theseus was too busy to
  // take saved nothing: its row says so, with a Try again that does it again. While the
  // page does not show its last saved change, a change made on it would carry a revision
  // Theseus has moved past, so the page reads first; when that read does not go through,
  // the act is not sent and says so, with a Try again that reads first again. The read
  // after an act is the page's (loadOnce): one that does not go through says so in words
  // of its own, and a change saved stays said as saved.
  // However it ends without a change saved (not sent, refused, too busy, or its answer
  // lost), `held` (lineAct), when given, hears it instead, before the page paints: { words, again }.
  act(key, action, role = null, held = null) {
    const run = async () => {
      this.errors.delete(key); this.agains.delete(key);
      // Not saved: not sent, as the read first did not go through (no error), or the failure that ended it.
      const unsaved = (error = null) => {
        const again = error === null || isBusy(error);
        const words = error === null ? missedWords(this.missed?.busy ?? true).notSaved : again ? BUSY_WORDS.notSaved : error.message;
        if (held) return held({ words, again });
        this.errors.set(key, words);
        if (again) this.agains.set(key, () => this.act(key, action, role));
      };
      try {
        if (this.unshown() && await this.load() === false) { unsaved(); this.paint(); }
        else {
          this.acting = key;
          // A refusal says why under its row, and the page reads again, so what it shows is how things stand now.
          // That read is the page's (loadOnce): however it fails, it never says anything of the act's.
          try { await action(); } catch (error) { unsaved(error); }
          try { await this.load(); } catch { this.paint(); }
        }
      } catch (error) { unsaved(error); this.paint(); } finally { this.acting = null; }
      if (role === false) return;
      const scope = `[data-key="${CSS.escape(key)}"]`;
      const target = (role ? this.host.querySelector(`${scope} [data-role="${CSS.escape(role)}"]`) : null) ?? this.host.querySelector(`${scope} a, ${scope} button`);
      target?.focus({ preventScroll: true });
    };
    this.chain = this.chain.then(run, run);
    return this.chain;
  }

  // A one-line add (the form rule): the line clears at once, so the next title can be
  // typed while this one is added, and only the person writes on it after. The title
  // leaves the line for one place of its own until Theseus has it, held under the line:
  // unseen while it is sent; whenever it is not added (act), shown on a line of its own
  // (heldLines) with why, where it can be changed, and where Try again or Return adds the
  // words it shows then. `make(title)` makes the change for those words (change), at its
  // turn. A change whose answer was lost may have been made: it stays as it was sent, and
  // Try again or Return sends that same change until Core answers it (sendChange).
  lineAct(key, title, clear, make) {
    clear();
    const entry = { id: ++this.heldSerial, text: title, make, change: null, said: null };
    this.held.set(key, [...(this.held.get(key) ?? []), entry]);
    return this.sendHeld(key, entry);
  }

  sendHeld(key, entry) {
    const title = entry.text.trim();
    entry.said = null;
    if (!title) { this.letGo(key, entry); return this.chain; }
    this.paint();
    return this.act(key, async () => {
      if (!entry.change?.unconfirmed) entry.change = entry.make(title);
      entry.change.saved(await this.sendChange(entry.change)); this.letGo(key, entry);
    }, false, said => { entry.said = said; })
      .then(() => {
        // Focus was on what the add took away (the Try again, or the held line itself): it goes back to the title, or the line.
        if (this.host.contains?.(document.activeElement)) return;
        const kept = this.held.get(key)?.includes(entry) ? `held:${key}:${entry.id}` : key;
        this.host.querySelector(`[data-key="${CSS.escape(kept)}"] input`)?.focus({ preventScroll: true });
      });
  }

  // A held title let go: added, emptied, or the person's Escape.
  letGo(key, entry) {
    const rest = (this.held.get(key) ?? []).filter(other => other !== entry);
    if (rest.length) this.held.set(key, rest); else this.held.delete(key);
    this.paint();
  }

  // The titles held under a one-line add that are not added, each a line of its own,
  // with why and, when Theseus was busy, Try again. A title whose answer was lost is
  // not known to be saved: it says so, with Try again, and stays as it was sent.
  // Escape lets it go.
  heldLines(key, label, placeholder) {
    return (this.held.get(key) ?? []).filter(entry => entry.said).map(entry => {
      const unknown = entry.change?.unconfirmed === true;
      const line = titleLine({ key: `held:${key}:${entry.id}`, value: entry.text, role: 'held-title', readonly: unknown, placeholder,
        label: `${label}, ${unknown ? 'not known if saved' : 'not saved'}`, hint: `Return ${unknown ? 'sends it again' : 'adds it'} · Escape lets it go`,
        input: value => { entry.text = value; }, submit: () => this.sendHeld(key, entry),
        cancel: () => { this.letGo(key, entry); this.host.querySelector(`[data-key="${CSS.escape(key)}"] input`)?.focus(); } });
      line.append(notSaved(unknown ? BUSY_WORDS.unconfirmed : entry.said.words, this.heldAgain(entry) ? () => this.sendHeld(key, entry) : null, { className: 'pl-line-note' }));
      return line;
    });
  }

  // Whether a title held offers Try again: when Theseus was too busy for it, or for the read before it,
  // or that read failed, or its answer was lost.
  heldAgain(entry) { return entry.said?.again || entry.change?.unconfirmed === true; }

  // Whether a place offers a Try again for a change made there: its act's, or a title held there.
  againAt(key) {
    return this.agains.has(key) || (this.held.get(key) ?? []).some(entry => entry.said && this.heldAgain(entry));
  }

  // What the page says at a place, in the order it happened: that the change before
  // was saved and is not shown yet, when the read after it did not go through (missedLine);
  // why its act was refused, with Try again when Theseus was too busy to take it; and,
  // when the read after that act did not go through, that the page may be behind (missedLine).
  // Beside an act's own Try again, which reads the page again too, the busy read's line
  // has none of its own and says only that the change before is saved.
  said(key, { tag = 'p', className = '', lead = '' } = {}) {
    const error = this.errors.get(key), nodes = [];
    if (this.missed !== null && this.missed.key === key) this.busyPlaced = true;
    const busy = !this.missedSaidAt(key) ? null : this.againAt(key)
      ? el(tag, { class: `pl-busy ${className}`.trim(), role: 'status' }, lead + missedWords(this.missed.busy).savedNotShown) : this.missedLine(key, { tag, className, lead });
    if (busy && this.unshown()) nodes.push(busy);
    if (error) nodes.push(notSaved(error, this.agains.get(key), { tag, className, lead }));
    if (busy && !this.unshown()) nodes.push(busy);
    return nodes;
  }

  // Whether the line of a read that did not go through is said at a place: always, unless a
  // change made there has a Try again of its own; then only to say that the change before it there is saved.
  missedSaidAt(key) {
    return this.missed !== null && this.missed.key === key && (!this.againAt(key) || this.unshown());
  }

  // A read that did not go through, in plain words for how (missedWords), with a Try again
  // that reads the page again in its turn with the acts. Without a place (key null) it heads the page.
  missedLine(key, { tag = 'p', className = '', lead = '' } = {}) {
    const said = missedWords(this.missed.busy), words = !this.shownAt ? said.unread : this.unshown() ? said.savedNotShown : said.behind;
    return el(tag, { class: `pl-busy ${className}`.trim(), role: 'status', 'data-key': key === null ? 'read-again' : null }, lead + words, ' ',
      tryAgain(() => this.readAgain(key ?? 'read-again')));
  }

  // Try again of a read: the page reads again, in its turn with the acts; one that does
  // not go through says so where it was (loadOnce). Focus, which was on the Try again
  // just removed, goes back into its place.
  readAgain(key) {
    const run = async () => {
      this.acting = key;
      try { await this.load(); } finally { this.acting = null; }
    };
    this.chain = this.chain.then(run, run);
    return this.chain.then(() => {
      const place = this.host.querySelector(`[data-key="${CSS.escape(key)}"]`);
      if (place && !place.contains?.(document.activeElement)) place.querySelector('input, textarea, button, a')?.focus({ preventScroll: true });
    });
  }

  stagesSection(pipeline) {
    const steer = pipeline.you_steer, editing = steer && this.editing;
    const list = editing ? this.stageLines(pipeline) : el('ol', { class: 'pl-stagedefs' }, ...pipeline.stages.map((stage, index) => el('li', {},
      el('span', { class: 'pl-num', text: String(index + 1) }),
      el('div', {}, el('p', {}, el('strong', { text: stage.name }), stage.done_when ? `: ${stage.done_when}` : ''),
        el('p', { class: 'muted', text: [gateWords(stage, pipeline.owner), stage.added_note].filter(Boolean).join(' ') })))));
    const foot = el('p', { class: 'muted pl-stagedefs-foot' }, 'A change to the stages applies to items that enter a stage after it; past times stay with the stage they were measured in. ',
      steer ? el('button', { type: 'button', 'data-role': 'edit-stages', 'aria-expanded': editing ? 'true' : 'false', text: editing ? 'Done' : 'Change stages',
        onclick: () => { this.editing = !this.editing; this.removed = null; this.stageNote = null; this.paint(); } }) : null);
    const key = 'stages';
    const details = el('details', { class: 'pl-group pl-stages-group', 'data-key': key, open: this.open.has(key) || editing || (!pipeline.ships_in_releases && !this.closed.has(key)) },
      el('summary', {}, 'Stages', el('span', { class: 'muted', text: ` · ${pipeline.stages.length}` })),
      el('p', { class: 'muted', text: editing ? 'Each change saves as you make it: a name or line on Return or when you click away, a chip when you tap it.' : 'This outcome’s own stages, in order. Every item passes through each one.' }),
      list, foot);
    return el('section', { class: 'pl-section', 'aria-label': 'Stages' }, details);
  }

  // Change stages (the form rule): each stage is its own line, edited where it stands.
  // Its name and done-when save on Return or blur, Rounds is three chips and the
  // owner's word one chip, each saved on its tap; a new stage is one line. The owner's
  // word is the owner's (D24): the coordinator sees it on a stage, as a fact, and
  // neither taps it nor removes a stage that waits on it.
  stageLines(pipeline) {
    const stages = pipeline.stages, owner = sentenceName(pipeline.owner === this.reader ? 'you' : pipeline.owner), mine = pipeline.you_own;
    const list = el('ol', { class: 'pl-stagedefs pl-stage-lines', 'aria-label': 'Change stages' }, ...stages.map((stage, index) => {
      const id = stage.stage_id, key = `stage:${id}`, name = stage.name || `stage ${index + 1}`;
      const chip = (role, words, pressed, title, change) => el('button', { type: 'button', class: 'pl-pick', 'data-role': role, 'aria-pressed': pressed ? 'true' : 'false', title, text: words,
        onclick: () => { if (change) this.stageAct(id, change, role); } });
      const move = (role, words, label, change, saved) => el('button', { type: 'button', class: 'pl-stage-move', 'data-role': role, 'aria-label': label, title: label, text: words,
        onclick: () => this.stageAct(id, change, role, saved) });
      // Where the missed read's line says the change is saved (missedLine), its quiet Saved does not say it again.
      const note = this.stageNote?.id === id && !this.missedSaidAt(key) ? el('p', { class: 'pl-stage-note muted', role: 'status', text: this.stageNote.text }) : null;
      return el('li', { 'data-key': key },
        el('span', { class: 'pl-num', text: String(index + 1) }),
        el('div', { class: 'pl-stage-edit' },
          el('div', { class: 'pl-stage-line' },
            this.stageField(stage, 'name', { 'aria-label': `Name of stage ${index + 1}`, placeholder: 'Stage name', maxlength: '200' }),
            this.stageField(stage, 'done', { 'aria-label': `When ${name} is done`, placeholder: 'Done when…', maxlength: '2000' })),
          el('div', { class: 'pl-stage-line' },
            el('span', { class: 'pl-picks', role: 'group', 'aria-label': `Rounds in ${name}` }, el('span', { class: 'muted', 'aria-hidden': 'true', text: 'Rounds' }),
              ...ROUND_CHIPS.map(([value, words, title]) => chip(`rounds-${value}`, words, stage.rounds === value, title,
                stage.rounds === value ? null : list => { stageIn(list, id).rounds = value; }))),
            mine ? chip('gate-owner', `${owner === 'You' ? 'Your' : `${owner}’s`} word`, !!stage.gate?.owner,
              stage.gate?.owner ? `Only ${owner === 'You' ? 'you move' : `${owner} moves`} an item past this stage. Tap to let it pass without.` : `Tap so that only ${owner === 'You' ? 'you move' : `${owner} moves`} an item past this stage.`,
              list => { const gate = stageIn(list, id).gate; gate.owner = !gate.owner; })
              : stage.gate?.owner ? el('span', { class: 'pl-word muted', 'data-role': 'gate-owner', title: `Only ${owner} moves an item past this stage, and only ${owner} changes that.`, text: `${owner}’s word` }) : null,
            this.stageField(stage, 'checks', { 'aria-label': `Who or what checks ${name}`, placeholder: 'Checked by…', maxlength: '1000' }),
            el('span', { class: 'pl-stage-moves' },
              index ? move('stage-up', '↑', `Move ${name} up`, list => { list.splice(index - 1, 0, list.splice(index, 1)[0]); }) : null,
              index < stages.length - 1 ? move('stage-down', '↓', `Move ${name} down`, list => { list.splice(index + 1, 0, list.splice(index, 1)[0]); }) : null,
              // "Removed …, Undo" only once Theseus has taken the removal, never beside a refusal of it.
              stages.length > 1 && (mine || !stage.gate?.owner) ? move('stage-remove', '×', `Remove ${name}`, list => {
                const at = list.findIndex(other => other.stage_id === id);
                if (at < 0) throw Error(`${name} is no longer a stage. The stages are shown as they are now.`);
                return { stage: list.splice(at, 1)[0], index: at };
              }, removed => { this.removed = removed; }) : null)),
          note, this.said(key)));
    }));
    const submit = title => this.lineAct('add-stage', title, () => { this.newStage = ''; }, title => {
      const slug = title.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '').slice(0, 40) || 'stage';
      const stage = { stage_id: `${slug}-${Date.now().toString(36)}`, name: title, done_when: '', gate: { checks: [], owner: false, words: '' }, rounds: 'none', scope: 'piece', added_note: '' };
      return this.stagesChange(list => { list.push(stage); }, () => { this.removed = null; this.stageNote = { id: stage.stage_id, text: 'Added. Its done-when and chips are optional.' }; });
    });
    const add = titleLine({
      key: 'add-stage', value: this.newStage, label: 'Add a stage', placeholder: 'Add a stage…', hint: 'Return adds it last · Escape clears',
      input: value => { this.newStage = value; },
      cancel: (had, input) => { if (!had) input.blur(); },
      submit,
    });
    add.append(...this.said('add-stage', { className: 'pl-line-note' }));
    const held = this.heldLines('add-stage', 'Add a stage', 'Add a stage…');
    const removed = this.removed;
    const undo = removed ? el('p', { class: 'pl-stage-removed muted', 'data-key': 'stage-undo', role: 'status' }, `Removed ${removed.stage.name}. `,
      el('button', { type: 'button', 'data-role': 'undo', text: 'Undo', onclick: () => this.act('stage-undo', async () => {
        await this.setStages(list => { if (!list.some(other => other.stage_id === removed.stage.stage_id)) list.splice(Math.min(removed.index, list.length), 0, removed.stage); });
        this.removed = null; this.stageNote = { id: removed.stage.stage_id, text: 'Back.' };
      }, false) }), this.said('stage-undo', { tag: 'span', lead: ' ' })) : null;
    return el('div', { class: 'pl-stage-editing' }, list, undo, add, held);
  }

  // One line of a stage, saved on Return or blur; Escape puts back what was saved.
  stageField(stage, field, attrs) {
    const id = stage.stage_id, key = `${id}:${field}`;
    const input = el('input', { type: 'text', class: `pl-stage-${field}`, 'data-role': `stage-${field}`, autocomplete: 'off', enterkeyhint: 'done', spellcheck: 'true', ...attrs });
    input.value = this.stageDrafts.get(key) ?? this.sent.get(key) ?? stageText(stage, field);
    input.oninput = () => { this.stageDrafts.set(key, input.value); if (this.stageNote?.id === id) this.stageNote = null; };
    input.onblur = () => { if (!this.painting) this.saveStageText(id, field); };
    input.onkeydown = event => {
      if (event.isComposing) return;
      if (event.key === 'Enter') { event.preventDefault(); this.saveStageText(id, field); }
      else if (event.key === 'Escape') {
        event.preventDefault(); event.stopPropagation();
        this.stageDrafts.delete(key); input.value = stageText(this.stageOf(id) ?? stage, field); input.blur();
      }
    };
    return input;
  }

  stageOf(id) { return this.answer?.pipeline?.stages?.find(stage => stage.stage_id === id) ?? null; }

  saveStageText(id, field) {
    const key = `${id}:${field}`, draft = this.stageDrafts.get(key), stage = this.stageOf(id);
    if (draft === undefined || !stage) return this.chain;
    if (draft.trim() === stageText(stage, field)) { this.stageDrafts.delete(key); return this.chain; }
    return this.act(`stage:${id}`, async () => {
      // Read again when its turn comes: an earlier save may already have sent these words.
      const text = this.stageDrafts.get(key), now = this.stageOf(id);
      if (text === undefined || !now) return;
      if (text.trim() !== stageText(now, field)) {
        if (field === 'name' && !text.trim()) throw Error('A stage needs a name. Escape puts the old one back.');
        await this.setStages(list => { setStageText(stageIn(list, id), field, text.trim()); });
        this.sent.set(key, text.trim()); this.stageNote = { id, text: 'Saved.' };
      }
      if (this.stageDrafts.get(key) === text) this.stageDrafts.delete(key);
    }, false);
  }

  // A tap on a stage's chip or move: the whole list as last read, with that one change.
  // Once it is saved, `saved` hears what the change gave back; without one, the stage says Saved.
  stageAct(id, change, role, saved = null) {
    return this.act(`stage:${id}`, async () => {
      let made;
      await this.setStages(list => { made = change(list); });
      if (saved) saved(made); else this.stageNote = { id, text: 'Saved.' };
    }, role);
  }

  // pipeline.stages_set takes the whole ordered list; each change is made to the list
  // as the page last read it, at its turn, with the revision that read carried.
  setStages(edit) { return this.sendChange(this.stagesChange(edit)); }

  stagesChange(edit, saved) {
    const pipeline = this.answer?.pipeline;
    if (!pipeline) throw Error('This pipeline changed since this page read it. It is shown as it is now.');
    const list = pipeline.stages.map(({ you_leave_it, ...stage }) => ({ ...stage, gate: { ...stage.gate } }));
    edit(list);
    return this.change('pipeline.stages_set', { workspace_id: this.workspace }, { stages: list }, { pipeline_revision: pipeline.version }, saved);
  }

  timesSection(answer, pipeline) {
    const times = answer.stage_times;
    if (!times?.rows?.length) return null;
    const sizes = times.sized ? ['small', 'medium', 'large'] : ['all'];
    const cell = typical => { const [value, sub] = typicalCell(typical); return el('td', {}, value, el('small', { text: sub })); };
    return section('How long stages have taken', times.sized
      ? 'Past data, not a promise. Each number is the median: half of past items of that size spent less time in the stage, half spent more.'
      : `Past data from this outcome's own ${pipeline.unit_many}, not a promise. Half of past ${pipeline.unit_many} spent less time in the stage, half spent more.`,
    el('table', { class: 'pl-table' },
      el('thead', {}, el('tr', {}, el('th', { scope: 'col', text: 'Stage' }), ...sizes.map(size => el('th', { scope: 'col', text: size === 'all' ? `Past ${pipeline.unit_many}` : SIZE_WORDS[size] })))),
      el('tbody', {}, ...times.rows.map(row => row.release
        ? el('tr', {}, el('th', { scope: 'row', text: row.label }), el('td', { colspan: String(sizes.length) }, typicalCell(row.release)[0],
          el('small', { text: 'whole release' + ('of' in row.release ? ` · median of the last ${row.release.of} releases` : ` · ${row.release.too_few} so far`) })))
        : el('tr', {}, el('th', { scope: 'row', text: row.label }), ...sizes.map(size => cell(row.cells[size])))))));
  }

  shippedSection(answer, pipeline, now) {
    const groups = answer.groups ?? [];
    if (!groups.length) return null;
    return section(pipeline.ships_in_releases ? 'Shipped' : 'Done', pipeline.ships_in_releases ? 'Each release lists what its deploy record says it shipped.' : `Each ${pipeline.unit_one} is done when its last stage is.`,
      ...groups.map((group, index) => {
        const key = `shipped:${group.label}`;
        return el('details', { class: 'pl-group pl-shipped', 'data-key': key, open: this.open.has(key) || (index === 0 && !this.closed.has(key)) },
          el('summary', {}, group.label + (group.deployed_ms ? `, ${datedTime(group.deployed_ms)}` : ''), el('span', { class: 'muted', text: ` · ${group.items.length} item${group.items.length === 1 ? '' : 's'}` })),
          el('ul', {}, ...group.items.map(item => el('li', {}, el('span', { role: 'img', 'aria-label': 'Shipped', text: '✅' }), el('span', { class: 'pl-num', text: String(item.number) }),
            el('a', { href: pipelineHref(this.workspace, item.number), class: 'pl-shipped-title', text: item.title }), ...this.origin(item)))),
          group.record || group.notes.length ? el('p', { class: 'muted' }, group.record ? `Release record ${group.record}` : 'Written by hand',
            group.notes.length ? ` · also ${group.notes.length} smaller change${group.notes.length === 1 ? '' : 's'} with no number` : '') : null,
          group.notes.length ? el('ul', { class: 'pl-notes' }, ...group.notes.map(text => el('li', { text }))) : null);
      }));
  }

  // The one line at the top of a section (the form rule): a title and Return adds it
  // there, last; Escape clears it. Words and size are set on the item afterwards.
  addLine(pipeline, name) {
    const key = `add:${name}`, label = pipeline.section_words?.[name] ?? sentenceName(name);
    const submit = title => {
      this.added = null;
      this.lineAct(key, title, () => { this.addDrafts[name] = ''; }, title => {
        const args = { title, section: name, sources: [{ note: 'the pipeline page' }] }; // "From the pipeline page"; who added it is its `added` event
        if (name === 'candidates') args.candidate_group = '';
        return this.change('pipeline.item_add', { workspace_id: this.workspace }, args, {}, result => {
          if (this.details !== null) this.saveWords(this.details);
          this.added = { section: name, number: result.number }; this.details = result.number;
          if (name === 'candidates') { this.open.add('group:'); this.closed.delete('group:'); }
        });
      });
    };
    const form = titleLine({
      key, value: this.addDrafts[name] ?? '', label: `Add an item to ${label}`, hint: `Return adds it to ${label} · Escape clears`,
      input: value => { this.addDrafts[name] = value; },
      cancel: (had, input) => { if (!had) input.blur(); },
      submit,
    });
    if (this.added?.section === name) form.append(el('p', { class: 'pl-line-note muted', role: 'status', text: `Added ${this.added.number}, last in ${label}. ${pipeline.sized ? 'Words and a size are' : 'Words are'} optional; set them on it below.` }));
    form.append(...this.said(key, { className: 'pl-line-note' }));
    return [form, ...this.heldLines(key, `Add an item to ${label}`, 'Add an item…')];
  }

  // An item's words (a sentence or two) and its size as three chips, set in place.
  detailsPanel(item, pipeline, record = false) {
    const number = item.number, key = `item:${number}`;
    const area = el('textarea', { class: 'pl-words-box', rows: '2', 'data-role': 'words', placeholder: 'Words, if they help: a sentence or two', 'aria-label': `Words for ${number}` });
    area.value = this.wordDrafts.get(number) ?? this.sent.get(`words:${number}`) ?? item.description ?? '';
    area.oninput = () => { this.wordDrafts.set(number, area.value); if (this.savedWords === number) this.savedWords = null; };
    area.onblur = () => { if (!this.painting) this.saveWords(number); };
    area.onkeydown = event => {
      if (event.key === 'Escape' && !record) { event.preventDefault(); this.closeDetails(number); }
      else if (event.key === 'Enter' && (event.metaKey || event.ctrlKey)) { event.preventDefault(); this.saveWords(number); }
    };
    const panel = el('div', { class: 'pl-details' }, area);
    if (pipeline.sized) {
      panel.append(el('div', { class: 'pl-sizes', role: 'group', 'aria-label': `Size of ${number}` }, el('span', { class: 'muted', text: 'Size' }),
        ...['small', 'medium', 'large'].map(size => el('button', {
          type: 'button', class: 'pl-size', 'data-role': `size-${size}`, 'aria-pressed': item.size === size ? 'true' : 'false', 'aria-label': SIZE_WORDS[size],
          title: item.size === size ? `${SIZE_WORDS[size]}. Tap again for no size.` : SIZE_WORDS[size], text: size[0].toUpperCase(),
          onclick: () => this.act(key, () => this.edit(number, { size: item.size === size ? null : size }), `size-${size}`),
        }))));
    }
    panel.append(el('p', { class: 'pl-details-note muted', text: this.savedWords === number ? 'Saved.' : 'Optional. Words save when you click away or press ⌘Return.' }));
    return panel;
  }

  saveWords(number) {
    const draft = this.wordDrafts.get(number), item = this.current(number);
    if (draft === undefined || !item) return this.chain;
    if (draft === (item.description ?? '')) { this.wordDrafts.delete(number); return this.chain; }
    return this.act(`item:${number}`, async () => {
      // Read again when its turn comes: an earlier save may already have sent these words.
      const words = this.wordDrafts.get(number), now = this.current(number);
      if (words === undefined || !now) return;
      if (words !== (now.description ?? '')) { await this.edit(number, { description: words }); this.sent.set(`words:${number}`, words); this.savedWords = number; }
      if (this.wordDrafts.get(number) === words) this.wordDrafts.delete(number);
    }, false);
  }

  toggleDetails(number) {
    if (this.details === number) return this.closeDetails(number);
    if (this.details !== null) this.saveWords(this.details);
    this.details = number; this.savedWords = null; this.paint();
    this.host.querySelector(`[data-key="item:${number}"] [data-role="words"]`)?.focus();
  }

  closeDetails(number) {
    this.saveWords(number);
    this.details = null; this.paint();
    this.host.querySelector(`[data-key="item:${number}"] [data-role="details"]`)?.focus();
  }

  // Turning a pipeline on is one tap with the defaults (the form rule); its title and
  // whether items are sized are changed afterwards on the page, in place.
  turnOn(pipeline) {
    // The owner or the coordinator turns it on (they steer it together).
    const answer = this.answer;
    const steers = answer.pipeline ? answer.pipeline.you_steer : answer.you_steer === true;
    const box = el('div', { class: 'pl-empty' }, el('p', {}, el('strong', { text: 'This outcome’s pipeline is off.' })),
      el('p', { class: 'muted', text: 'A pipeline numbers every piece of work this outcome delivers, from first mention to the release that shipped it. Turning it on keeps nothing hidden; turning it off keeps every item.' }));
    if (!steers) return box;
    box.append(el('p', { class: 'pl-turn-on' }, el('button', { type: 'button', class: 'primary', 'data-role': 'turn-on', text: 'Turn on',
      onclick: () => this.act('turn-on', async () => {
        await this.command('pipeline.configure', { workspace_id: this.workspace }, { enabled: true }, { pipeline_revision: pipeline?.version ?? 0 });
        this.settingsNote = { text: 'On. Change its title above, or whether items are sized, right here.' };
      }, false).then(() => { if (this.answer?.pipeline?.enabled) this.titleField?.focus({ preventScroll: true }); }) }),
    el('span', { class: 'muted', text: pipeline ? `Comes back as ${pipeline.title}, with every item it kept.` : 'Starts as “Pipeline”, with items sized small, medium or large. Both change on the page after.' })));
    box.append(...this.said('turn-on'));
    return el('div', { 'data-key': 'turn-on' }, box);
  }

  // The owner's title, edited where it stands in the page header: Return or blur saves it,
  // Escape puts it back. The same element stays across repaints, so typing is never lost.
  titleInPlace(pipeline) {
    const field = this.titleField ??= el('input', { type: 'text', class: 'pl-title-input', 'aria-label': 'Pipeline title', title: 'Return saves · Escape puts it back',
      autocomplete: 'off', enterkeyhint: 'done', maxlength: '200', spellcheck: 'true' });
    field.oninput = () => { this.titleDraft = field.value; this.settingsNote = null; };
    field.onblur = () => this.saveTitle();
    field.onkeydown = event => {
      if (event.isComposing) return;
      if (event.key === 'Enter') { event.preventDefault(); this.saveTitle(); }
      else if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); this.titleDraft = null; field.value = this.answer?.pipeline?.title ?? ''; field.blur(); }
    };
    if (document.activeElement !== field) field.value = this.titleDraft ?? this.sent.get('title') ?? pipeline.title;
    const title = this.header.title;
    if (title.childNodes.length !== 1 || title.firstChild !== field) title.replaceChildren(field);
  }

  saveTitle() {
    const draft = this.titleDraft, pipeline = this.answer?.pipeline;
    if (draft === null || !pipeline) return this.chain;
    if (draft.trim() === pipeline.title) { this.titleDraft = null; return this.chain; }
    return this.act('settings', async () => {
      const text = this.titleDraft, now = this.answer?.pipeline;
      if (text === null || !now) return;
      if (text.trim() !== now.title) {
        await this.command('pipeline.configure', { workspace_id: this.workspace }, { enabled: now.enabled, title: text.trim() }, { pipeline_revision: now.version });
        this.sent.set('title', text.trim()); this.settingsNote = { text: 'Title saved.' };
      }
      if (this.titleDraft === text) this.titleDraft = null;
    }, false);
  }

  // The owner's settings line: whether items are sized, one chip, saved on its tap;
  // the quiet note or the refusal for the title above shows here too.
  settingsLine(pipeline) {
    return el('div', { class: 'pl-settings', 'data-key': 'settings' },
      el('button', { type: 'button', class: 'pl-pick', 'data-role': 'sized', 'aria-pressed': pipeline.sized ? 'true' : 'false',
        title: pipeline.sized ? 'Items can be small, medium or large. Tap to stop sizing them.' : 'Tap so that items can be small, medium or large.',
        text: 'Sized S · M · L',
        onclick: () => this.act('settings', async () => {
          const now = this.answer.pipeline;
          await this.command('pipeline.configure', { workspace_id: this.workspace }, { enabled: now.enabled, sized: !now.sized }, { pipeline_revision: now.version });
          this.settingsNote = { text: now.sized ? 'Items are no longer sized.' : 'Items can be sized now: S, M or L on each.' };
        }, 'sized') }),
      this.settingsNote ? el('span', { class: 'muted', role: 'status', text: this.settingsNote.text }) : null,
      this.said('settings', { tag: 'span' }));
  }

  // ---- an item's record (9.1.5) ----------------------------------------------------

  paintRecord(now) {
    const answer = this.record, item = answer.item, pipeline = answer.pipeline;
    this.paintHeader(`${answer.workspace_title} · ${pipeline.title}`, `${item.number}. ${item.title}`, item.description);
    const page = [el('p', {}, el('a', { href: pipelineHref(this.workspace), text: `← ${pipeline.title}` }))];
    const status = item.shipped ? (item.shipped.release ? `Shipped in release ${item.shipped.release}, ${shownTime(item.shipped.deployed_ms, now)}.`
      : item.shipped.note ? `Shipped ${item.shipped.note}.` : `Done ${shownTime(item.shipped.done_ms, now)}.`)
      : item.section === 'dropped' ? 'Dropped. Kept with its number, so it can come back.' : `In ${pipeline.section_words?.[item.section] ?? item.section}.`;
    const editable = item.section !== 'dropped' && !item.shipped;
    page.push(el('article', { class: 'pl-item', 'data-key': `item:${item.number}` }, this.head(item), this.meta(item, pipeline, now), el('p', { class: 'pl-meta', text: status }),
      editable ? this.detailsPanel(item, pipeline, true) : null, this.said(`item:${item.number}`)));
    for (const ask of item.asks) page.push(el('p', { class: 'pl-note' }, `${ask.by} asked you, since ${shownTime(ask.since_ms, now)}: “${ask.excerpt}” In `, el('a', { href: topicHref(ask.topic_id), text: ask.topic_title }), '.'));
    const runs = sourceRuns(item.sources, this.reader);
    page.push(section('Where this came from', null, runs.parts.length
      ? el('p', { class: 'pl-origin-whole' }, ...runs.parts.map(part => part.message ? el('a', { href: topicHref(part.message.topic_id), text: part.text }) : part.text))
      : el('p', { class: 'muted', text: 'Nothing recorded.' })));
    const gathering = gatheringWords(item.gathering, this.reader);
    if (gathering) page.push(el('p', { class: 'pl-origin pl-maturing' }, gathering.lead, gathering.link ?? '', gathering.mine ? ' (you)' : '', gathering.after));
    if (item.pieces?.length) {
      page.push(section('Stages', null, ...item.pieces.map(piece => el('div', { class: 'pl-item' },
        piece.topic ? el('p', { class: 'pl-meta' }, 'Topic: ', el('a', { href: topicHref(piece.topic.id), text: piece.topic.title })) : null,
        piece.stage ? this.stageBox(piece, pipeline, now, true)
          : el('ol', { class: 'pl-history-list' }, ...piece.stages_so_far.map(entry => el('li', { text: historyLine(entry, now) })))))));
    }
    const progress = item.events.find(event => event.payload?.progress?.length);
    if (progress) page.push(el('p', { class: 'pl-note', text: `Progress noted by hand before the pipeline was part of Theseus: ${progress.payload.progress.join(' ')}` }));
    // A move made on the owner's word links that word to the message it was said in.
    page.push(section('What happened to it', null, el('ol', { class: 'pl-events' }, ...item.events.map(event => el('li', {},
      ...eventRuns(event, this.reader, pipeline.section_words, now, item.number).map(run => run.message
        ? el('a', { href: messageHref(run.message.topic_id, run.message.message_id), text: run.text }) : run.text))))));
    this.show(page);
  }
}

const isField = node => node instanceof HTMLInputElement || node instanceof HTMLTextAreaElement;

const tryAgain = onclick => el('button', { type: 'button', 'data-role': 'try-again', text: BUSY_WORDS.again, onclick });
// Why a change is not saved, with Try again when there is one.
const notSaved = (words, again, { tag = 'p', className = '', lead = '' } = {}) =>
  el(tag, { class: `pl-error ${className}`.trim(), role: 'status' }, lead + words, again ? ' ' : null, again ? tryAgain(again) : null);

// One line for a title (the form rule): placeholder "Add an item…", Return submits it,
// Escape clears it (and then `cancel` hears whether it held text). The pipeline page's
// sections and a message's Add to pipeline both use it.
export function titleLine({ key, value = '', label, hint, placeholder = 'Add an item…', role = 'add-title', readonly = false, input, submit, cancel }) {
  const field = el('input', { type: 'text', class: 'pl-line-input', 'data-role': role, placeholder, 'aria-label': label, autocomplete: 'off',
    enterkeyhint: 'done', maxlength: '500', spellcheck: 'true', readonly });
  field.value = value;
  field.oninput = () => input?.(field.value);
  field.onkeydown = event => {
    if (event.key !== 'Escape' || event.isComposing) return;
    event.preventDefault(); event.stopPropagation();
    const had = field.value !== '';
    if (had) { field.value = ''; input?.(''); }
    cancel?.(had, field);
  };
  const form = el('form', { class: 'pl-line', 'data-key': key }, field, el('span', { class: 'pl-line-hint muted', text: hint }));
  form.onsubmit = event => { event.preventDefault(); const title = field.value.trim(); if (title) submit(title, field); };
  return form;
}

function glyph(kind) {
  const [mark, words] = glyphWords(kind);
  return el('span', { class: 'pl-glyph', 'data-glyph': kind, role: 'img', 'aria-label': words, title: words, text: mark });
}

function section(title, note, ...kids) {
  return el('section', { class: 'pl-section', 'aria-label': title }, el('h2', { text: title }), note ? el('span', { class: 'muted', text: note }) : null, ...kids);
}

const ROUND_CHIPS = [
  ['none', 'None', 'No rounds: an item passes this stage once.'],
  ['together', 'Together', 'Each round together: an item can come back for more rounds, each counted with the first.'],
  ['split', 'First apart', 'First round apart: the first round is counted on its own, later rounds together.'],
];
const STAGE_TEXT = { name: stage => stage.name, done: stage => stage.done_when ?? '', checks: stage => stage.gate?.words ?? '' };
const stageText = (stage, field) => STAGE_TEXT[field](stage);
function setStageText(stage, field, text) {
  if (field === 'name') stage.name = text;
  else if (field === 'done') stage.done_when = text;
  else stage.gate.words = text;
}
function stageIn(list, id) {
  const stage = list.find(other => other.stage_id === id);
  if (!stage) throw Error('That stage is no longer there. The stages are shown as they are now.');
  return stage;
}

function gateWords(stage, owner) {
  const parts = [];
  if (stage.gate?.owner) parts.push(`${owner}’s word. Nothing else moves an item past this stage.`);
  if (stage.gate?.checks?.length) parts.push(`checks ${listWords(stage.gate.checks)}`);
  if (stage.gate?.words) parts.push(stage.gate.words);
  return parts.length ? `Checked by: ${parts.join('; ')}` : 'Checked by: its own words';
}

// The rail's Pipeline row (D19): the pipeline's title, Waiting on you when an
// item is ❓ for the reader, and the work ring when a Now piece's topic has work seen.
export class PipelineRow {
  constructor({ section, host, workspace, current = false, read, reader }) {
    Object.assign(this, { section, host, workspace, current, read, reader });
    this.on = false; // whether the outcome's pipeline is on: a message offers Add to pipeline only then
  }
  async load() {
    let answer;
    try { answer = await this.read('pipeline_row', { workspace_id: this.workspace }); }
    catch { return false; }
    if (answer?.error) return false;
    const row = answer.pipeline;
    this.on = !!row;
    this.section.hidden = !row;
    if (!row) { this.host.replaceChildren(); return true; }
    const link = el('a', { class: 'nav-item', href: pipelineHref(this.workspace), 'aria-current': this.current ? 'page' : null }, row.title);
    if (row.asks) link.append(el('span', { class: 'topic-state mine', 'data-state': 'waiting', text: 'Waiting on you' }));
    if (row.work?.observed) {
      link.title = workNote(row.work, this.reader);
      link.append(el('span', { class: 'topic-work', 'aria-hidden': 'true' }), el('span', { class: 'sr-only', text: ` · ${link.title}` }));
    }
    this.host.replaceChildren(link);
    return true;
  }
}

// The outcome page's pipeline line (D11): "Pipeline: Release pipeline ›", or,
// for its owner or coordinator while the pipeline is off, "Pipeline: off · Turn on".
export async function paintPipelineLine(host, { workspace, read }) {
  let answer;
  try { answer = await read('pipeline_row', { workspace_id: workspace }); } catch { host.hidden = true; return false; }
  if (answer?.error) { host.hidden = true; return false; }
  if (answer.pipeline) {
    host.replaceChildren('Pipeline: ', el('a', { href: pipelineHref(workspace), text: `${answer.pipeline.title} ›` }),
      answer.pipeline.asks ? el('span', { class: 'topic-state mine', 'data-state': 'waiting', text: 'Waiting on you' }) : '');
    host.hidden = false;
  } else if (answer.you_steer) {
    host.replaceChildren('Pipeline: off · ', el('a', { href: pipelineHref(workspace), text: 'Turn on' }));
    host.hidden = false;
  } else host.hidden = true;
  return true;
}
