from __future__ import annotations

"""A pipeline's stages and the pieces of work that move through them (item 84;
docs/pipeline.md): starting a piece, entering stages in rounds, a stage gated on
the owner's word, combining pieces into a release, finishing, and changing the
stages without losing past times."""

import datetime
import unittest

from test_pipeline_steering import PipelineCase


STAGES = ({"stage_id": "build", "name": "Build", "rounds": "together"},
          {"stage_id": "review", "name": "Review", "rounds": "split"},
          {"stage_id": "wait", "name": "Wait"},
          {"stage_id": "tony-reads", "name": "Tony reads it", "gate": {"owner": True, "words": "Tony says when"}})


class StagesCase(PipelineCase):
    def create_topic(self):
        self.workspace()
        return self.topic("t1", "Topic")

    def setUp(self):
        super().setUp()
        self.committed(self.on())
        self.stages(*STAGES)
        for n in range(1, 4):
            self.add("Now %d" % n, section="now", size=("small", "large", "medium")[n - 1])
        self.add("Next 4")

    def start(self, numbers=(1, 2), stage="build", topic="t1", actor="owner", **args):
        return self.command("pipeline.piece_start", dict({"numbers": list(numbers), "topic_id": topic, "stage_id": stage}, **args),
                            {"workspace_id": "w1"}, actor=actor)

    def enter(self, piece, stage, actor="owner", **args):
        return self.command("pipeline.stage_enter", dict({"stage_id": stage, "left_outcome": "approved"}, **args), {"piece_id": piece}, actor=actor)

    def entries(self, piece):
        return self.sql("SELECT stage_id,round,left_outcome,left_ms IS NOT NULL FROM pipeline_stage_entries WHERE piece_id=? ORDER BY seq", (piece,))

    def topic(self, topic_id, title="Another topic"):
        # 632: stage fixtures build in agent-opened topics. Keep the shared
        # ReaderCase's person-opened writing fixtures unchanged.
        from test_build_topics import BuildTopicsCase
        return BuildTopicsCase.topic(self, topic_id, title)

    def test_a_piece_starts_only_on_now_items_not_already_in_work_in_a_topic_of_this_outcome(self):
        before = self.sql("SELECT count(*) FROM pipeline_pieces")
        self.rejected(self.start(numbers=(1, 4)), "NOT_IN_NOW")
        self.assertEqual(self.sql("SELECT count(*) FROM pipeline_pieces"), before, "nothing written")
        self.rejected(self.start(stage="nowhere"), "STAGE_UNKNOWN")
        piece = self.committed(self.start())
        self.assertEqual((piece["numbers"], piece["stage_id"]), ([1, 2], "build"))
        self.rejected(self.start(numbers=(2, 3)), "ITEM_IN_PIECE")
        self.rejected(self.start(numbers=(3,)), "TOPIC_HAS_PIECE")
        # The piece is as large as its largest item, and Now shows it being built.
        now = self.store.view("owner", "pipeline", {"workspace_id": "w1"})
        [shown] = now["pieces"]
        self.assertEqual((shown["items"], shown["size"], shown["stage"]["name"], shown["ahead"]),
                         ([1, 2], "large", "Build, round 1", ["Review", "Wait", "Tony reads it"]))
        self.assertEqual({item["number"]: item["glyph"] for item in now["items"]}, {1: "building", 2: "building", 3: "idle"})
        self.assertEqual(now["facts"]["building"], {"items": [1, 2], "pieces": 1})

    def test_entering_a_stage_closes_the_last_one_in_the_same_act_and_rounds_follow_the_stage(self):
        piece = self.committed(self.start())["piece_id"]
        self.committed(self.enter(piece, "build", round=2, left_outcome="sent back"))
        self.rejected(self.enter(piece, "build", round=2), "INVALID_ARGUMENT")
        self.committed(self.enter(piece, "review"))
        self.committed(self.enter(piece, "review", round=2, left_outcome="sent back"))
        self.committed(self.enter(piece, "wait"))
        self.rejected(self.enter(piece, "wait", round=2), "INVALID_ARGUMENT")
        self.assertEqual(self.entries(piece), [("build", 1, "sent back", 1), ("build", 2, "approved", 1), ("review", 1, "sent back", 1),
                                               ("review", 2, "approved", 1), ("wait", 1, None, 0)])
        # Each round reads by its stage's setting: every round named, or only the later ones.
        names = [entry["name"] for entry in self.store.view("owner", "pipeline", {"workspace_id": "w1"})["pieces"][0]["stages_so_far"]]
        self.assertEqual(names, ["Build, round 1", "Build, round 2", "Review", "Review, round 2"])
        self.rejected(self.enter(piece, "review", left_outcome="finished"), "INVALID_ARGUMENT")

    def test_a_stage_on_the_owners_word_needs_an_ask_and_only_the_owner_leaves_it(self):
        piece = self.committed(self.start())["piece_id"]
        self.rejected(self.enter(piece, "tony-reads", actor="agent-a"), "ASK_REQUIRED")
        tell = self.post("Ready for you", actor="agent-a", addressees=["owner"], intent="tell")
        self.rejected(self.enter(piece, "tony-reads", actor="agent-a", ask_message_id=tell), "ASK_REQUIRED")
        ask = self.post("@owner would you read 1 and 2?", actor="agent-a", addressees=["owner"], intent="ask")
        self.committed(self.enter(piece, "tony-reads", actor="agent-a", ask_message_id=ask))
        shown = self.store.view("owner", "pipeline", {"workspace_id": "w1"})["pieces"][0]["stage"]
        self.assertEqual((shown["owner_gate"], shown["you_leave_it"], shown["gate_words"]), (True, True, "Tony says when"))
        self.assertFalse(self.store.view("agent-a", "pipeline", {"workspace_id": "w1"})["pieces"][0]["stage"]["you_leave_it"])
        self.rejected(self.enter(piece, "wait", actor="agent-a", left_outcome="passed"), "STAGE_GATED")
        self.committed(self.enter(piece, "wait", actor="owner", left_outcome="passed"))
        self.assertEqual(self.entries(piece)[-2:], [("tony-reads", 1, "passed", 1), ("wait", 1, None, 0)])

    def test_only_the_owner_gives_or_takes_off_their_word_on_a_stage(self):
        # A stage that waits on the owner's word stays the owner's: the
        # coordinator steers the stages with them but cannot take the word
        # off a stage, remove such a stage, or give the owner's word to one.
        set_by = lambda actor, stages: self.command("pipeline.stages_set", {"stages": list(stages)}, {"workspace_id": "w1"}, actor=actor)
        piece = self.committed(self.start())["piece_id"]
        ask = self.post("@owner would you read 1 and 2?", actor="agent-a", addressees=["owner"], intent="ask")
        self.committed(self.enter(piece, "tony-reads", actor="agent-a", ask_message_id=ask))
        self.rejected(self.enter(piece, "wait", actor="agent-a", left_outcome="passed"), "STAGE_GATED")
        kept = lambda: self.sql("SELECT stage_id,position,gate FROM pipeline_stages ORDER BY stage_id")
        before = kept()
        without = dict(STAGES[3], gate={"owner": False, "words": "Tony says when"})
        for name, stages in (("the word taken off", STAGES[:3] + (without,)),
                             ("the stage removed", STAGES[:3]),
                             ("the word given to another stage", (STAGES[0], STAGES[1], dict(STAGES[2], gate={"owner": True}), STAGES[3])),
                             ("a new stage on the word", STAGES + ({"stage_id": "sign", "name": "Sign off", "gate": {"owner": True}},))):
            with self.subTest(name):
                refused = set_by("agent-a", stages)
                self.rejected(refused, "FORBIDDEN")
                self.assertIn("owner", refused["error"]["message"])
        self.assertEqual(kept(), before, "nothing changed")
        self.rejected(self.enter(piece, "wait", actor="agent-a", left_outcome="passed"), "STAGE_GATED")
        self.assertEqual(self.entries(piece)[-1], ("tony-reads", 1, None, 0), "the piece waits on the owner still")
        # The rest of the stages are the coordinator's to change: a name, the order, a new stage.
        self.committed(set_by("agent-a", (dict(STAGES[3], name="Tony reads"),) + STAGES[:3] + ({"stage_id": "polish", "name": "Polish"},)))
        # A stage the owner retired with a piece in it keeps the owner's word, and the coordinator cannot bring it back without.
        self.stages(*STAGES[:3])
        self.rejected(set_by("agent-a", STAGES[:3] + (without,)), "FORBIDDEN")
        self.rejected(self.enter(piece, "wait", actor="agent-a", left_outcome="passed"), "STAGE_GATED")
        # The owner gives and takes off their own word.
        self.stages(*STAGES[:3], without)
        self.committed(self.enter(piece, "wait", actor="agent-a", left_outcome="passed"))
        self.stages(*STAGES)
        self.assertEqual([stage["gate"].get("owner", False) for stage in self.store.view("owner", "pipeline", {"workspace_id": "w1"})["pipeline"]["stages"]],
                         [False, False, False, True])

    def test_undo_that_would_drop_an_item_being_built_asks_first_like_any_drop(self):
        drop = self.committed(self.move(3, section="dropped"))["event"]
        restore = self.committed(self.move(3, undo_of=drop))["event"]
        self.assertEqual(self.item(3)["section"], "now")
        piece = self.committed(self.start(numbers=(3,)))["piece_id"]
        refused = self.move(3, undo_of=restore)
        self.rejected(refused, "ITEM_UNDERWAY")
        self.assertEqual((self.item(3)["section"], self.entries(piece)), ("now", [("build", 1, None, 0)]), "nothing changed")
        self.committed(self.move(3, undo_of=restore, underway_confirmed=True))
        self.assertEqual((self.item(3)["section"], self.entries(piece)), ("dropped", [("build", 1, "dropped", 1)]))

    def test_an_item_being_built_stays_in_now_until_it_is_dropped_or_set_aside_with_a_note(self):
        # D26: Now always holds every piece's items, so the light's work in Now covers them.
        to_now = self.committed(self.move(4, section="now"))["event"]
        piece = self.committed(self.start(numbers=(4,), size="small"))["piece_id"]
        words = {"next": "Next", "later": "Later", "candidates": "Candidates"}
        for section in ("next", "later", "candidates"):
            for confirmed in (False, True):
                with self.subTest(section=section, confirmed=confirmed):
                    refused = self.move(4, section=section, **({"underway_confirmed": True} if confirmed else {}))
                    self.rejected(refused, "NOTE_REQUIRED")
                    self.assertEqual(refused["error"]["message"],
                                     "4 is being built now, so moving it to %s sets its piece aside: say why in a short note" % words[section])
        refused = self.move(4, undo_of=to_now)
        self.rejected(refused, "ITEM_IN_PIECE")
        self.assertEqual(refused["error"]["message"], "4 is being built now, and an Undo never sets its piece aside: move it with a note to set the piece aside")
        self.assertEqual((self.item(4)["section"], 4 in self.section("now"), self.section("next"), self.entries(piece)),
                         ("now", True, [], [("build", 1, None, 0)]))
        # It is still ordered within Now, and an item not being built still moves out.
        self.committed(self.move(4, above=1))
        self.committed(self.move(3, section="later"))
        self.assertEqual((self.section("now"), self.section("later")), ([4, 1, 2], [3]))

    ASIDE = "The tests miss the real case; rethink them"

    def test_moving_an_item_being_built_out_of_now_with_a_note_sets_its_whole_piece_aside(self):
        # Item 183: work that misses lands, or goes to Later to rethink its spec or tests. Its piece closes, every item of
        # it moves (an item is never half in a piece), and its stages so far and the note stay with each item.
        piece = self.committed(self.start())["piece_id"]
        self.committed(self.enter(piece, "review"))
        waiting = self.add("Later 5", section="later")
        moved = self.committed(self.move(2, section="later", above=waiting, note=self.ASIDE))
        self.assertEqual((moved["section"], moved.get("set_aside", {}).get("piece_id"), moved.get("set_aside", {}).get("numbers")), ("later", piece, [2, 1]))
        self.assertEqual((self.section("now"), self.section("later")), ([3], [2, 1, waiting]), "the item moved goes where it is put, the rest of its piece below it")
        self.assertEqual(self.entries(piece), [("build", 1, "approved", 1), ("review", 1, "dropped", 1)])
        self.assertEqual(self.sql("SELECT number,left_ms IS NOT NULL FROM pipeline_piece_members WHERE piece_id=? ORDER BY number", (piece,)),
                         [(1, 1), (2, 1)])
        self.assertEqual(self.store.view("owner", "pipeline", {"workspace_id": "w1"})["pieces"], [], "the piece has closed")
        for number in (1, 2):
            record = self.item(number)
            [past] = record["pieces"]
            self.assertEqual([(entry["name"], entry["left_outcome"], entry["left_note"]) for entry in past["stages_so_far"]],
                             [("Build, round 1", "approved", ""), ("Review", "set aside", "Set aside: " + self.ASIDE)])
            [event] = [event for event in record["events"] if event["kind"] == "moved"]
            self.assertEqual(event["payload"].get("set_aside"), {"piece": piece, "note": self.ASIDE, "stages": ["Build, round 1", "Review"], "numbers": [2, 1]})
            self.assertEqual((record["section"], record["piece"], record["stage"]), ("later", None, None))
        shown = {item["number"]: item for item in self.store.view("owner", "pipeline", {"workspace_id": "w1", "section": "later"})["items"]}
        self.assertEqual((shown[1]["placed"].get("set_aside") or {}).get("note"), self.ASIDE, "the pipeline page's row says why")
        # Back in Now, an item starts a new piece; the one set aside stays as it was.
        self.committed(self.move(1, section="now"))
        again = self.committed(self.start(numbers=(1,), stage="review", size="large"))["piece_id"]
        self.assertNotEqual(again, piece)
        self.assertEqual([other["id"] for other in self.item(1)["pieces"]], [piece, again])
        self.assertEqual(self.entries(piece)[-1], ("review", 1, "dropped", 1))
        # A stage left by setting work aside is not a time that stage took.
        self.assertEqual(self.store.view("owner", "pipeline", {"workspace_id": "w1"})["pieces"][0]["stage"]["typical"], {"too_few": 0, "min": 3})

    def test_undo_of_a_set_aside_puts_the_item_back_in_now_and_leaves_its_piece_closed(self):
        piece = self.committed(self.start())["piece_id"]
        moved = self.committed(self.move(1, section="next", note=self.ASIDE))
        self.assertEqual(self.section("next"), [4, 1, 2])
        back = self.committed(self.move(1, undo_of=moved["event"]))
        self.assertEqual((back["section"], back.get("set_aside_stays")), ("now", piece))
        self.assertEqual((self.section("now"), self.section("next")), ([3, 1], [4, 2]), "only the item undone comes back, last in Now")
        self.assertEqual((self.item(1)["piece"], self.entries(piece)), (None, [("build", 1, "dropped", 1)]), "no piece is opened again")
        self.assertEqual(self.store.view("owner", "pipeline", {"workspace_id": "w1"})["pieces"], [])
        self.assertEqual(self.item(1)["placed"].get("set_aside_stays"), piece)

    def test_a_piece_is_set_aside_only_with_a_note_by_whoever_steers_and_a_note_goes_with_nothing_else(self):
        self.committed(self.move(4, section="now"))
        piece = self.committed(self.start(numbers=(4, 3)))["piece_id"]
        before = (self.positions(), self.entries(piece), self.sql("SELECT count(*) FROM pipeline_events"))
        for note in ({}, {"note": "  "}):
            refused = self.move(4, section="later", **note)
            self.rejected(refused, "NOTE_REQUIRED")
            self.assertEqual(refused["error"]["message"], "4 is being built now, so moving it to Later sets its piece aside, with 3: say why in a short note")
        self.rejected(self.move(4, section="later", note="x" * 2049), "INVALID_ARGUMENT")
        self.rejected(self.move(4, section="later", note=self.ASIDE, actor="agent-b"), "FORBIDDEN")
        self.rejected(self.move(4, section="later", note=self.ASIDE, actor="outsider"), "FORBIDDEN")
        for args in ({"section": "dropped", "underway_confirmed": True}, {"above": 3}):
            refused = self.move(4, note=self.ASIDE, **args)
            self.rejected(refused, "INVALID_ARGUMENT")
            self.assertEqual(refused["error"]["message"], "A note goes only with moving an item being built out of Now, which sets its piece aside")
        refused = self.move(1, section="later", note=self.ASIDE)
        self.rejected(refused, "INVALID_ARGUMENT")
        self.assertEqual((self.positions(), self.entries(piece), self.sql("SELECT count(*) FROM pipeline_events")), before, "nothing changed")
        self.assertEqual([shown["you_set_aside"] for shown in self.store.view("owner", "pipeline", {"workspace_id": "w1"})["pieces"]], [True])
        self.assertEqual([shown["you_set_aside"] for shown in self.store.view("agent-b", "pipeline", {"workspace_id": "w1"})["pieces"]], [False])

    def test_the_coordinator_sets_aside_only_what_the_owner_did_not_place_and_not_while_the_owner_holds_it(self):
        self.topic("t2")
        mine = self.add("The coordinator's", section="now", actor="agent-a", size="small")
        held = self.add("The coordinator's, held", section="now", actor="agent-a", size="small")
        with_owners = self.committed(self.start(numbers=(mine, 3), actor="agent-a"))["piece_id"]
        on_the_owners_word = self.committed(self.start(numbers=(held,), topic="t2", actor="agent-a"))["piece_id"]
        ask = self.post("@owner would you read it?", actor="agent-a", topic="t2", addressees=["owner"], intent="ask")
        self.committed(self.enter(on_the_owners_word, "tony-reads", actor="agent-a", ask_message_id=ask))
        aside = lambda actor: {shown["id"]: shown["you_set_aside"] for shown in self.store.view(actor, "pipeline", {"workspace_id": "w1"})["pieces"]}
        self.assertEqual(aside("owner"), {with_owners: True, on_the_owners_word: True})
        self.assertEqual(aside("agent-a"), {with_owners: False, on_the_owners_word: False})
        before = (self.positions(), self.entries(with_owners), self.entries(on_the_owners_word))
        refused = self.move(mine, section="later", note=self.ASIDE, actor="agent-a")
        self.rejected(refused, "OWNER_PLACED")
        self.assertIn("owner placed 3, which is built with %d," % mine, refused["error"]["message"])
        refused = self.move(held, section="later", note=self.ASIDE, actor="agent-a")
        self.rejected(refused, "STAGE_GATED")
        self.assertEqual(refused["error"]["message"], "Only owner leaves this stage")
        self.assertEqual((self.positions(), self.entries(with_owners), self.entries(on_the_owners_word)), before, "nothing changed")
        # Once the owner's item has left the piece, the coordinator sets the rest aside.
        self.committed(self.move(3, section="dropped", underway_confirmed=True))
        self.assertEqual((aside("agent-a")[with_owners], aside("agent-b")[with_owners]), (True, False), "a member steers nothing")
        self.committed(self.move(mine, section="later", note=self.ASIDE, actor="agent-a"))
        self.assertEqual(self.sql("SELECT left_outcome,left_note,left_by FROM pipeline_stage_entries WHERE piece_id=?", (with_owners,)),
                         [("dropped", "Set aside: " + self.ASIDE, "agent-a")])
        # The owner sets aside work waiting on their word.
        self.committed(self.move(held, section="later", note=self.ASIDE))
        self.assertEqual(self.entries(on_the_owners_word)[-1], ("tony-reads", 1, "dropped", 1))

    def test_a_release_pipeline_sets_a_piece_aside_but_never_the_release_and_a_record_naming_its_item_ships_it(self):
        self.release_stages(*STAGES[:3], self.CHECKS)
        self.topic("t2")
        source = self.committed(self.start(stage="wait"))["piece_id"]
        self.freeze_source(source)
        release = self.committed(self.combine([source], commit_sha="5" * 40))["piece_id"]
        refused = self.move(1, section="later", note=self.ASIDE)
        self.rejected(refused, "ITEM_IN_PIECE")
        self.assertEqual(refused["error"]["message"], "1 is in release 23, being put together, so it stays in Now until it ships, goes back to building or is dropped")
        self.assertEqual(self.store.view("owner", "pipeline", {"workspace_id": "w1"})["release_piece"]["you_set_aside"], False)
        piece = self.committed(self.start(numbers=(3,), topic="t2"))["piece_id"]
        self.committed(self.move(3, section="later", note=self.ASIDE))
        self.assertEqual(self.entries(piece), [("build", 1, "dropped", 1)])
        # What a record names has shipped wherever it is: the piece set aside shipped it after all.
        self.store.record_release({"release": 23, "commit": "5" * 40, "deployed_at": "2026-11-07T08:00:00-08:00", "items": [1, 2, 3], "source": "deploy"})
        self.assertEqual((self.entries(release)[-1][2], self.entries(piece)[-1][2], self.item(3)["shipped"]["release"]), ("shipped", "shipped", 23))

    def test_finishing_a_piece_marks_its_items_done_in_any_pipeline(self):
        piece = self.committed(self.start())["piece_id"]
        self.committed(self.command("pipeline.piece_finish", {"note": "done"}, {"piece_id": piece}))
        self.assertEqual(self.entries(piece), [("build", 1, "done", 1)])
        self.assertEqual(self.item(1)["glyph"], "shipped")
        self.assertEqual(self.section("now"), [3])
        done = self.store.view("owner", "pipeline", {"workspace_id": "w1", "section": "shipped"})["groups"]
        self.assertEqual([(group["label"], [item["number"] for item in group["items"]]) for group in done], [("Done", [1, 2])])
        self.rejected(self.command("pipeline.piece_finish", {}, {"piece_id": piece}), "PIECE_CLOSED")
        self.committed(self.on(ships_in_releases=True))
        other = self.committed(self.start(numbers=(3,)))["piece_id"]
        self.committed(self.command("pipeline.piece_finish", {"note": "Live in release 94; its record missed it"}, {"piece_id": other}))
        self.assertEqual(self.entries(other), [("build", 1, "done", 1)])
        self.assertEqual(self.item(3)["glyph"], "shipped")

    def test_combining_pieces_into_a_release_moves_their_items_and_keeps_their_topics_open(self):
        self.release_stages(*STAGES[:3], self.CHECKS)
        self.topic("t2")
        first = self.committed(self.start(stage="wait"))["piece_id"]
        second = self.committed(self.start(numbers=(3,), topic="t2", stage="wait"))["piece_id"]
        self.freeze_source(first); self.freeze_source(second)
        release = self.committed(self.command("pipeline.combine", {"release_number": 23, "piece_ids": [first, second], "topic_id": "t1", "commit_sha": "5" * 40},
                                              {"workspace_id": "w1"}))
        self.assertEqual(release["numbers"], [1, 2, 3])
        self.assertEqual(self.entries(first), [("wait", 1, "combined", 1)])
        self.assertEqual(self.entries(second), [("wait", 1, "combined", 1)])
        self.assertEqual(self.entries(release["piece_id"]), [("checks", 1, None, 0)])
        self.assertEqual(self.sql("SELECT state FROM topics WHERE id IN ('t1','t2') ORDER BY id"), [("active",), ("active",)])
        now = self.store.view("owner", "pipeline", {"workspace_id": "w1"})
        self.assertEqual((now["facts"]["next_release"]["release"], now["facts"]["next_release"]["items"]), (23, [1, 2, 3]))
        self.assertEqual(now["pieces"], [], "the work pieces are in the release now")
        # Once the release of its commit is recorded, its items have shipped and its piece is closed. It
        # goes live after the stages above were entered, whenever this check runs.
        entered = self.sql("SELECT max(entered_ms) FROM pipeline_stage_entries")[0][0]
        live = datetime.datetime.fromtimestamp(entered / 1000 + 1, datetime.timezone.utc).isoformat(timespec="seconds")
        self.store.record_release({"release": 23, "commit": "5" * 40, "deployed_at": live, "items": [1, 2, 3], "source": "deploy"})
        now = self.store.view("owner", "pipeline", {"workspace_id": "w1"})
        self.assertEqual((now["facts"]["next_release"], (now["banner"] or {}).get("release"), now["items"]), (None, 23, []), "shipped, under its banner")
        self.rejected(self.command("pipeline.combine", {"release_number": 23, "piece_ids": [first], "topic_id": "t1"}, {"workspace_id": "w1"}), "RELEASE_NUMBER")

    def test_one_release_at_a_time_numbered_one_past_the_last_shipped(self):
        # 3.4: the release is the outcome's one open release piece; its number is the last shipped + 1, so the deploy, the
        # Reporter and the page can only mean one piece, until the record of its commit ships it.
        self.release_stages(*STAGES[:3], self.CHECKS)
        self.topic("t2")
        first = self.committed(self.start(stage="wait"))["piece_id"]
        second = self.committed(self.start(numbers=(3,), topic="t2", stage="wait"))["piece_id"]
        self.freeze_source(first); self.freeze_source(second)
        self.rejected(self.combine([first], release=24), "RELEASE_NUMBER")
        this = self.committed(self.combine([first], commit_sha="5" * 40))
        self.assertEqual(this["release_number"], 23)
        self.assertIsNone(self.sql("SELECT release_number FROM pipeline_pieces WHERE id=?", (this["piece_id"],))[0][0], "no number is stored")
        refused = self.combine([second])
        self.rejected(refused, "RELEASE_OPEN")
        self.assertIn("Release 23 is still being put together (Release checks)", refused["error"]["message"])
        self.assertEqual(self.entries(second), [("wait", 1, None, 0)], "nothing was combined")
        now = self.store.view("owner", "pipeline", {"workspace_id": "w1"})
        self.assertEqual((now["facts"]["next_release"]["release"], now["release_piece"]["id"], now["release_piece"]["release_number"]),
                         (23, this["piece_id"], 23))
        self.store.record_release({"release": 23, "commit": "5" * 40, "deployed_at": "2026-11-07T08:00:00-08:00", "items": [1, 2], "source": "deploy"})
        self.assertIsNone(self.store.view("owner", "pipeline", {"workspace_id": "w1"})["facts"]["next_release"], "it shipped")
        self.assertEqual(self.entries(this["piece_id"])[-1][2], "shipped")
        later = self.committed(self.combine([second]))
        now = self.store.view("owner", "pipeline", {"workspace_id": "w1"})
        self.assertEqual((later["release_number"], now["facts"]["next_release"]["piece_id"], now["facts"]["next_release"]["release"]),
                         (24, later["piece_id"], 24))

    CHECKS = {"stage_id": "checks", "name": "Release checks", "scope": "release"}

    def release_stages(self, *stages):
        """A pipeline that ships in releases, with release 22 live, so the next release is 23."""
        self.store.record_release({"release": 22, "commit": "4" * 40, "deployed_at": "2026-11-06T05:28:35-08:00", "items": [], "source": "backfill"})
        self.committed(self.on(ships_in_releases=True))
        self.stages(*(stages or STAGES + (self.CHECKS,)))

    def combine(self, pieces, actor="owner", release=None, topic="t1", **args):
        if release is not None:
            args["release_number"] = release
        return self.command("pipeline.combine", dict({"piece_ids": list(pieces), "topic_id": topic}, **args), {"workspace_id": "w1"}, actor=actor)

    def freeze_source(self, piece):
        # 994: multi-item releases need each real source classification before
        # the stage/owner/ship behavior under test can be exercised.
        version = self.sql("SELECT version FROM pipeline_pieces WHERE id=?", (piece,))[0][0]
        self.committed(self.command("pipeline.freeze", {"base_commit": "a" * 40, "head_commit": "b" * 40,
            "files": ["web/stage-fixture.mjs"]}, {"piece_id": piece}, {"piece_revision": version}))

    def test_combining_leaves_a_stage_on_the_owners_word_only_by_the_owner(self):
        # The blocker probe: agent-b (a member) combined a piece the owner was holding.
        self.release_stages()
        piece = self.committed(self.start())["piece_id"]
        self.freeze_source(piece)
        ask = self.post("@owner would you read 1 and 2?", actor="agent-a", addressees=["owner"], intent="ask")
        self.committed(self.enter(piece, "tony-reads", actor="agent-a", ask_message_id=ask))
        self.rejected(self.enter(piece, "wait", actor="agent-a", left_outcome="passed"), "STAGE_GATED")
        held = self.entries(piece)
        for actor in ("agent-b", "agent-a"):
            with self.subTest(actor=actor):
                refused = self.combine([piece], actor=actor)
                self.rejected(refused, "STAGE_GATED")
                self.assertIn("owner", refused["error"]["message"])
        self.assertEqual((self.entries(piece), self.sql("SELECT count(*) FROM pipeline_pieces WHERE kind='release'")), (held, [(0,)]),
                         "the owner's held ask is held still, and no release was made")
        # The owner's word lets it go: the owner combines it.
        self.committed(self.combine([piece]))
        self.assertEqual(self.sql("SELECT stage_id,left_outcome,left_by FROM pipeline_stage_entries WHERE piece_id=? ORDER BY seq", (piece,))[-1],
                         ("tony-reads", "combined", "owner"))

    def test_combining_into_a_release_stage_on_the_owners_word_needs_an_ask_in_the_release_topic(self):
        self.release_stages(*STAGES[:3], {"stage_id": "approve", "name": "Approve the release", "scope": "release", "gate": {"owner": True}})
        piece = self.committed(self.start(stage="wait"))["piece_id"]
        self.freeze_source(piece)
        self.rejected(self.combine([piece], actor="agent-b"), "ASK_REQUIRED")
        self.rejected(self.combine([piece]), "ASK_REQUIRED")
        self.assertEqual((self.entries(piece), self.sql("SELECT count(*) FROM pipeline_pieces WHERE kind='release'")), ([("wait", 1, None, 0)], [(0,)]))
        tell = self.post("Release 23 is ready", actor="agent-b", addressees=["owner"], intent="tell")
        someone_elses = self.post("@owner may I release 23?", actor="agent-a", addressees=["owner"], intent="ask")
        for ask in (tell, someone_elses):
            self.rejected(self.combine([piece], actor="agent-b", ask_message_id=ask), "ASK_REQUIRED")
        ask = self.post("@owner may I release 23?", actor="agent-b", addressees=["owner"], intent="ask")
        release = self.committed(self.combine([piece], actor="agent-b", ask_message_id=ask))["piece_id"]
        self.assertEqual(self.sql("SELECT stage_id,ask_message_id,left_ms FROM pipeline_stage_entries WHERE piece_id=?", (release,)),
                         [("approve", ask, None)])
        self.rejected(self.command("pipeline.stage_enter", {"stage_id": "approve", "left_outcome": "approved"}, {"piece_id": release}, actor="agent-b"),
                      "INVALID_ARGUMENT")

    def test_a_piece_is_combined_once_it_waits_in_the_last_piece_stage(self):
        # Spec 3.4: combine takes the approved pieces, those waiting in the last stage a piece goes through.
        self.release_stages(*STAGES[:3], self.CHECKS)
        building = self.committed(self.start())["piece_id"]
        self.freeze_source(building)
        refused = self.combine([building])
        self.rejected(refused, "PIECE_NOT_APPROVED")
        self.assertIn("Wait", refused["error"]["message"])
        self.committed(self.enter(building, "review"))
        self.committed(self.enter(building, "wait"))
        self.committed(self.combine([building], actor="agent-b"))
        self.assertEqual(self.entries(building)[-1], ("wait", 1, "combined", 1))

    def test_dropping_the_last_item_of_a_piece_on_the_owners_word_is_the_owners(self):
        # The coordinator placed this item, so the coordinator may drop it, except while the owner holds its piece.
        number = self.add("Placed by the coordinator", section="now", actor="agent-a", size="small")
        piece = self.committed(self.start(numbers=(number,)))["piece_id"]
        ask = self.post("@owner would you read it?", actor="agent-a", addressees=["owner"], intent="ask")
        self.committed(self.enter(piece, "tony-reads", actor="agent-a", ask_message_id=ask))
        self.rejected(self.move(number, actor="agent-a", section="dropped", underway_confirmed=True), "STAGE_GATED")
        self.assertEqual((self.item(number)["section"], self.entries(piece)[-1]), ("now", ("tony-reads", 1, None, 0)), "nothing changed")
        self.committed(self.move(number, section="dropped", underway_confirmed=True))
        self.assertEqual(self.entries(piece)[-1], ("tony-reads", 1, "dropped", 1))

    def test_the_database_keeps_the_owners_word_whatever_writes_the_entry(self):
        # The rule lives where entries are opened and closed, so no command can step around it.
        import sqlite3
        piece = self.committed(self.start())["piece_id"]
        ask = self.post("@owner would you read 1 and 2?", actor="agent-a", addressees=["owner"], intent="ask")
        self.topic("t2")
        elsewhere = self.post("@owner would you read 1 and 2?", actor="agent-a", topic="t2", addressees=["owner"], intent="ask")
        not_the_owner = self.post("@agent-b would you read 1 and 2?", actor="agent-a", addressees=["agent-b"], intent="ask")
        with sqlite3.connect(str(self.path)) as db:
            db.execute("PRAGMA foreign_keys=ON")
            seq = db.execute("SELECT seq FROM pipeline_stage_entries WHERE piece_id=?", (piece,)).fetchone()[0]
            db.execute("UPDATE pipeline_stage_entries SET left_ms=2,left_outcome='approved',left_by='agent-a' WHERE seq=?", (seq,))
            insert = ("INSERT INTO pipeline_stage_entries(piece_id,workspace_id,stage_id,round,entered_ms,entered_by,ask_message_id) "
                      "VALUES (?,'w1','tony-reads',1,3,?,?)")
            for actor, message in (("agent-a", None), ("agent-b", ask), ("agent-a", elsewhere), ("agent-a", not_the_owner), ("agent-a", "m-none")):
                with self.subTest(enter_as=actor, ask=message), self.assertRaisesRegex(sqlite3.IntegrityError, "ASK_REQUIRED|FOREIGN KEY"):
                    db.execute(insert, (piece, actor, message))
            db.execute(insert, (piece, "agent-a", ask))
            for actor in ("agent-a", "agent-b", None):
                with self.subTest(leave_as=actor), self.assertRaisesRegex(sqlite3.IntegrityError, "STAGE_GATED"):
                    db.execute("UPDATE pipeline_stage_entries SET left_ms=4,left_outcome='combined',left_by=? WHERE piece_id=? AND left_ms IS NULL",
                               (actor, piece))
            db.execute("UPDATE pipeline_stage_entries SET left_ms=4,left_outcome='passed',left_by='owner' WHERE piece_id=? AND left_ms IS NULL", (piece,))
        # A release record's end is nobody's and leaves the stage, but only at a record, or at turning releases on, in this
        # outcome's own facts and at that moment: nobody leaves at any other fact, at another moment or at another outcome's record.
        with sqlite3.connect(str(self.path)) as elsewhere:
            elsewhere.execute("INSERT INTO pipeline_facts(workspace_id,kind,ms,by,payload) VALUES ('w-elsewhere','record',8,NULL,'{\"release\": 1}')")
        fact = "INSERT INTO pipeline_facts(workspace_id,kind,ms,by,payload) VALUES ('w1',?,?,NULL,?)"
        leave = "UPDATE pipeline_stage_entries SET left_ms=?,left_outcome='shipped',left_by=NULL WHERE piece_id=? AND left_ms IS NULL"
        with sqlite3.connect(str(self.path)) as db:
            db.execute("PRAGMA foreign_keys=ON")
            db.execute(insert, (piece, "agent-a", ask))
            db.execute(fact, ("record", 2, '{"release": 1}'))
            db.execute(fact, ("drop", 5, '{"number": 1}'))
            db.execute(fact, ("ships", 6, '{"on": false}'))
            for ms in (5, 6, 7, 8):
                with self.subTest(left_by_nobody_at=ms), self.assertRaisesRegex(sqlite3.IntegrityError, "STAGE_GATED"):
                    db.execute(leave, (ms, piece))
            db.execute(fact, ("record", 9, '{"release": 1}'))
            db.execute(fact, ("ships", 10, '{"on": true}'))
            db.execute(leave, (9, piece))
            db.execute(insert, (piece, "agent-a", ask))
            db.execute(leave, (10, piece))
        self.assertEqual(self.sql("SELECT left_ms,left_by FROM pipeline_stage_entries WHERE piece_id=? ORDER BY seq", (piece,))[-2:],
                         [(9, None), (10, None)])

    def test_changing_the_stages_keeps_past_entries_with_the_stage_they_were_in(self):
        piece = self.committed(self.start())["piece_id"]
        self.committed(self.enter(piece, "review"))
        renamed = [dict(STAGES[0], name="Make it"), dict(STAGES[1], name="Read it")] + list(STAGES[2:])
        self.stages(*renamed)
        shown = self.store.view("owner", "pipeline", {"workspace_id": "w1"})["pieces"][0]
        self.assertEqual(([entry["name"] for entry in shown["stages_so_far"]], shown["stage"]["name"]), (["Make it, round 1"], "Read it"))
        # Retiring a stage keeps the entries made in it, and a new piece cannot start there.
        self.stages(renamed[1], renamed[2])
        shown = self.store.view("owner", "pipeline", {"workspace_id": "w1"})["pieces"][0]
        self.assertEqual([entry["stage_id"] for entry in shown["stages_so_far"]], ["build"])
        self.topic("t2")
        self.rejected(self.start(numbers=(3,), stage="build", topic="t2"), "STAGE_UNKNOWN")
        self.committed(self.start(numbers=(3,), stage="wait", topic="t2"))
        events = self.sql("SELECT kind,by FROM pipeline_events WHERE kind='stages' ORDER BY seq")
        self.assertEqual(events, [("stages", "owner")] * 3)
        self.rejected(self.command("pipeline.stages_set", {"stages": []}, {"workspace_id": "w1"}), "INVALID_ARGUMENT")
        self.rejected(self.command("pipeline.stages_set", {"stages": [STAGES[0], STAGES[0]]}, {"workspace_id": "w1"}), "INVALID_ARGUMENT")
        self.rejected(self.command("pipeline.stages_set", {"stages": [STAGES[0]]}, {"workspace_id": "w1"}, actor="agent-b"), "FORBIDDEN")


if __name__ == "__main__":
    unittest.main()
