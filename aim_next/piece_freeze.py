from __future__ import annotations

"""Append-only, source-bound piece classification (987). Git stays in the binder."""
import json
from pathlib import PurePosixPath

from .contract import Refusal, _json
from .delivery import _now_ms
from .judgment import SHA, guarded

FREEZE_SCHEMA = [
    "CREATE TABLE pipeline_piece_freezes (seq INTEGER PRIMARY KEY AUTOINCREMENT, "
    "piece_id TEXT NOT NULL REFERENCES pipeline_pieces(id), piece_revision INTEGER NOT NULL, "
    "base_commit TEXT NOT NULL, head_commit TEXT NOT NULL, files TEXT NOT NULL, judged INTEGER NOT NULL CHECK(judged IN (0,1)), "
    "by_actor TEXT NOT NULL REFERENCES actors(id), ms INTEGER NOT NULL, UNIQUE(piece_id,piece_revision))",
    "CREATE INDEX freeze_of_piece ON pipeline_piece_freezes(piece_id,seq DESC)",
    "CREATE TRIGGER freeze_stays_u BEFORE UPDATE ON pipeline_piece_freezes BEGIN SELECT RAISE(ABORT,'a freeze keeps its past'); END",
    "CREATE TRIGGER freeze_stays_d BEFORE DELETE ON pipeline_piece_freezes BEGIN SELECT RAISE(ABORT,'a freeze keeps its past'); END",
]
FREEZE_DOWN = ("DROP TABLE pipeline_piece_freezes",)
FREEZE_OCCUPIED = "SELECT EXISTS (SELECT 1 FROM pipeline_piece_freezes)"


class PieceFreezes:
    def _piece_freeze(self, db, piece_id):
        row = db.execute("SELECT * FROM pipeline_piece_freezes WHERE piece_id=? ORDER BY seq DESC LIMIT 1", (piece_id,)).fetchone()
        if row is None:
            return None
        return {"base_commit": row["base_commit"], "head_commit": row["head_commit"], "files": json.loads(row["files"]),
                "judged": bool(row["judged"]), "by": row["by_actor"], "at_ms": row["ms"], "piece_revision": row["piece_revision"]}

    def _release_freeze(self, db, workspace_id, release_id):
        # The combine fact names the exact source pieces. Timestamp/member
        # guesses could attach an earlier incarnation of an item to this release.
        fact = db.execute("SELECT payload FROM pipeline_facts WHERE workspace_id=? AND kind='combine' "
                          "AND json_extract(payload,'$.release')=? ORDER BY seq DESC LIMIT 1",
                          (workspace_id, release_id)).fetchone()
        if fact is None:
            return None
        sources = json.loads(fact["payload"])["pieces"]
        source = db.execute("SELECT f.piece_id,p.version FROM json_each(?) selected "
                            "JOIN pipeline_piece_freezes f ON f.piece_id=selected.value "
                            "JOIN pipeline_pieces p ON p.id=f.piece_id WHERE p.workspace_id=? "
                            "ORDER BY f.seq DESC LIMIT 1", (_json(sources), workspace_id)).fetchone()
        if source is None:
            return None
        return dict(self._piece_freeze(db, source["piece_id"]), source_piece_id=source["piece_id"],
                    source_piece_version=source["version"])

    def _view_pipeline_freeze(self, db, actor, args):
        if set(args) != {"piece_id"}:
            raise Refusal("INVALID_ARGUMENT", "Name the piece whose freeze is read")
        piece, workspace = self._piece(db, actor, args.get("piece_id"))
        return {"instance_id": self.instance_id, "workspace_id": workspace["id"], "piece_id": piece["id"],
                "piece_revision": piece["version"], "freeze": self._piece_freeze(db, piece["id"])}

    def _command_pipeline_freeze(self, db, actor, envelope):
        target, args = envelope["target"], envelope["args"]
        if set(target) != {"piece_id"} or set(args) != {"base_commit", "head_commit", "files"} or "piece_revision" not in envelope["expected"]:
            raise Refusal("INVALID_ARGUMENT", "A freeze names its piece, version, exact base/head and changed files")
        piece, workspace = self._piece(db, actor, target["piece_id"])
        self._pipeline_on(db, workspace["id"])
        if piece["kind"] != "piece":
            raise Refusal("INVALID_ARGUMENT", "Freeze an item piece before combining its release")
        creator = db.execute("SELECT creator_id FROM topics WHERE id=?", (piece["topic_id"],)).fetchone()[0]
        if not self._steers(workspace, actor) and actor != creator:
            raise Refusal("FORBIDDEN", "The builder, coordinator or owner records this piece's freeze")
        self._check_revision(envelope, "piece_revision", piece["version"])
        entry = self._moving_entry(db, piece)
        if any(not isinstance(args[k], str) or SHA.fullmatch(args[k]) is None for k in ("base_commit", "head_commit")):
            raise Refusal("INVALID_ARGUMENT", "A freeze keeps the full base and head commits")
        files = args["files"]
        if not isinstance(files, list) or len(files) > 4096 or any(
                not isinstance(p, str) or not p or len(p) > 1024 or "\0" in p or "\\" in p or p.startswith("/") or
                any(part in (".", "..") for part in p.split("/")) or str(PurePosixPath(p)) != p for p in files):
            raise Refusal("INVALID_ARGUMENT", "Changed files are bounded relative repository paths")
        if len(set(files)) != len(files):
            raise Refusal("INVALID_ARGUMENT", "A changed file is named once")
        files = sorted(files)
        judged = bool(guarded(files))
        old = self._piece_freeze(db, piece["id"])
        same_source = old is not None and all(old[k] == args[k] for k in ("base_commit", "head_commit")) and old["files"] == files
        if same_source and old["judged"] == judged:
            return {"changed": False, "freeze": old, "piece_revision": piece["version"]}, []
        stages = [s for s in self._stages(db, workspace["id"]) if s["scope"] == "piece"]
        if old is not None and not same_source and stages and entry["stage_id"] == stages[-1]["stage_id"]:
            raise Refusal("PIECE_APPROVED", "Return this approved piece to review before changing its frozen source")
        version = piece["version"] + 1
        db.execute("UPDATE pipeline_pieces SET version=? WHERE id=?", (version, piece["id"]))
        db.execute("INSERT INTO pipeline_piece_freezes(piece_id,piece_revision,base_commit,head_commit,files,judged,by_actor,ms) "
                   "VALUES (?,?,?,?,?,?,?,?)", (piece["id"], version, args["base_commit"], args["head_commit"], _json(files),
                    int(judged), actor, _now_ms()))
        pipeline_version = self._bump(db, workspace["id"], piece["id"])
        return {"changed": True, "freeze": self._piece_freeze(db, piece["id"]), "piece_revision": version,
                "pipeline_revision": pipeline_version}, []
