# Gauntlet classification and release facts (994)

Intake: Warp14926 assigns994 after1000, now verified shipped296 at
df17f2a8c7a96096dea56e7ce5b264c53589849e. Warp14936 settles medium sizing, build NOW on corrected57b982c2, freeze against
that head, land only after987 is live and carry then. This supersedes14640’s
earlier build timing. Rebase once if987’s approved head changes. The actual994
record is medium Now/Build1, bound by Fable14939 to piece
p-010e462dc6c64ca5b1958851 in the existing topic.

New own preparation tree:
/Users/commonfabric/aim-worktrees/core-lane8-994-gauntlet-residuals-98757,
branch core-lane8/994-gauntlet-residuals-98757. Its exact build/freeze base is
approved corrected98757b982c2ab208ed86d1fea6ebbabfc8969b99560, not superseded
45ecc17f and not an installed987 claim. Current live296 lacks these new987
modules. Warp14936/Fable14939 authorize this build now; carry waits for987
actually live. Preserve all other trees, including frozen1000 and987.

## One piece, five recorded residuals

1. Re-derive classification when freezing the same base/head/files. A true
   no-op includes the newly derived classification. A changed classification
   appends provenance even while the piece is approved when its frozen source
   is unchanged; a source change still requires return to review. Permissions,
   revision fences, append-only history and command transaction stay authoritative.
2. Before combining more than one item, every selected piece has a source-bound
   freeze. Count actual distinct member items, including several within one
   piece. Refuse missing classification before creating a release or leaving
   any stage. Keep judged-plus-anything refusal and existing one-item semantics.
3. The release view retains the Judged mark from the newest relevant freeze of
   its combined members. Derive it from the existing freeze record rather than
   storing a competing release-only fact. Unfrozen work gets no invented mark.
   The existing combine fact supplies the exact source IDs; the derived view
   names source_piece_id/source_piece_version because the freeze revision
   belongs to that original piece, not the new release's version. Both actual
   work and Next release renderers consume that same read field.
4. Pre-landing admission also accounts for an ordinary combined release still
   in release stages through facts.next_release. Read complete, stable scoped
   pipeline facts and name blockers. Preserve ordinary heads’ independent path
   and the authenticated already-started-train exception fixed in57b982c2.
   next_release is required (null means none); an open release must match the
   scoped release_piece owner, members, stage, number and stable page version.
5. Move the DEPLOY_TOOL rationale to its actual owner aim_next/judgment.py;
   share the actual source-diff read and SHA pattern, and remove repeated
   combine validation. Retain ancestry, source-bound classification, refusal
   behavior, bounded complete reads and transaction ordering. No installed
   guard, launcher, host policy or workflow edits.

The existing source owners are aim_next/piece_freeze.py, pipeline.py and
judgment.py, plus tools/gauntlet_admission.py and train.py. No schema addition
is expected; retain987’s61 and the separately owned757 successor. Fable owns
item/stage binding. Warp owns source composition, carry and landing.

## Classification is a fact of the actual freeze

994’s source note calls this an ordinary piece and14640 says ordinary paths.
Its recorded scope nevertheless names tools/gauntlet_admission.py, train.py
and aim_next/judgment.py, all present in unchanged DEPLOY_TOOL. Do not remove
protection or force an ordinary flag to match prose. Warp14936 resolved this crossing: actual final files derive classification,
tools/Core changes make it judged, and either way it holds no ordinary work.
The actual final diff and Core freeze derive the mark.
No planned-path declaration substitutes for that record.

## Meaningful checks and defaults

Use real isolated Store/CLI command/view fixtures and scratch Git repositories;
no live accounts, Core database writes or external dispatch. Keep actual reds
for classification drift, approved re-freeze, missing multi-item freeze,
release-card mark and open-release admission. Verify unchanged-source no-op,
changed-source refusal, permissions/revisions, atomic rollback, multi-item
single-piece handling, lone cases, complete paging and started-landing behavior.
Exercise the actual pipeline/card owner for the same Core freeze fact.

Use the existing diff/classifier and admission owners, not copied product
bodies in tests or page-only facts. Apply, catch and byte-exact restore the
load-bearing classification/combine/admission mutations, with a distinct
PYTHONPYCACHEPREFIX per Python mutant and restored control. Run one relevant
substantial workload at a time; preserve diagnostics and compare affected
predecessor failures without claiming a broad green suite.

Freeze meaningful green, push only this exact own branch and report head,
files/pass/fail for Warp’s one independent read. Actual source classification
controls the landing route; it is not waived by the intake wording. Native
proof is required only if the changed user interaction needs it, with the
standing isolated-browser notice and verified cleanup. No new approval gate,
old sealed workflow, timing from fake DOM or installation claim.
