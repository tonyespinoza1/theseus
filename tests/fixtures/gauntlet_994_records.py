"""Fresh isolated Core page answers for the real PipelinePage renderer."""
import json
from pathlib import Path
import sys

source = Path(sys.argv[1]).resolve()
sys.path[:0] = [str(source), str(source / 'tests')]
from test_gauntlet_987 import Freeze987

records = {}
for classification in ('judged', 'ordinary', 'unclassified'):
    case = Freeze987('test_987_lone_judged_item_combines')
    case.setUp()
    scratch = Path(case.temp.name)
    try:
        piece = case.piece([1])
        if classification != 'unclassified':
            case.committed(case.freeze(piece, None if classification == 'judged' else ['web/example.mjs']))
        def pages():
            return {section: case.store.view('agent-b', 'pipeline', {'workspace_id': 'w1', 'section': section})
                    for section in ('now', 'next', 'later', 'candidates', 'dropped', 'shipped')}
        records[classification + '_work'] = pages()
        case.wait(piece)
        case.committed(case.combine([piece]))
        records[classification + '_release'] = pages()
    finally:
        case.doCleanups()
    assert not scratch.exists(), 'the scratch Core store was not removed'
print(json.dumps(records))
