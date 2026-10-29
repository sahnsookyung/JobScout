"""An empty extracted unit must not poison a whole provider embedding batch."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from database.repositories.job_post import JobPostRepository


@pytest.mark.parametrize('invalid', ['', ' \n\t', '\u3000', None, 123])
def test_save_requirements_skips_empty_units_and_preserves_valid_content(invalid):
    db = MagicMock()
    repo = JobPostRepository(db)
    job = SimpleNamespace(id='job-id')
    valid = '  3 years Python experience  '

    repo.save_requirements(job, [
        {'text': invalid, 'req_type': 'must_have'},
        {'req_type': 'must_have'},
        {'text': valid, 'req_type': 'nice_to_have', 'ordinal': 7},
    ])

    db.add.assert_called_once()
    row = db.add.call_args.args[0]
    assert row.text == valid
    assert row.req_type == 'preferred'
    assert row.ordinal == 7
    assert row.min_years == 3
    db.flush.assert_called_once()
