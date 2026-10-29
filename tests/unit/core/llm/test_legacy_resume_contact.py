"""Legacy stored resumes remain usable while new extraction schemas stay strict."""

from copy import deepcopy

import pytest
from pydantic import ValidationError

from core.llm.schema_models import ResumeSchema
from tests.unit.etl.test_resume_profiler import make_resume_schema


def test_missing_legacy_contact_loads_without_mutating_stored_payload():
    payload = make_resume_schema().model_dump()
    del payload['profile']['contact']
    original = deepcopy(payload)

    result = ResumeSchema.model_validate(payload)

    assert result.profile.contact.name is None
    assert result.profile.contact.email is None
    assert result.profile.contact.links == []
    assert payload == original
    assert result.profile.summary.text == payload['profile']['summary']['text']


def test_contact_remains_required_in_extraction_json_schema():
    schema = ResumeSchema.model_json_schema()
    assert 'contact' in schema['$defs']['Profile']['required']
    assert schema['$defs']['Profile']['additionalProperties'] is False


@pytest.mark.parametrize('contact', [None, [], 'invalid'])
def test_malformed_contact_is_not_silently_replaced(contact):
    payload = make_resume_schema().model_dump()
    payload['profile']['contact'] = contact
    with pytest.raises(ValidationError):
        ResumeSchema.model_validate(payload)
