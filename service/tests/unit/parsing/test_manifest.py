import json

import pytest

from autoscaler.parsing.manifest import parse_manifest


@pytest.mark.parametrize(
    "payload",
    [{"clusters": []}, {"other": 1}, [], {"clusters": [{"cluster_id": "a"}]}],
    ids=["empty", "no-clusters-key", "not-an-object", "missing-fields"],
)
def test_malformed_manifest_fails_loudly(payload):
    with pytest.raises(ValueError):
        parse_manifest(json.dumps(payload).encode())
