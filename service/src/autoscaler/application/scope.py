"""What one run covers: the clusters from the manifest and the rules to apply to them."""
from __future__ import annotations

from dataclasses import dataclass

from autoscaler.application.ports import ReadObject
from autoscaler.domain.models import ClusterSpec, Rule
from autoscaler.parsing.manifest import parse_manifest
from autoscaler.parsing.rules_file import parse_rules


@dataclass(frozen=True)
class InputLocations:
    """Run's configuration objects location in the object storage."""
    bucket: str
    manifest_key: str
    rules_key: str


@dataclass(frozen=True)
class RunScope:
    """Scope, fixed for the whole run. I defines the target clesters to evaluate and the rules to evaluate by."""
    clusters: tuple[ClusterSpec, ...]
    rules: tuple[Rule, ...]  # in file order: first-match-wins depends on it


def load_scope(read_object: ReadObject, locations: InputLocations) -> RunScope:
    """Reads and validates the manifest and rules. Raises ValueError if either is malformed, 
    as a run can't proceed without these."""
    return RunScope(
        clusters=tuple(parse_manifest(read_object(locations.bucket, locations.manifest_key))),
        rules=tuple(parse_rules(read_object(locations.bucket, locations.rules_key))),
    )
