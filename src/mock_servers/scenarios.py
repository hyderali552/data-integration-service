"""Named failure scenarios for the mock world.

Source modes:   normal | down (503) | always_429 | slow | flaky (fails first N
                hits per page, then recovers) | fail_after_page_1 (5xx from
                page 2) | malformed_json | bad_shape | pagination_loop | empty
Downstream:     ok | timeout_once (accepts then responds too slowly, first
                time each key) | always_timeout | flaky_503 | reject_400 | down
`unreachable_sources` simulates TCP-level connection failure (in-process only).
"""
from __future__ import annotations

from dataclasses import dataclass, field, replace


@dataclass(frozen=True)
class Scenario:
    sources: dict = field(default_factory=lambda: {"a": "normal", "b": "normal", "c": "normal"})
    downstream: str = "ok"
    unreachable_sources: tuple = ()
    slow_seconds: float = 3.0          # must exceed the client timeout to trigger one
    downstream_delay: float = 3.0
    retry_after_seconds: float = 0.05  # kept tiny so demos are quick
    flaky_failures: int = 2

    def with_(self, **kw) -> "Scenario":
        return replace(self, **kw)


PRESETS = {
    "happy": Scenario(),
    # Everything bad at once: B dies after page 1, C is rate-limited forever,
    # downstream accepts-then-times-out. A and the CSV are healthy.
    "demo": Scenario(sources={"a": "normal", "b": "fail_after_page_1", "c": "always_429"},
                     downstream="timeout_once"),
    "source-a-down": Scenario(unreachable_sources=("a",)),
    "source-a-503": Scenario(sources={"a": "down", "b": "normal", "c": "normal"}),
    "slow-source": Scenario(sources={"a": "slow", "b": "normal", "c": "normal"}),
    "flaky": Scenario(sources={"a": "flaky", "b": "flaky", "c": "flaky"}, downstream="flaky_503"),
    "downstream-timeout": Scenario(downstream="timeout_once"),
    "downstream-down": Scenario(downstream="down"),
    "downstream-reject": Scenario(downstream="reject_400"),
    "pagination-loop": Scenario(sources={"a": "pagination_loop", "b": "normal", "c": "normal"}),
}
