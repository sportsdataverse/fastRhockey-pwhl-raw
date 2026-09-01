"""Flat-parser parity vs committed final.json captures (perfect R oracles).

The committed ``pwhl/json/final/{gid}.json`` files carry BOTH the raw payloads
and the R-parsed flat sections, so each parser is validated against the real
R output for real games — columns exact, values exact (floats to 1e-3, R's
jsonlite rounds on write).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pwhl_raw import parsers as P

ROOT = Path(__file__).resolve().parents[1]
FIXTURE_GIDS = [18, 322]

FLAT = {
    "game_info": P.parse_game_info,
    "team_box": P.parse_team_box,
    "scoring_summary": P.parse_scoring_summary,
    "penalty_summary": P.parse_penalty_summary,
    "three_stars": P.parse_three_stars,
    "officials": P.parse_officials,
    "shots_by_period": P.parse_shots_by_period,
    "shootout_summary": P.parse_shootout,
    "game_rosters": P.parse_game_rosters,
}

RAW_KEYS = ("pbp_raw", "summary_raw", "gc_raw", "shifts_raw")


def _fixture(gid: int) -> dict:
    return json.loads((ROOT / "pwhl" / "json" / "final" / f"{gid}.json").read_text(encoding="utf-8"))


def _values_match(a, b) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, float) or isinstance(b, float):
        try:
            return abs(float(a) - float(b)) < 1e-3
        except (TypeError, ValueError):
            return False
    return a == b


@pytest.mark.parametrize("gid", FIXTURE_GIDS)
@pytest.mark.parametrize("key", sorted(FLAT))
def test_parser_matches_committed_r_output(gid, key):
    fx = _fixture(gid)
    raw = {k: fx.get(k) for k in RAW_KEYS}
    expected = fx.get(key)
    got = FLAT[key](raw, gid)

    if not expected:
        assert not got, f"{key}: R committed nothing but python produced {len(got)} rows"
        return
    assert got, f"{key}: R committed {len(expected)} rows but python produced none"
    assert len(got) == len(expected), f"{key}: row count {len(got)} != {len(expected)}"
    assert set(got[0]) == set(expected[0]), (
        f"{key}: columns diverge — py-only={set(got[0]) - set(expected[0])}, r-only={set(expected[0]) - set(got[0])}"
    )
    for i, (g, e) in enumerate(zip(got, expected)):
        for col in e:
            assert _values_match(g.get(col), e.get(col)), (
                f"{key} row {i} col {col!r}: py={g.get(col)!r} r={e.get(col)!r}"
            )
