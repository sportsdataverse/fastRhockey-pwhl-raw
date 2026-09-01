"""build_final_json port — raw payloads + processed sections + flat tables.

Processed sections (pbp / shifts / skaters / goalies) come from sdv-py's PWHL
surface (``pwhl_pbp`` / ``pwhl_game_shifts`` / ``pwhl_player_box`` — the
python siblings of the fastRhockey functions the R scraper used; pbp is a
documented enrichment superset of the R output). Every block degrades to a
missing field on error, which is why callers MUST route the result through
``client.merge_with_existing`` before overwriting a previously-captured game.
"""

from __future__ import annotations

from typing import Optional

from pwhl_raw.raw import build_raw_json

_SECTION_ORDER = [
    "pbp",
    "shifts",
    "skaters",
    "goalies",
    "game_info",
    "team_box",
    "scoring_summary",
    "penalty_summary",
    "three_stars",
    "officials",
    "shots_by_period",
    "shootout_summary",
    "game_rosters",
]


def _records(df) -> Optional[list[dict]]:
    if df is None:
        return None
    try:
        if df.height == 0:
            return None
        return df.to_dicts()
    except AttributeError:
        return None


def build_final_json(gid: int, raw_data: Optional[dict] = None) -> Optional[dict]:
    if raw_data is None:
        raw_data = build_raw_json(gid)
    if raw_data is None:
        return None

    final = dict(raw_data)

    # -- processed sections via sdv-py (fastRhockey siblings) --------------
    try:
        from sportsdataverse.pwhl import pwhl_pbp

        rec = _records(pwhl_pbp(game_id=gid))
        if rec:
            final["pbp"] = rec
    except Exception as exc:  # noqa: BLE001
        print(f"PBP pipeline failed for {gid}: {exc}")

    try:
        from sportsdataverse.pwhl import pwhl_game_shifts

        rec = _records(pwhl_game_shifts(game_id=gid))
        if rec:
            final["shifts"] = rec
    except Exception as exc:  # noqa: BLE001
        print(f"Shifts pipeline failed for {gid}: {exc}")

    try:
        from sportsdataverse.pwhl import pwhl_player_box

        box = pwhl_player_box(game_id=gid)
        if isinstance(box, dict):
            sk = _records(box.get("skaters"))
            gl = _records(box.get("goalies"))
            if sk:
                final["skaters"] = sk
            if gl:
                final["goalies"] = gl
    except Exception as exc:  # noqa: BLE001
        print(f"Player box failed for {gid}: {exc}")

    # -- flat tables parsed from raw_data (no extra API calls) -------------
    from pwhl_raw import parsers as P

    flat = {
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
    for key, fn in flat.items():
        try:
            rows = fn(raw_data, gid)
            if rows:
                final[key] = rows
        except Exception as exc:  # noqa: BLE001
            print(f"{key} parse failed for {gid}: {exc}")

    return final
