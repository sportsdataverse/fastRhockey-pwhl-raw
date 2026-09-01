"""Flat-dataset parsers — key-for-key port of the R ``.parse_*`` functions.

Inputs are the parsed raw payload dict (``build_raw_json`` shape). Each parser
returns ``list[dict]`` (records, R data.frame parity) or ``None``; all tolerate
missing fields. ``None``/missing scalar values become ``None`` (R ``NA`` →
json ``null``).
"""

from __future__ import annotations

from typing import Any, Optional


def _get(x: Any, *keys: str, default: Any = None) -> Any:
    for k in keys:
        if not isinstance(x, dict):
            return default
        x = x.get(k)
    return x if x is not None else default


def _as_int(x: Any, default: Optional[int] = None) -> Optional[int]:
    if x is None or x == "":
        return default
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return default


def _as_num(x: Any, default: Optional[float] = None) -> Optional[float]:
    if x is None or x == "":
        return default
    try:
        return float(x)
    except (TypeError, ValueError):
        return default


def _as_chr(x: Any, default: Optional[str] = None) -> Optional[str]:
    # R parity: .as_chr("") keeps "" (only NULL becomes NA/None).
    if x is None:
        return default
    return str(x)


def parse_game_info(raw: dict, gid: int) -> list[dict]:
    s = raw.get("summary_raw")
    if s is None:
        return [{"game_id": int(gid)}]
    d = s.get("details") or {}
    ht = _get(s, "homeTeam", "info", default={}) or {}
    vt = _get(s, "visitingTeam", "info", default={}) or {}
    hs = _get(s, "homeTeam", "stats", default={}) or {}
    vs = _get(s, "visitingTeam", "stats", default={}) or {}
    return [
        {
            "game_id": int(gid),
            "game_number": _as_chr(d.get("gameNumber")),
            "game_date": _as_chr(d.get("date")),
            "game_date_iso": _as_chr(d.get("GameDateISO8601")),
            "start_time": _as_chr(d.get("startTime")),
            "end_time": _as_chr(d.get("endTime")),
            "game_duration": _as_chr(d.get("duration")),
            "game_venue": _as_chr(d.get("venue")),
            "attendance": _as_int(d.get("attendance"), 0),
            "game_status": _as_chr(d.get("status")),
            "game_season_id": _as_int(d.get("seasonId")),
            "started": _as_int(d.get("started"), 0),
            "final": _as_int(d.get("final"), 0),
            "home_team_id": _as_int(ht.get("id")),
            "home_team": _as_chr(ht.get("name")),
            "home_team_abbr": _as_chr(ht.get("abbreviation")),
            "home_score": _as_int(hs.get("goals"), 0),
            "away_team_id": _as_int(vt.get("id")),
            "away_team": _as_chr(vt.get("name")),
            "away_team_abbr": _as_chr(vt.get("abbreviation")),
            "away_score": _as_int(vs.get("goals"), 0),
            "has_shootout": _as_int(s.get("hasShootout"), 0),
            "game_report_url": _as_chr(d.get("gameReportUrl")),
            "boxscore_url": _as_chr(d.get("textBoxscoreUrl")),
        }
    ]


def parse_team_box(raw: dict, gid: int) -> Optional[list[dict]]:
    s = raw.get("summary_raw")
    if s is None:
        return None

    def one(side: Optional[dict], side_label: str) -> dict:
        side = side or {}
        info = side.get("info") or {}
        stats = side.get("stats") or {}
        rec = _get(side, "seasonStats", "teamRecord", default={}) or {}
        return {
            "game_id": int(gid),
            "team_id": _as_int(info.get("id")),
            "team": _as_chr(info.get("name")),
            "team_abbr": _as_chr(info.get("abbreviation")),
            "team_side": side_label,
            "shots": _as_int(stats.get("shots"), 0),
            "goals": _as_int(stats.get("goals"), 0),
            "hits": _as_int(stats.get("hits"), 0),
            "pp_goals": _as_int(stats.get("powerPlayGoals"), 0),
            "pp_opportunities": _as_int(stats.get("powerPlayOpportunities"), 0),
            "goal_count": _as_int(stats.get("goalCount"), 0),
            "assist_count": _as_int(stats.get("assistCount"), 0),
            "penalty_minutes": _as_int(stats.get("penaltyMinuteCount"), 0),
            "infraction_count": _as_int(stats.get("infractionCount"), 0),
            "faceoff_attempts": _as_int(stats.get("faceoffAttempts"), 0),
            "faceoff_wins": _as_int(stats.get("faceoffWins"), 0),
            "faceoff_win_pct": _as_num(stats.get("faceoffWinPercentage")),
            "season_wins": _as_int(rec.get("wins"), 0),
            "season_losses": _as_int(rec.get("losses"), 0),
            "season_ot_wins": _as_int(rec.get("OTWins"), 0),
            "season_ot_losses": _as_int(rec.get("OTLosses"), 0),
            "season_so_losses": _as_int(rec.get("SOLosses"), 0),
            "season_record": _as_chr(rec.get("formattedRecord")),
        }

    return [one(s.get("homeTeam"), "home"), one(s.get("visitingTeam"), "away")]


def parse_scoring_summary(raw: dict, gid: int) -> Optional[list[dict]]:
    periods = _get(raw, "summary_raw", "periods")
    if not periods:
        return None
    rows: list[dict] = []
    for p in periods:
        pinfo = p.get("info") or {}
        for g in p.get("goals") or []:
            team = g.get("team") or {}
            scorer = g.get("scoredBy") or {}
            props = g.get("properties") or {}
            assists = g.get("assists") or []
            a1 = assists[0] if len(assists) >= 1 else {}
            a2 = assists[1] if len(assists) >= 2 else {}
            rows.append(
                {
                    "game_id": int(gid),
                    "period_id": _as_int(pinfo.get("id")),
                    "period": _as_chr(pinfo.get("longName")),
                    "time": _as_chr(g.get("time")),
                    "team_id": _as_int(team.get("id")),
                    "team": _as_chr(team.get("name")),
                    "team_abbr": _as_chr(team.get("abbreviation")),
                    "game_goal_id": _as_int(g.get("game_goal_id")),
                    "scorer_goal_number": _as_int(g.get("scorerGoalNumber")),
                    "scorer_id": _as_int(scorer.get("id")),
                    "scorer_first": _as_chr(scorer.get("firstName")),
                    "scorer_last": _as_chr(scorer.get("lastName")),
                    "scorer_position": _as_chr(scorer.get("position")),
                    "assist_1_id": _as_int(a1.get("id")),
                    "assist_1_first": _as_chr(a1.get("firstName")),
                    "assist_1_last": _as_chr(a1.get("lastName")),
                    "assist_2_id": _as_int(a2.get("id")),
                    "assist_2_first": _as_chr(a2.get("firstName")),
                    "assist_2_last": _as_chr(a2.get("lastName")),
                    "is_power_play": _as_int(_get(props, "isPowerPlay"), 0),
                    "is_short_handed": _as_int(_get(props, "isShortHanded"), 0),
                    "is_empty_net": _as_int(_get(props, "isEmptyNet"), 0),
                    "is_penalty_shot": _as_int(_get(props, "isPenaltyShot"), 0),
                    "is_insurance": _as_int(_get(props, "isInsuranceGoal"), 0),
                    "is_game_winning": _as_int(_get(props, "isGameWinningGoal"), 0),
                    "x_location": _as_num(g.get("xLocation")),
                    "y_location": _as_num(g.get("yLocation")),
                }
            )
    return rows or None


def parse_penalty_summary(raw: dict, gid: int) -> Optional[list[dict]]:
    periods = _get(raw, "summary_raw", "periods")
    if not periods:
        return None
    rows: list[dict] = []
    for p in periods:
        pinfo = p.get("info") or {}
        for pen in p.get("penalties") or []:
            against = pen.get("againstTeam") or {}
            taken = pen.get("takenBy") or {}
            served = pen.get("servedBy") or {}
            rows.append(
                {
                    "game_id": int(gid),
                    "period_id": _as_int(pinfo.get("id")),
                    "period": _as_chr(pinfo.get("longName")),
                    "time": _as_chr(pen.get("time")),
                    "team_id": _as_int(against.get("id")),
                    "team": _as_chr(against.get("name")),
                    "team_abbr": _as_chr(against.get("abbreviation")),
                    "game_penalty_id": _as_int(pen.get("game_penalty_id")),
                    "minutes": _as_num(pen.get("minutes")),
                    "description": _as_chr(pen.get("description")),
                    "rule_number": _as_chr(pen.get("ruleNumber")),
                    "is_power_play": int(pen.get("isPowerPlay") is True),
                    "is_bench": int(pen.get("isBench") is True),
                    "taken_by_id": _as_int(taken.get("id")),
                    "taken_by_first": _as_chr(taken.get("firstName")),
                    "taken_by_last": _as_chr(taken.get("lastName")),
                    "taken_by_position": _as_chr(taken.get("position")),
                    "served_by_id": _as_int(served.get("id")),
                    "served_by_first": _as_chr(served.get("firstName")),
                    "served_by_last": _as_chr(served.get("lastName")),
                }
            )
    return rows or None


def parse_three_stars(raw: dict, gid: int) -> Optional[list[dict]]:
    mvps = _get(raw, "summary_raw", "mostValuablePlayers")
    if not mvps:
        return None
    rows: list[dict] = []
    for i, m in enumerate(mvps, start=1):
        team = m.get("team") or {}
        pinfo = _get(m, "player", "info", default={}) or {}
        pstats = _get(m, "player", "stats", default={}) or {}
        rows.append(
            {
                "game_id": int(gid),
                "star": i,
                "team_id": _as_int(team.get("id")),
                "team": _as_chr(team.get("name")),
                "team_abbr": _as_chr(team.get("abbreviation")),
                "player_id": _as_int(pinfo.get("id")),
                "first_name": _as_chr(pinfo.get("firstName")),
                "last_name": _as_chr(pinfo.get("lastName")),
                "jersey_number": _as_int(pinfo.get("jerseyNumber")),
                "position": _as_chr(pinfo.get("position")),
                "is_goalie": int(m.get("isGoalie") is True),
                "is_home": _as_int(m.get("homeTeam"), 0),
                "goals": _as_int(pstats.get("goals"), 0),
                "assists": _as_int(pstats.get("assists"), 0),
                "points": _as_int(pstats.get("points"), 0),
                # R parity: `pstats$shots` partial-matches "shotsAgainst" for
            # goalies (R list `$` prefix matching) — port bug-for-bug.
            "shots": _as_int(
                pstats.get("shots", pstats.get("shotsAgainst")), 0
            ),
                "saves": _as_int(pstats.get("saves"), 0),
                "shots_against": _as_int(pstats.get("shotsAgainst"), 0),
                "goals_against": _as_int(pstats.get("goalsAgainst"), 0),
                "time_on_ice": _as_chr(pstats.get("toi") or pstats.get("timeOnIce")),
            }
        )
    return rows


def parse_officials(raw: dict, gid: int) -> Optional[list[dict]]:
    s = raw.get("summary_raw") or {}

    def collect(lst: Any, role: str) -> list[dict]:
        return [
            {
                "game_id": int(gid),
                "role": role,
                "first_name": _as_chr(o.get("firstName")),
                "last_name": _as_chr(o.get("lastName")),
                "jersey_number": _as_int(o.get("jerseyNumber")),
                "official_role": _as_chr(o.get("role")),
            }
            for o in (lst or [])
        ]

    out = (
        collect(s.get("referees"), "Referee")
        + collect(s.get("linesmen"), "Linesperson")
        + collect(s.get("scorekeepers"), "Scorekeeper")
    )
    return out or None


def parse_shots_by_period(raw: dict, gid: int) -> Optional[list[dict]]:
    periods = _get(raw, "summary_raw", "periods")
    if not periods:
        return None
    rows = []
    for p in periods:
        pinfo = p.get("info") or {}
        pstat = p.get("stats") or {}
        rows.append(
            {
                "game_id": int(gid),
                "period_id": _as_int(pinfo.get("id")),
                "period": _as_chr(pinfo.get("longName")),
                "home_goals": _as_int(pstat.get("homeGoals"), 0),
                "home_shots": _as_int(pstat.get("homeShots"), 0),
                "away_goals": _as_int(pstat.get("visitingGoals"), 0),
                "away_shots": _as_int(pstat.get("visitingShots"), 0),
            }
        )
    return rows


def parse_shootout(raw: dict, gid: int) -> Optional[list[dict]]:
    s = raw.get("summary_raw")
    if not s or s.get("hasShootout") is not True:
        return None
    ps = s.get("penaltyShots")
    if not ps:
        return None

    def collect(lst: Any, side: str) -> list[dict]:
        rows = []
        for i, sh in enumerate(lst or [], start=1):
            shooter = sh.get("shooter") or sh.get("player") or {}
            goalie = sh.get("goalie") or {}
            rows.append(
                {
                    "game_id": int(gid),
                    "round": i,
                    "team_side": side,
                    "shooter_id": _as_int(shooter.get("id")),
                    "shooter_first": _as_chr(shooter.get("firstName")),
                    "shooter_last": _as_chr(shooter.get("lastName")),
                    "goalie_id": _as_int(goalie.get("id")),
                    "goalie_first": _as_chr(goalie.get("firstName")),
                    "goalie_last": _as_chr(goalie.get("lastName")),
                    "is_goal": int(sh.get("isGoal") is True),
                }
            )
        return rows

    out = collect(ps.get("homeTeam"), "home") + collect(ps.get("visitingTeam"), "away")
    return out or None


def parse_game_rosters(raw: dict, gid: int) -> Optional[list[dict]]:
    s = raw.get("summary_raw")
    if s is None:
        return None

    def side(team: Optional[dict], side_label: str) -> list[dict]:
        team = team or {}
        info = team.get("info") or {}
        team_id = _as_int(info.get("id"))
        team_nm = _as_chr(info.get("name"))
        team_ab = _as_chr(info.get("abbreviation"))

        def one(p: dict, kind: str) -> dict:
            pinfo = p.get("info") or {}
            return {
                "game_id": int(gid),
                "team_id": team_id,
                "team": team_nm,
                "team_abbr": team_ab,
                "team_side": side_label,
                "player_type": kind,
                "player_id": _as_int(pinfo.get("id")),
                "first_name": _as_chr(pinfo.get("firstName")),
                "last_name": _as_chr(pinfo.get("lastName")),
                "jersey_number": _as_int(pinfo.get("jerseyNumber")),
                "position": _as_chr(pinfo.get("position")),
                "birth_date": _as_chr(pinfo.get("birthDate")),
                "starting": _as_int(p.get("starting"), 0),
                "status": _as_chr(p.get("status")),
            }

        return [one(p, "skater") for p in team.get("skaters") or []] + [
            one(p, "goalie") for p in team.get("goalies") or []
        ]

    out = side(s.get("homeTeam"), "home") + side(s.get("visitingTeam"), "away")
    return out or None
