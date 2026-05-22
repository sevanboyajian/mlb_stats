"""Flat 1-unit P&L from American odds and graded bet outcomes."""

from __future__ import annotations


def pnl_units_from_odds(odds: int | None, *, won: bool, push: bool) -> float:
    """Return P&L in units at flat 1u stake (push → 0, loss → -1)."""
    if push:
        return 0.0
    if not won:
        return -1.0
    if odds is None:
        return 1.0
    return (odds / 100.0) if odds > 0 else (100.0 / abs(odds))


def avoid_result_from_hypothesis(hypo: str) -> str:
    """Map counterfactual win/loss/push to good_avoid / bad_avoid / push_avoid."""
    if hypo == "push":
        return "push_avoid"
    if hypo == "win":
        return "bad_avoid"
    return "good_avoid"


def grade_moneyline_side(
    *,
    team: str,
    home_abbr: str,
    away_abbr: str,
    home_score: int,
    away_score: int,
    odds: int | None,
) -> tuple[str, float] | None:
    if team == home_abbr:
        won = home_score > away_score
    elif team == away_abbr:
        won = away_score > home_score
    else:
        return None
    result = "win" if won else "loss"
    pnl = pnl_units_from_odds(odds, won=won, push=False)
    return result, pnl


def grade_total_side(
    *,
    side: str,
    line: float,
    runs: int,
    odds: int | None,
) -> tuple[str, float]:
    if runs == line:
        return "push", pnl_units_from_odds(odds, won=False, push=True)
    won = (side == "over" and runs > line) or (side == "under" and runs < line)
    result = "win" if won else "loss"
    pnl = pnl_units_from_odds(odds, won=won, push=False)
    return result, pnl


def grade_runline_side(
    *,
    team: str,
    line: float,
    home_abbr: str,
    away_abbr: str,
    home_score: int,
    away_score: int,
    odds: int | None,
) -> tuple[str, float] | None:
    if team == home_abbr:
        adj = home_score + line
        opp = away_score
    elif team == away_abbr:
        adj = away_score + line
        opp = home_score
    else:
        return None
    if adj == opp:
        return "push", pnl_units_from_odds(odds, won=False, push=True)
    won = adj > opp
    result = "win" if won else "loss"
    pnl = pnl_units_from_odds(odds, won=won, push=False)
    return result, pnl


def counterfactual_hypothesis(
    *,
    market: str,
    bet_text: str,
    home_abbr: str,
    away_abbr: str,
    home_score: int,
    away_score: int,
    total_line: float | None,
) -> str | None:
    """Return win / loss / push for the bet that was avoided (no P&L)."""
    from core.grading.parsing import parse_ml_team, parse_runline_bet, parse_total_bet

    runs = home_score + away_score
    if market == "moneyline":
        team = parse_ml_team(bet_text)
        if not team:
            return None
        if team == home_abbr:
            won = home_score > away_score
        elif team == away_abbr:
            won = away_score > home_score
        else:
            return None
        return "win" if won else "loss"
    if market == "total":
        side, parsed_line = parse_total_bet(bet_text)
        if side is None:
            return None
        line = total_line if total_line is not None else parsed_line
        if line is None:
            return None
        if runs == line:
            return "push"
        won = (side == "over" and runs > line) or (side == "under" and runs < line)
        return "win" if won else "loss"
    if market in ("spread", "runline"):
        team, line = parse_runline_bet(bet_text)
        if team is None or line is None:
            return None
        if team == home_abbr:
            adj = home_score + line
            opp = away_score
        elif team == away_abbr:
            adj = away_score + line
            opp = home_score
        else:
            return None
        if adj == opp:
            return "push"
        won = adj > opp
        return "win" if won else "loss"
    return None
