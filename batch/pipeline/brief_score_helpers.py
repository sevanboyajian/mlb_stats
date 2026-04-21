"""Confidence-score helpers for brief rendering (used by generate_daily_brief)."""


def score_label(score: int) -> str:
    if score >= 9:
        return "HIGH CONFIDENCE — full stake"
    if score >= 7:
        return "MODERATE — half stake"
    if score >= 5:
        return "WATCH — quarter stake or pass"
    return "MONITOR ONLY — do not bet"


def best_pick_from_sigs(sigs: dict) -> dict | None:
    picks = sigs.get("picks") or []
    if not picks:
        return None
    return max(picks, key=lambda p: (-int(p.get("confidence_score") or 0), int(p.get("priority", 99))))


def entry_best_confidence(entry: dict) -> int:
    picks = (entry.get("sigs") or {}).get("picks") or []
    if not picks:
        return 0
    return max(int(p.get("confidence_score") or 0) for p in picks)


def entry_best_graded_confidence(entry: dict) -> int:
    graded = entry.get("graded") or []
    if not graded:
        return 0
    return max(int(p.get("confidence_score") or 0) for p in graded)


def sort_entries_by_pick_confidence(entries: list) -> None:
    entries.sort(key=lambda e: -entry_best_confidence(e))


def finding_bet_label(game: dict, finding) -> str:
    side = finding.bet_side
    if side == "away_ml":
        return f"{game.get('away_abbr', '')} ML"
    if side == "over_total":
        tot = game.get("total_line")
        return f"OVER {tot}" if tot is not None else "OVER"
    if side in ("under_total", "under"):
        tot = game.get("total_line")
        return f"UNDER {tot}" if tot is not None else "UNDER"
    return str(side)


def score_row_color_hex(score: int) -> str:
    if score >= 8:
        return "1F6E43"
    if score >= 6:
        return "D97706"
    return "DC2626"
