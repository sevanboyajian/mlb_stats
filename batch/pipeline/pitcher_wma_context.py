"""Context-aware home SP ERA WMA resolution for gate evaluation (Open Item #30)."""

from __future__ import annotations

WMA_MIN_HOME_SPLIT_STARTS = 3


def home_sp_era_for_gate(home_sp: object | None) -> tuple[float | None, int, str]:
    """
    Return (era, starts_in_window, source) for home-SP gates.

    source is one of: 'home_split', 'aggregate_fallback', 'missing'
    """
    if home_sp is None:
        return None, 0, "missing"

    home_era = getattr(home_sp, "era_wma_home", None)
    home_starts = int(getattr(home_sp, "starts_in_window_home", 0) or 0)
    if home_era is not None and home_starts >= WMA_MIN_HOME_SPLIT_STARTS:
        return float(home_era), home_starts, "home_split"

    agg_era = getattr(home_sp, "era_wma", None)
    agg_starts = int(getattr(home_sp, "starts_in_window", 0) or 0)
    if agg_era is not None:
        return float(agg_era), agg_starts, "aggregate_fallback"

    return None, 0, "missing"


def format_home_sp_era_label(source: str) -> str:
    if source == "home_split":
        return "home-context"
    if source == "aggregate_fallback":
        return "aggregate, insufficient home sample"
    return "missing"


def format_home_sp_data_line(
    era: float,
    *,
    source: str,
    starts: int,
    gate_desc: str,
) -> str:
    ctx = format_home_sp_era_label(source)
    n_note = f", n={starts} home starts" if source == "home_split" else ""
    return f"home SP ERA WMA ({ctx}) {era:.2f} ({gate_desc}{n_note})"
