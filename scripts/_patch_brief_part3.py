"""
Apply Part 3 brief patches to batch/pipeline/generate_daily_brief.py.

Requires: batch/pipeline/brief_score_helpers.py (committed).
Close generate_daily_brief.py in the editor, then run from repo root:
  python scripts/_patch_brief_part3.py
"""
from __future__ import annotations

import os
import pathlib
import shutil

ROOT = pathlib.Path(__file__).resolve().parent.parent
P = ROOT / "batch" / "pipeline" / "generate_daily_brief.py"

IMPORT_BLOCK = """
from batch.pipeline.brief_score_helpers import (
    best_pick_from_sigs,
    entry_best_confidence,
    entry_best_graded_confidence,
    finding_bet_label,
    score_label,
    score_row_color_hex,
    sort_entries_by_pick_confidence,
)

"""


def main() -> None:
    t = P.read_text(encoding="utf-8")
    if "brief_score_helpers" not in t:
        needle = "from core.db.connection import connect as db_connect, get_db_path\n\n# ── Optional .env support"
        if needle not in t:
            raise SystemExit("import anchor not found")
        t = t.replace(needle, "from core.db.connection import connect as db_connect, get_db_path\n" + IMPORT_BLOCK + "\n# ── Optional .env support", 1)

    a_ev = (
        "        out = scored_game_to_eval_dict(scored, session)\n"
        '        out["output_tier"] = scored.output_tier\n'
    )
    b_ev = (
        "        out = scored_game_to_eval_dict(scored, session)\n"
        '        out["scored"] = scored\n'
        '        out["output_tier"] = scored.output_tier\n'
    )
    if a_ev in t and 'out["scored"]' not in t:
        t = t.replace(a_ev, b_ev, 1)

    a_ex = '            "stake_multiplier": 0.0,\n        }\n'
    b_ex = '            "stake_multiplier": 0.0,\n            "scored": None,\n        }\n'
    if '"scored": None' not in t and a_ex in t:
        t = t.replace(a_ex, b_ex, 1)

    t = t.replace(
        """    def _best_pick_fields(entry: dict) -> tuple[str | None, str | None, int | None]:
        try:
            p = sorted(entry["sigs"]["picks"], key=lambda x: x["priority"])[0]
""",
        """    def _best_pick_fields(entry: dict) -> tuple[str | None, str | None, int | None]:
        try:
            p = best_pick_from_sigs(entry["sigs"]) or {}
""",
        1,
    )

    t = t.replace(
        '            p   = sorted(entry["sigs"]["picks"], key=lambda x: x["priority"])[0]\n',
        '            p = best_pick_from_sigs(entry["sigs"]) or {}\n',
        1,
    )

    t = t.replace(
        '            # Use highest-priority pick for this game\n'
        '            p = sorted(entry["sigs"]["picks"], key=lambda x: x["priority"])[0]\n',
        '            # Use highest-confidence active pick for this game\n'
        '            p = best_pick_from_sigs(entry["sigs"]) or {}\n',
        1,
    )

    t = t.replace(
        "            all_picks_entries.sort(key=lambda e: min(p[\"priority\"] for p in e[\"sigs\"][\"picks\"]))\n",
        "            sort_entries_by_pick_confidence(all_picks_entries)\n",
    )

    old_loop = """    all_picks   = []
    avoid_games = []
    no_signal   = []

    for game in games:
        sigs = evaluate_signals(
            conn, game, streaks, "primary", starters,
            verbose=verbose, debug_wind=debug_wind,
        )
        entry = {
            "game":    game,
            "sigs":    sigs,
            "starter": starter_line(game, starters),
            "streak":  streak_line(game, streaks),
        }
        if sigs["picks"]:
            all_picks.append(entry)
        elif sigs["avoid"]:
            avoid_games.append(entry)
        else:
            no_signal.append(entry)

    # Sort picks by priority (lower = higher priority)
    all_picks.sort(key=lambda e: min(p["priority"] for p in e["sigs"]["picks"]))

"""
    new_loop = """    all_picks   = []
    avoid_games = []
    no_signal   = []
    all_entries: list = []

    for game in games:
        sigs = evaluate_signals(
            conn, game, streaks, "primary", starters,
            verbose=verbose, debug_wind=debug_wind,
        )
        entry = {
            "game":    game,
            "sigs":    sigs,
            "starter": starter_line(game, starters),
            "streak":  streak_line(game, streaks),
        }
        all_entries.append(entry)
        if sigs["picks"]:
            all_picks.append(entry)
        elif sigs["avoid"]:
            avoid_games.append(entry)
        else:
            no_signal.append(entry)

    # Sort games by highest active-pick confidence
    sort_entries_by_pick_confidence(all_picks)

"""
    if "all_entries: list = []" not in t:
        if old_loop not in t:
            raise SystemExit("build_primary loop not found")
        t = t.replace(old_loop, new_loop, 1)

    old_top = """        p   = sorted(top["sigs"]["picks"], key=lambda x: x["priority"])[0]

        lines.append(f"\\n  {matchup_line(g)}")
        lines.append(f"  {weather_line(g)}")
        lines.append(f"  {top['starter']}")
        lines.append(f"  {top['streak']}")
        # Movement alert — compare vs earliest prior session pick today
        alert = movement_alert(conn, game_date, session,
                               g["game_pk"], g.get("total_line"), g.get("home_ml"))
        if alert:
            lines.append("")
            lines.append(alert)
        lines.append(f"\\n  ┌─────────────────────────────────────────────────────────┐")
        lines.append(f"  │  BET:     {p['bet']:<20}  ODDS: {p['odds']:<8}        │")
        lines.append(f"  │  SIGNAL:  {', '.join(top['sigs']['signals']):<47}  │")
        lines.append(f"  └─────────────────────────────────────────────────────────┘")
        lines.append(f"\\n  {odds_summary_line(g)}")
        lines.append(f"\\n  REASON: {textwrap.fill(p['reason'], width=66, subsequent_indent='          ')}")
"""
    new_top = """        p = best_pick_from_sigs(top["sigs"]) or {}
        sc = int(p.get("confidence_score") or 0)
        sid = str(p.get("signal_id") or ", ".join(top["sigs"]["signals"]))
        sbasis = str(p.get("score_basis") or "")

        lines.append(f"\\n  {matchup_line(g)}")
        lines.append(f"  {weather_line(g)}")
        lines.append(f"  {top['starter']}")
        lines.append(f"  {top['streak']}")
        # Movement alert — compare vs earliest prior session pick today
        alert = movement_alert(conn, game_date, session,
                               g["game_pk"], g.get("total_line"), g.get("home_ml"))
        if alert:
            lines.append("")
            lines.append(alert)
        lines.append(f"\\n  ┌─────────────────────────────────────────────────────────┐")
        lines.append(f"  │  BET:     {str(p.get('bet') or ''):<20}  ODDS: {str(p.get('odds') or ''):<10}│")
        lines.append(f"  │  SIGNAL:  {sid:<20}  SCORE: [{sc}/10]     │")
        lines.append(f"  │  BASIS:   {sbasis[:48]:<48}│")
        lines.append(f"  └─────────────────────────────────────────────────────────┘")
        lines.append(f"\\n  {odds_summary_line(g)}")
        lines.append(f"\\n  REASON: {textwrap.fill(str(p.get('reason') or ''), width=66, subsequent_indent='          ')}")
"""
    if "│  BASIS:" not in t:
        if old_top not in t:
            raise SystemExit("TOP pick box not found")
        t = t.replace(old_top, new_top, 1)

    old_rest = """        best = sorted(sigs["picks"], key=lambda x: x["priority"])[0]
        lines.append(f"\\n  #{i}  {matchup_line(g)}")
        lines.append(f"       {weather_line(g)}")
        lines.append(f"       {entry['starter']}")
        lines.append(f"       {entry['streak']}")
        alert = movement_alert(conn, game_date, session,
                               g["game_pk"], g.get("total_line"), g.get("home_ml"))
        if alert:
            lines.append("")
            lines.append(alert)
        lines.append(f"       BET: {best['bet']:<20} ODDS: {best['odds']:<8} SIGNAL: {', '.join(sigs['signals'])}")
        lines.append(f"       {textwrap.fill(best['reason'], width=66, subsequent_indent='       ')}")
"""
    new_rest = """        best = best_pick_from_sigs(sigs) or {}
        bsc = int(best.get("confidence_score") or 0)
        bsid = str(best.get("signal_id") or ", ".join(sigs["signals"]))
        bbasis = str(best.get("score_basis") or "")
        lines.append(f"\\n  #{i}  {matchup_line(g)}")
        lines.append(f"       {weather_line(g)}")
        lines.append(f"       {entry['starter']}")
        lines.append(f"       {entry['streak']}")
        alert = movement_alert(conn, game_date, session,
                               g["game_pk"], g.get("total_line"), g.get("home_ml"))
        if alert:
            lines.append("")
            lines.append(alert)
        lines.append("       ┌─────────────────────────────────────────────────────────┐")
        lines.append(
            f"       │  BET:     {str(best.get('bet') or ''):<20}  ODDS: {str(best.get('odds') or ''):<10}│"
        )
        lines.append(f"       │  SIGNAL:  {bsid:<20}  SCORE: [{bsc}/10]     │")
        lines.append(f"       │  BASIS:   {bbasis[:48]:<48}│")
        lines.append("       └─────────────────────────────────────────────────────────┘")
        lines.append(f"       {textwrap.fill(str(best.get('reason') or ''), width=66, subsequent_indent='       ')}")
"""
    if "best = best_pick_from_sigs" not in t and old_rest in t:
        t = t.replace(old_rest, new_rest, 1)

    watch_blk = """
    # ── Model watch list (confidence 5–6, below full-stake threshold) ───
    watch_model_rows: list[tuple[dict, list]] = []
    for e in all_entries:
        sc = (e.get("sigs") or {}).get("scored")
        if sc is not None and getattr(sc, "watch_list", None):
            wl = list(sc.watch_list)
            if wl:
                watch_model_rows.append((e, wl))
    if watch_model_rows:
        nwl = sum(len(w) for _, w in watch_model_rows)
        lines.append("─" * 72)
        lines.append(f"  👁  WATCH LIST  ({nwl} — scored 5-6, not yet betting threshold)")
        lines.append("─" * 72)
        for wentry, wl in watch_model_rows:
            wg = wentry["game"]
            lines.append(f"  {matchup_line(wg)}")
            for finding in wl:
                bl = finding_bet_label(wg, finding)
                lines.append(
                    f"  {finding.signal_id}  [{finding.confidence_score}/10]  "
                    f"{bl}  {finding.odds}  —  {finding.score_basis[:60]}"
                )
            lines.append("")

"""
    s6a = '    lines.append(section(f"🔬  S6 PITCHER STREAK MONITOR'
    if "watch_model_rows" not in t and s6a in t:
        t = t.replace(s6a, watch_blk + s6a, 1)

    old_ns = """    if no_signal:
        lines.append(section(f"—  NO SIGNAL  ({len(no_signal)} games — market efficient or insufficient data)"))
        for entry in no_signal:
            g = entry["game"]
            lines.append(f"  {matchup_line(g)}  |  {weather_line(g)}")
            lines.append(f"    {odds_summary_line(g)}")
            if entry["sigs"]["data_flags"]:
                for f in entry["sigs"]["data_flags"]:
                    lines.append(f"    ⚠ {f}")
        lines.append("")
"""
    new_ns = """    if no_signal:
        lines.append(section(f"—  NO SIGNAL  ({len(no_signal)} games — market efficient or insufficient data)"))
        for entry in no_signal:
            g = entry["game"]
            lines.append(f"  {matchup_line(g)}  |  {weather_line(g)}")
            lines.append(f"    {odds_summary_line(g)}")
            sc = entry["sigs"].get("scored")
            if sc is not None:
                for f in getattr(sc, "watch_list", []) or []:
                    lines.append(
                        f"    [{f.confidence_score}/10]  {f.signal_id}  {f.bet_side}  "
                        f"— {f.score_basis[:55]}"
                    )
                for f in getattr(sc, "contradicted", []) or []:
                    lines.append(
                        f"    [MONITOR]  {f.signal_id}  — score {f.confidence_score}/10"
                    )
            if entry["sigs"]["data_flags"]:
                for f in entry["sigs"]["data_flags"]:
                    lines.append(f"    ⚠ {f}")
        lines.append("")
"""
    if 'sc = entry["sigs"].get("scored")' not in t and old_ns in t:
        t = t.replace(old_ns, new_ns, 1)

    t = t.replace(
        "        sorted(current_games, key=lambda e: min(p[\"priority\"] for p in e[\"sigs\"][\"picks\"]))\n",
        "        sorted(current_games, key=lambda e: -entry_best_confidence(e))\n",
        1,
    )
    t = t.replace(
        "                    f\"      Signal still active — higher-priority pick displaced it at top.\\n\"\n",
        "                    f\"      Signal still active — higher-confidence pick displaced it at top.\\n\"\n",
        1,
    )

    t = t.replace(
        "        key=lambda e: min(p[\"priority\"] for p in e[\"graded\"])\n",
        "        key=lambda e: -entry_best_graded_confidence(e)\n",
        1,
    )

    t = t.replace(
        '            p = sorted(entry["graded"], key=lambda x: x["priority"])[0]\n',
        '            p = max(entry["graded"], key=lambda x: (-int(x.get("confidence_score") or 0), x.get("priority", 99)))\n',
        1,
    )

    t = t.replace(
        "    pick_entries  = sorted([e for e in evaluated if e[\"graded\"]],\n"
        "                           key=lambda e: min(p[\"priority\"] for p in e[\"graded\"]))\n",
        "    pick_entries  = sorted([e for e in evaluated if e[\"graded\"]],\n"
        "                           key=lambda e: -entry_best_graded_confidence(e))\n",
        1,
    )

    t = t.replace(
        "        picks_entries.sort(key=lambda e: min(p[\"priority\"] for p in e[\"sigs\"][\"picks\"]))\n",
        "        sort_entries_by_pick_confidence(picks_entries)\n",
        1,
    )

    t = t.replace(
        "        pick_entries  = sorted([e for e in evaluated if e[\"graded\"]],\n"
        "                               key=lambda e: min(p[\"priority\"] for p in e[\"graded\"]))\n",
        "        pick_entries  = sorted([e for e in evaluated if e[\"graded\"]],\n"
        "                               key=lambda e: -entry_best_graded_confidence(e))\n",
        1,
    )

    old_doc_pick = """        for pick in sorted(sigs["picks"], key=lambda x: x["priority"]):
            row_cells = tbl.add_row().cells
            pick_bg   = "E8F5E9" if pick["market"] == "TOTAL" else "E3F2FD"
            for cell in row_cells:
                _set_cell_bg(cell, pick_bg)
            _cell_para(row_cells[0], ", ".join(sigs["signals"]),
                       bold=True, size_pt=9, color_hex="1F3864")
            _cell_para(row_cells[1], pick["bet"],
                       bold=True, size_pt=10, color_hex="0D47A1")
            _cell_para(row_cells[2], pick["odds"],
                       bold=True, size_pt=10, color_hex="1B5E20",
                       align=WD_ALIGN_PARAGRAPH.CENTER)
            # Wrap reason text
            reason_short = (pick["reason"][:300] + "…")                            if len(pick["reason"]) > 300 else pick["reason"]
            _cell_para(row_cells[3], reason_short, size_pt=8, color_hex="333333")

        doc.add_paragraph()  # spacer
"""
    new_doc_pick = """        for pick in sorted(
            sigs["picks"],
            key=lambda x: (-int(x.get("confidence_score") or 0), x.get("priority", 99)),
        ):
            row_cells = tbl.add_row().cells
            pick_bg   = "E8F5E9" if pick["market"] == "TOTAL" else "E3F2FD"
            for cell in row_cells:
                _set_cell_bg(cell, pick_bg)
            sig_cell = str(pick.get("signal_id") or ", ".join(sigs["signals"]))
            _cell_para(row_cells[0], sig_cell,
                       bold=True, size_pt=9, color_hex="1F3864")
            _cell_para(row_cells[1], pick["bet"],
                       bold=True, size_pt=10, color_hex="0D47A1")
            _cell_para(row_cells[2], pick["odds"],
                       bold=True, size_pt=10, color_hex="1B5E20",
                       align=WD_ALIGN_PARAGRAPH.CENTER)
            # Wrap reason text
            reason_short = (pick["reason"][:300] + "…")                            if len(pick["reason"]) > 300 else pick["reason"]
            _cell_para(row_cells[3], reason_short, size_pt=8, color_hex="333333")

            sc = int(pick.get("confidence_score") or 0)
            sc_row = tbl.add_row().cells
            for cell in sc_row:
                _set_cell_bg(cell, pick_bg)
            _cell_para(sc_row[0], "SCORE", bold=True, size_pt=9, color_hex="1F3864")
            sc_txt = f"[{sc}/10] — {score_label(sc)}"
            _cell_para(
                sc_row[1],
                sc_txt,
                bold=True,
                size_pt=9,
                color_hex=score_row_color_hex(sc),
            )
            sc_row[2].merge(sc_row[3])
            basis_txt = str(pick.get("score_basis") or "")[:220]
            _cell_para(sc_row[2], basis_txt, size_pt=8, color_hex="333333")

        doc.add_paragraph()  # spacer
"""
    if "score_row_color_hex(sc)" not in t and old_doc_pick in t:
        t = t.replace(old_doc_pick, new_doc_pick, 1)

    doc_watch = """
        # Model watch list (5–6 confidence) — same session games only
        watch_doc_rows: list[tuple[dict, list]] = []
        for e in entries:
            scw = (e.get("sigs") or {}).get("scored")
            if scw is not None and getattr(scw, "watch_list", None):
                wwl = list(scw.watch_list)
                if wwl:
                    watch_doc_rows.append((e, wwl))
        if watch_doc_rows:
            nwd = sum(len(w) for _, w in watch_doc_rows)
            _add_heading(doc, f"Watch list — model 5–6 scores ({nwd})", level=2)
            _add_note(
                doc,
                "Scored below full-stake threshold (7+). Monitor only unless you scale down.",
                italic=True,
                color_hex="475569",
            )
            for wde, wl in watch_doc_rows:
                wg = wde["game"]
                p0 = doc.add_paragraph()
                r0 = p0.add_run(matchup_line(wg))
                r0.bold = True
                for finding in wl:
                    bl = finding_bet_label(wg, finding)
                    p1 = doc.add_paragraph()
                    r1 = p1.add_run(
                        f"  {finding.signal_id}  [{finding.confidence_score}/10]  {bl}  {finding.odds}"
                    )
                    r1.font.size = Pt(9)
                    r1 = p1.add_run(f" — {finding.score_basis[:120]}")
                    r1.font.size = Pt(8)
                    r1.font.color.rgb = RGBColor(0x55, 0x55, 0x55)
                doc.add_paragraph()

"""
    anchor_doc = '        # No signal\n        _add_heading(doc, f"No Signal'
    if "Watch list — model 5–6" not in t and anchor_doc in t:
        t = t.replace(anchor_doc, doc_watch + anchor_doc, 1)

    old_nosig_doc = """        # No signal
        _add_heading(doc, f"No Signal  ({len(nosig_entries)} games)", level=2)
        _add_full_slate_table(doc, [e["game"] for e in nosig_entries])
"""
    new_nosig_doc = """        # No signal
        _add_heading(doc, f"No Signal  ({len(nosig_entries)} games)", level=2)
        _add_full_slate_table(doc, [e["game"] for e in nosig_entries])
        for ne in nosig_entries:
            scn = ne["sigs"].get("scored")
            if not scn:
                continue
            extras = list(getattr(scn, "watch_list", []) or []) + list(
                getattr(scn, "contradicted", []) or []
            )
            if not extras:
                continue
            p0 = doc.add_paragraph()
            r0 = p0.add_run(matchup_line(ne["game"]))
            r0.bold = True
            r0.font.size = Pt(10)
            for f in getattr(scn, "watch_list", []) or []:
                p2 = doc.add_paragraph()
                r2 = p2.add_run(
                    f"  [{f.confidence_score}/10]  {f.signal_id}  {f.bet_side}  "
                    f"— {(f.score_basis or '')[:90]}"
                )
                r2.font.size = Pt(8)
            for f in getattr(scn, "contradicted", []) or []:
                p3 = doc.add_paragraph()
                r3 = p3.add_run(f"  [MONITOR]  {f.signal_id}  — score {f.confidence_score}/10")
                r3.font.size = Pt(8)
"""
    if "for ne in nosig_entries:" not in t and old_nosig_doc in t:
        t = t.replace(old_nosig_doc, new_nosig_doc, 1)

    t = t.replace(
        "            for pick in sorted(graded_picks, key=lambda x: x.get(\"priority\", 99)):\n",
        "            for pick in sorted(\n"
        "                graded_picks,\n"
        "                key=lambda x: (-int(x.get(\"confidence_score\") or 0), x.get(\"priority\", 99)),\n"
        "            ):\n",
        1,
    )

    t = t.replace(
        "            all_sig.sort(key=lambda e: min(p[\"priority\"]\n"
        "                                          for p in e[\"sigs\"][\"picks\"]))\n",
        "            sort_entries_by_pick_confidence(all_sig)\n",
        1,
    )

    tmp = pathlib.Path(os.environ.get("TEMP", ".")) / "_gdb_brief_patched.py"
    tmp.write_text(t, encoding="utf-8")
    shutil.copyfile(tmp, P)
    tmp.unlink(missing_ok=True)
    print("OK:", P)


if __name__ == "__main__":
    main()
