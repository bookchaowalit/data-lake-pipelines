#!/usr/bin/env python3
"""
Solo Empire Alerts — System health checks and warnings
Usage:
    python3 alerts.py            # Show all alerts
    python3 alerts.py --quiet    # Only show critical alerts
    python3 alerts.py --json     # JSON output for machine consumption
    python3 alerts.py --telegram # Send alerts to Telegram
"""

import json
# Add utils directory to path for imports
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
from task_store import ClientConfigError, open_task_store
from cell_store import CellStoreError, open_cell_store
import crm_ledger
from habits_store import HabitsStoreError, open_habits_store, scheduled_on
import sys
from pathlib import Path
from datetime import date, datetime, timedelta

# Load .env before anything else
from _env import get_env
from secret_resolver import SecretResolutionError, classify_secret_error


class AlertDeliveryError(RuntimeError):
    """A safe error code for a failed notification attempt."""

def check_alerts(quiet=False, json_mode=False, telegram_mode=False):
    """Run all alert checks and report issues."""
    today = date.today()
    today_str = today.isoformat()
    critical = []
    warnings = []
    info = []

    # === CRITICAL ALERTS ===

    # 1. Revenue past due (not received and past revenue_date), from the CRM ledger
    ledger = None
    try:
        ledger = crm_ledger.open_crm_ledger()
        overdue_rows, _ = ledger.revenue_all(until=(today - timedelta(days=1)).isoformat(), received=False)
        pending_total = sum(float(r["amount"] or 0) for r in overdue_rows
                            if (r.get("currency") or "THB").upper() == "THB")
        if pending_total > 10000:
            critical.append(f"💰 ฿{pending_total:,.0f} revenue past due ({len(overdue_rows)} entries)")
    except (crm_ledger.LedgerError, ClientConfigError):
        ledger = None
        warnings.append("⚠️ CRM ledger unavailable — revenue checks skipped")

    # === WARNINGS ===

    # 5. Habits not started
    try:
        habits_api = open_habits_store("reporting-reader")
        habits_done = len({log["habit_id"] for log in habits_api.habit_logs(date_from=today_str, date_to=today_str)})
        habits_total = len(habits_api.habits(status="active"))
        if habits_done == 0 and habits_total > 0:
            warnings.append(f"⚡ 0/{habits_total} habits done — start tracking")
    except (HabitsStoreError, ClientConfigError):
        pass  # habits API unavailable: skip this check

    # 7. Goals stuck at 0%
    try:
        active_goal_rows = open_cell_store("goals", "reporting-reader").list("goals", status="active")
    except (CellStoreError, ClientConfigError):
        active_goal_rows = []
        warnings.append("⚠️ goals API unavailable — goal checks skipped")
    stuck_goals = sum(1 for g in active_goal_rows if g["progress_percent"] == 0)
    if stuck_goals > 0:
        warnings.append(f"🎯 {stuck_goals} goal(s) at 0% — no progress")

    # 7b. Revenue skills decaying (not practiced in 14+ days)
    decay_days = 14
    cutoff = (today - timedelta(days=decay_days)).isoformat()
    try:
        revenue_skills = open_cell_store("roles-skills", "reporting-reader").list("skills", revenue_relevant=True)
    except (CellStoreError, ClientConfigError):
        revenue_skills = []
        warnings.append("⚠️ roles-skills API unavailable — skill decay check skipped")
    decaying = sorted(
        (sk for sk in revenue_skills if not sk.get("last_practiced") or str(sk["last_practiced"])[:10] < cutoff),
        key=lambda sk: (sk.get("last_practiced") is not None, str(sk.get("last_practiced") or "")),
    )
    if decaying:
        names = ", ".join(d["name"] for d in decaying[:3])
        extra = f" +{len(decaying)-3} more" if len(decaying) > 3 else ""
        warnings.append(f"📚 {len(decaying)} revenue skill(s) decaying: {names}{extra}")

    # 7c. MRR drop detection (compare last 3 months avg vs prior 3 months)
    try:
        if ledger is None:
            raise crm_ledger.LedgerError(503, "unavailable")
        received_rows, _ = ledger.revenue_all(until=today_str, received=True)
        by_month: dict[str, float] = {}
        for r in received_rows:
            if (r.get("currency") or "THB").upper() == "THB":
                by_month[r["revenue_date"][:7]] = by_month.get(r["revenue_date"][:7], 0) + float(r["amount"] or 0)
        monthly_totals = [by_month[m] for m in sorted(by_month, reverse=True)[:6]]
        if len(monthly_totals) >= 4:
            recent = sum(monthly_totals[:3]) / 3
            prior = sum(monthly_totals[3:]) / len(monthly_totals[3:])
            if prior > 0 and recent < prior * 0.8:
                pct = round((1 - recent / prior) * 100)
                critical.append(f"📉 MRR dropped {pct}% — recent avg ฿{recent:,.0f}/mo vs prior ฿{prior:,.0f}/mo")
            elif prior > 0 and recent < prior:
                pct = round((1 - recent / prior) * 100)
                warnings.append(f"📉 MRR down {pct}% — recent avg ฿{recent:,.0f}/mo vs prior ฿{prior:,.0f}/mo")
    except Exception:
        pass

    # 7d. Goal at-risk escalation (due within 30 days and <50% progress)
    try:
        at_risk_date = (today + timedelta(days=30)).isoformat()
        at_risk_goals = [
            g for g in active_goal_rows
            if g.get("due_date") and str(g["due_date"])[:10] <= at_risk_date and g["progress_percent"] is not None and g["progress_percent"] < 50
        ]
        if at_risk_goals:
            names = ", ".join(g['title'][:25] for g in at_risk_goals[:3])
            extra = f" +{len(at_risk_goals)-3} more" if len(at_risk_goals) > 3 else ""
            critical.append(f"🎯 {len(at_risk_goals)} goal(s) at-risk (due <30d, <50% done): {names}{extra}")
    except Exception:
        pass

    # 7e. Habit consistency below 20% (30-day window)
    try:
        habits = open_habits_store("reporting-reader").habits(status="active")
        if habits:
            start = (today - timedelta(days=29)).isoformat()
            done_days = {(log["habit_id"], log["log_date"]) for log in
                         open_habits_store("reporting-reader").habit_logs(
                             date_from=start, date_to=today.isoformat(), completed=True)}
            total_possible = 0
            total_done = 0
            for h in habits:
                for d in range(30):
                    check = today - timedelta(days=d)
                    schedule = h.get("schedule_days") or []
                    if schedule and scheduled_on(h, ("mon", "tue", "wed", "thu", "fri", "sat", "sun")[check.weekday()]):
                        total_possible += 1
                        if (h["id"], check.isoformat()) in done_days:
                            total_done += 1
            if total_possible > 0:
                consistency = total_done / total_possible * 100
                if consistency < 20:
                    warnings.append(f"⚡ Habit consistency {consistency:.0f}% (30d) — below 20% threshold")
    except Exception:
        pass

    # === INFO ===

    # 9. Cron job health — check DB for failed jobs
    try:
        failed_jobs = open_cell_store("automation", "reporting-reader").list("cron-jobs", last_status="failed")
        for fj in failed_jobs:
            warnings.append(f"⚙️ Cron job '{fj['name']}' failed at {fj['last_run_at'] or 'unknown'}")
    except (CellStoreError, ClientConfigError):
        pass  # the automation cell is not configured or unreachable

    # 10b. Open tasks created by the Gmail pipeline
    try:
        gmail_groups = open_task_store(None, "reporting-reader").stats(
            group_by="status", external_source="gmail", include_archived=True
        )["groups"]
        gmail_tasks = sum(
            count for status, count in gmail_groups.items()
            if status is not None and status not in ("done", "cancelled", "archived")
        )
        if gmail_tasks > 0:
            info.append(f"📥 {gmail_tasks} open task(s) from Gmail pipeline")
    except Exception:
        pass

    # 10c. Secret provider readiness (status only — never secret values)
    try:
        from secret_resolver import probe_secrets, STATUS_OK
        report = probe_secrets()
        status = report.get("status")
        provider = report.get("provider")
        if status != STATUS_OK:
            if status in ("authentication_failed", "credentials_not_configured"):
                critical.append(
                    f"🔐 Secrets provider not ready: provider={provider} status={status}"
                )
            else:
                warnings.append(
                    f"🔐 Secrets provider: provider={provider} status={status}"
                )
    except Exception:
        warnings.append("🔐 Secrets health probe failed to run")

    # JSON output mode
    if json_mode:
        result = {
            'timestamp': datetime.now().isoformat(),
            'critical': [a for a in critical],
            'warnings': [a for a in warnings],
            'info': [a for a in info],
            'summary': {
                'critical': len(critical),
                'warnings': len(warnings),
                'info': len(info),
                'healthy': len(critical) == 0 and len(warnings) == 0,
            }
        }
        print(json.dumps(result, indent=2))
        return len(critical)

    # Telegram notification mode
    if telegram_mode:
        _send_telegram(critical, warnings, info, today)
        return len(critical)

    # Print results
    print(f"\n🚨 SYSTEM ALERTS — {today.strftime('%A, %B %d')}")
    print(f"{'═' * 60}")

    if critical:
        print(f"\n  🔴 CRITICAL ({len(critical)})")
        print(f"  {'─' * 50}")
        for a in critical:
            print(f"  ⛔ {a}")

    if warnings and not quiet:
        print(f"\n  🟡 WARNINGS ({len(warnings)})")
        print(f"  {'─' * 50}")
        for a in warnings:
            print(f"  ⚠️ {a}")

    if info and not quiet:
        print(f"\n  🔵 INFO ({len(info)})")
        print(f"  {'─' * 50}")
        for a in info:
            print(f"  ℹ️ {a}")

    if not critical and not warnings:
        print("\n  ✅ All systems healthy!")

    # Summary
    print(f"\n  Summary: {len(critical)} critical, {len(warnings)} warnings, {len(info)} info")
    print()

    return len(critical)

def send_telegram_message(message, *, parse_mode=None):
    """Send one bounded message through the configured Telegram bot.

    Only safe error codes leave this boundary. Credentials and provider
    response bodies never appear in logs or exception text.
    """
    import urllib.request
    import urllib.error

    try:
        token = get_env("TELEGRAM_NOTI_BOT_TOKEN") or get_env("TELEGRAM_BOT_TOKEN")
        chat_id = get_env("TELEGRAM_NOTI_CHAT_ID") or get_env("TELEGRAM_CHAT_ID")
    except SecretResolutionError as exc:
        raise AlertDeliveryError("secret_" + classify_secret_error(exc)) from None
    if not token or not chat_id:
        raise AlertDeliveryError("telegram_credentials_missing")

    payload = {
        "chat_id": chat_id,
        "text": message,
        "disable_web_page_preview": True,
    }
    if parse_mode:
        payload["parse_mode"] = parse_mode
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()
    except urllib.error.HTTPError as exc:
        # Telegram returns 404 for an invalid bot token on the Bot API path;
        # treat it as an authentication failure so rotation is actionable.
        if exc.code in (401, 403, 404):
            raise AlertDeliveryError("telegram_authentication_failed") from None
        if 400 <= exc.code < 500:
            raise AlertDeliveryError("telegram_request_rejected") from None
        raise AlertDeliveryError("telegram_delivery_failed") from None
    except Exception:
        raise AlertDeliveryError("telegram_delivery_failed") from None


def _send_telegram(critical, warnings, info, today):
    """Format alerts as HTML and send to Telegram."""
    lines = [f"<b>🚨 System Alerts — {today.strftime('%A, %B %d')}</b>"]
    if critical:
        lines.append(f"\n<b>🔴 CRITICAL ({len(critical)})</b>")
        for a in critical:
            lines.append(f"⛔ {a}")
    if warnings:
        lines.append(f"\n<b>🟡 WARNINGS ({len(warnings)})</b>")
        for a in warnings:
            lines.append(f"⚠️ {a}")
    if info:
        lines.append(f"\n<b>🔵 INFO ({len(info)})</b>")
        for a in info:
            lines.append(f"ℹ️ {a}")
    if not critical and not warnings:
        lines.append("\n✅ <b>All systems healthy!</b>")
    lines.append(f"\n<i>{len(critical)} critical, {len(warnings)} warnings, {len(info)} info</i>")

    send_telegram_message("\n".join(lines), parse_mode="HTML")
    print("✓ Telegram alert sent")


def main():
    args = sys.argv[1:]
    quiet = "--quiet" in args
    json_mode = "--json" in args
    telegram_mode = "--telegram" in args
    try:
        check_alerts(quiet=quiet, json_mode=json_mode, telegram_mode=telegram_mode)
    except AlertDeliveryError as exc:
        # Keep the human-facing console path useful when the optional delivery
        # provider is unavailable. The structured stderr record remains the
        # machine-facing failure signal for cron and monitoring.
        print(
            f"Telegram alert unavailable ({exc}); alerts were generated locally.",
            file=sys.stdout,
        )
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 1
    # Exit 0 on successful run — exit code means "did the script work",
    # not "are there alerts" (schedulers interpret non-zero as failure)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
