"""Twenty fake x_feed rows (as intake-api GET /v1/x-feed/pulls/{pull_id}/rows returns them). TEST DATA only."""

from __future__ import annotations

from typing import Any

CHIEFS = ("chief-data", "chief-of-staff", "chief-opportunities", "chief-health")
TAGS = ("ai,agents", "solopreneur", "thai-tech", "health,sleep", "jobs,remote")


def x_feed_rows(pull_id: str = "test-pull-0001", count: int = 20) -> list[dict[str, Any]]:
    rows = []
    for n in range(count):
        post_id = str(1990000000000000000 + n)
        chief = CHIEFS[n % len(CHIEFS)]
        query_id = f"q{n % 4}"
        rows.append(
            {
                "sighting_key": f"{pull_id}:{post_id}:{query_id}:{chief}",
                "pull_id": pull_id,
                "collected_at": "2026-10-25T23:30:00+00:00",
                "requesting_chief": chief,
                "watch_type": "keyword" if n % 2 else "account",
                "watch_value": f"test_value_{n % 3}",
                "post_id": post_id,
                "post_url": f"https://x.com/test_user_{n}/status/{post_id}",
                "author_handle": f"test_user_{n}",
                "posted_at": "2026-10-25T23:00:00+00:00",
                "text": f"TEST DATA {n}: fake post about agents",
                "likes": 10 * n,
                "reposts": n,
                "replies": n // 2,
                "views": 1000 + n,
                "tag": TAGS[n % len(TAGS)],
                "query_id": query_id,
                "score": round(0.5 + n / 100, 3),
                "top": n % 5 == 0,
            }
        )
    return rows
