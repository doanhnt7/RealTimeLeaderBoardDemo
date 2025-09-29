import json
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Deque, Dict, List, Tuple


def parse_iso_to_epoch_ms(ts: str) -> int:
    # Handle ISO8601 with timezone
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return int(dt.timestamp() * 1000)


def score_str(uid: str, score: float, last_update_ms: int, previous_score_val) -> str:
    prev = ("%.2f" % previous_score_val) if previous_score_val is not None else "null"
    return "Score{id='%s', score=%.2f, lastUpdate=%d, previousScore=%s}" % (
        uid,
        score,
        last_update_ms,
        prev,
    )


def change_event_str(change_type: str, score_repr: str) -> str:
    return "ScoreChangeEvent{type=%s, score=%s}" % (change_type, score_repr)


def load_events(path: str) -> List[Tuple[int, str, int]]:
    # returns list of (timestamp_ms, uid, level)
    events: List[Tuple[int, str, int]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            uid = obj["uid"]
            level = int(obj["level"])
            ts_ms = parse_iso_to_epoch_ms(obj.get("updatedAt") or obj.get("updated_at"))
            events.append((ts_ms, uid, level))
    # sort by timestamp then stable by input order
    events.sort(key=lambda x: x[0])
    return events


def group_events_by_timestamp(events: List[Tuple[int, str, int]]) -> Dict[int, List[Tuple[str, int]]]:
    grouped: Dict[int, List[Tuple[str, int]]] = defaultdict(list)
    for ts, uid, level in events:
        grouped[ts].append((uid, level))
    return dict(sorted(grouped.items(), key=lambda kv: kv[0]))


def generate_score_change(input_path: str, output_path: str, window_ms: int = 60_000, top_n: int = 10) -> None:
    events = load_events(input_path)
    grouped = group_events_by_timestamp(events)

    # per-user tracking
    last_level: Dict[str, int] = defaultdict(int)
    # deque of (ts_ms, delta) within window
    user_window: Dict[str, Deque[Tuple[int, float]]] = defaultdict(deque)
    rolling_sum: Dict[str, float] = defaultdict(float)
    previous_total_emitted: Dict[str, float] = defaultdict(float)

    # global top-N state across time
    # maintain current set for comparison and last known score when in top-N
    current_top_set: set = set()
    last_top_score: Dict[str, float] = {}

    out_lines: List[str] = []

    for ts, items in grouped.items():
        # aggregate per user at same timestamp: sum of positive deltas
        delta_by_user: Dict[str, float] = defaultdict(float)
        for uid, level in items:
            prev_level = last_level[uid]
            delta = max(level - prev_level, 0)
            if delta > 0:
                delta_by_user[uid] += float(delta)
            last_level[uid] = level

        # expire window and compute rolling sums before adding new deltas
        window_start = ts - window_ms
        for uid in set(list(delta_by_user.keys()) + list(user_window.keys())):
            dq = user_window[uid]
            # expire outdated
            while dq and dq[0][0] <= window_start:
                old_ts, old_delta = dq.popleft()
                rolling_sum[uid] -= old_delta
                if rolling_sum[uid] < 1e-9:
                    rolling_sum[uid] = 0.0

        # For each affected user, compute new rolling sum and previous score value
        for uid, dsum in delta_by_user.items():
            prev_total = previous_total_emitted.get(uid, 0.0)
            # add current timestamp delta (as one bucket)
            if dsum > 0:
                user_window[uid].append((ts, dsum))
                rolling_sum[uid] += dsum

            new_total = rolling_sum[uid]

            # Update previous_total_emitted AFTER computing new_total to match Java state behavior
            # but previousTotal printed should be the last emitted total (not pre-expire value)
            score_repr = score_str(uid, new_total, ts, prev_total if prev_total != 0.0 else None)

            # We will use these per-user updated totals for global Top-N diff below
            previous_total_emitted[uid] = new_total

        # Build global scores snapshot at this timestamp (only for users with non-zero totals)
        # Note: include all users with rolling_sum > 0 for ranking
        nonzero_users = [(uid, total) for uid, total in rolling_sum.items() if total > 0.0]
        nonzero_users.sort(key=lambda x: (-x[1], x[0]))
        new_top_users = set([uid for uid, _ in nonzero_users[:top_n]])

        # Determine deletes (users leaving top-N)
        to_delete = current_top_set - new_top_users
        # Determine inserts (users entering top-N)
        to_insert = new_top_users - current_top_set

        # Emit deletes first
        for uid in sorted(to_delete):
            # Use last known top score if available; otherwise current rolling sum
            score_val = last_top_score.get(uid, rolling_sum.get(uid, 0.0))
            score_repr = score_str(uid, score_val, ts, None)
            out_lines.append(change_event_str("DELETE", score_repr))

        # Emit inserts
        for uid in sorted(to_insert):
            score_val = rolling_sum.get(uid, 0.0)
            prev_total = None  # not strictly needed for inserts; keep as null for clarity
            score_repr = score_str(uid, score_val, ts, prev_total)
            out_lines.append(change_event_str("INSERT", score_repr))

        # Update top state trackers
        current_top_set = new_top_users
        for uid in current_top_set:
            last_top_score[uid] = rolling_sum.get(uid, 0.0)

    # write output
    with open(output_path, "w", encoding="utf-8") as f:
        for line in out_lines:
            f.write(line + "\n")


if __name__ == "__main__":
    # Inputs/outputs inside repo for easy comparison
    input_file = "app-python/fixed-dataset.jsonl"
    output_file = "app-python/score-change-python.txt"
    generate_score_change(input_file, output_file, window_ms=60_000, top_n=10)


