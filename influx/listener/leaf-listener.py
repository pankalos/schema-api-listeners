import os
import time
import argparse
import datetime as dt
from datetime import timezone
from typing import Any, Dict, List, Optional, Set

import requests


# =========================================================
# 1. SMALL HELPERS
# =========================================================

def env_or(cli_val: Optional[str], env_key: str) -> str:
    """
    Return the CLI value if the user gave one.
    Otherwise fallback to an environment variable.

    Example:
        --api_url https://portal.m-unlock.com/api/data

    or:
        export LEAF_API_URL=https://portal.m-unlock.com/api/data
    """
    if cli_val:
        return cli_val

    val = os.getenv(env_key)
    if not val:
        raise SystemExit(f"Missing {env_key} and missing corresponding CLI flag.")
    return val


def to_iso_z(epoch_s: int) -> str:
    """
    Convert epoch seconds -> ISO UTC string with trailing Z.

    Example:
        1778156030 -> "2026-05-07T12:13:50Z"

    We use this because you tested that LEAF accepts exact
    timestamps with seconds, and that it behaves like [from, to).
    """
    return (
        dt.datetime.fromtimestamp(epoch_s, tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


# =========================================================
# 2. CLI ARGUMENTS
# =========================================================

def parse_args() -> argparse.Namespace:
    """
    Define all command-line arguments.

    This listener needs:
    - where the modeler lives
    - how to reach LEAF
    - which organisation / department / entity / metrics to use
    - how often to poll
    """
    p = argparse.ArgumentParser(
        description="LEAF listener that fetches multiple metrics, combines them, and sends them to a modeler."
    )

    # -----------------------------
    # Modeler connection parameters
    # -----------------------------
    p.add_argument(
        "--target_service",
        required=True,
        help="Modeler host or k8s service name, e.g. 127.0.0.1 or modeler-service"
    )
    p.add_argument(
        "--target_endpoint",
        required=True,
        help="Modeler endpoint, e.g. model"
    )
    p.add_argument(
        "--port",
        required=True,
        help="Modeler port, e.g. 8080"
    )

    # -----------------------------
    # LEAF API connection parameters
    # -----------------------------
    p.add_argument(
        "--api_url",
        default=None,
        help="LEAF API URL, or env LEAF_API_URL"
    )
    p.add_argument(
        "--token",
        default=None,
        help="LEAF API token, or env LEAF_API_TOKEN"
    )

    # -----------------------------
    # Required LEAF query fields
    # -----------------------------
    p.add_argument("--organisation", required=True)
    p.add_argument("--department", required=True)

    # -----------------------------
    # Optional LEAF query field
    # -----------------------------
    p.add_argument(
        "--entity",
        default=None,
        help="Optional entity filter. Strongly recommended for this simple listener."
    )

    # -----------------------------
    # One or more metrics
    # Example:
    #   --metrics mem.used disk.used cpu.usage_idle
    # -----------------------------
    p.add_argument(
        "--metrics",
        nargs="+",
        required=True,
        help="One or more metric names"
    )

    # -----------------------------
    # Polling cadence
    # Example:
    #   --everyTs 10
    # means target cadence is every 10 sec.
    # If the loop takes longer than 10 sec, do not sleep.
    # -----------------------------
    p.add_argument(
        "--everyTs",
        type=int,
        required=True,
        help="Polling cadence in seconds"
    )

    # -----------------------------
    # Optional: include time vector
    # If set, payload sent to modeler will also contain:
    #   "ts": [epoch1, epoch2, ...]
    # -----------------------------
    p.add_argument(
        "--include_time_to_mod",
        action="store_true",
        help="If set, include timestamps under fixed key 'ts'"
    )

    return p.parse_args()


# =========================================================
# 3. READ ONE METRIC FROM LEAF
# =========================================================

def fetch_leaf_rows_for_one_metric(
    session: requests.Session,
    api_url: str,
    token: str,
    organisation: str,
    department: str,
    metric: str,
    entity: Optional[str],
    start_s: int,
    stop_s: int,
) -> List[dict]:
    """
    Query LEAF for a SINGLE metric in a SINGLE time window.

    Window semantics:
        [start_s, stop_s)

    So:
        from is included
        to is excluded

    Example request:
        organisation=UNLOCK
        department=FDP
        entity=ichibi.wurnet.nl
        metric=mem.used
        from=2026-05-07T12:13:50Z
        to=2026-05-07T12:14:00Z
    """
    params = {
        "organisation": organisation,
        "department": department,
        "metric": metric,
        "from": to_iso_z(start_s),
        "to": to_iso_z(stop_s),
    }

    # Add entity only if user gave one
    if entity:
        params["entity"] = entity

    response = session.get(
        api_url,
        params=params,
        headers={
            "accept": "application/json",
            "Authorization": f"Bearer {token}",
        },
        # Large timeout because LEAF may respond slowly
        timeout=(10, 300),
    )

    # Crash this cycle if HTTP status is not success
    response.raise_for_status()

    data = response.json()

    if not isinstance(data, list):
        raise RuntimeError(
            f"LEAF API response for metric '{metric}' must be a JSON list."
        )

    return data


# =========================================================
# 4. READ ALL REQUESTED METRICS
# =========================================================

def fetch_all_metrics(
    session: requests.Session,
    api_url: str,
    token: str,
    organisation: str,
    department: str,
    metrics: List[str],
    entity: Optional[str],
    start_s: int,
    stop_s: int,
) -> Dict[str, List[dict]]:
    """
    Query LEAF ONCE PER METRIC using the SAME time window.

    Output example:
        {
          "mem.used": [...rows...],
          "disk.used": [...rows...]
        }

    This is the simplest safe way to support multiple metrics,
    since LEAF did not appear to support multi-metric filtering
    in one request.
    """
    rows_by_metric: Dict[str, List[dict]] = {}

    for metric in metrics:
        print(
            f"[listener] Fetching metric '{metric}' "
            f"from {to_iso_z(start_s)} to {to_iso_z(stop_s)}"
        )

        rows = fetch_leaf_rows_for_one_metric(
            session=session,
            api_url=api_url,
            token=token,
            organisation=organisation,
            department=department,
            metric=metric,
            entity=entity,
            start_s=start_s,
            stop_s=stop_s,
        )

        print(f"[listener] Metric '{metric}' returned {len(rows)} row(s)")
        rows_by_metric[metric] = rows

    return rows_by_metric


# =========================================================
# 5. COMBINE METRICS INTO ONE MODELER PAYLOAD
# =========================================================

def build_combined_payload(
    rows_by_metric: Dict[str, List[dict]],
    entity: Optional[str],
    include_time: bool,
) -> Dict[str, Any]:
    """
    Example input from LEAF:

    rows_by_metric = {
        "mem.used": [
            {"time": "2026-05-07T12:00:10+00:00", "entity": "ichibi.wurnet.nl", "metric": "mem.used", "value": 100},
            {"time": "2026-05-07T12:00:20+00:00", "entity": "ichibi.wurnet.nl", "metric": "mem.used", "value": 101},
            {"time": "2026-05-07T12:00:30+00:00", "entity": "ichibi.wurnet.nl", "metric": "mem.used", "value": 102},
        ],
        "disk.used": [
            {"time": "2026-05-07T12:00:10+00:00", "entity": "ichibi.wurnet.nl", "metric": "disk.used", "value": 900},
            {"time": "2026-05-07T12:00:20+00:00", "entity": "ichibi.wurnet.nl", "metric": "disk.used", "value": 901},
            {"time": "2026-05-07T12:00:30+00:00", "entity": "ichibi.wurnet.nl", "metric": "disk.used", "value": 902},
        ],
    }

    Desired final payload:

    {
        "mem.used": [100, 101, 102],
        "disk.used": [900, 901, 902],
        "ts": [t1, t2, t3]
    }

    Why do we first build timestamp -> value dictionaries?

    Because it makes alignment easy.

    Example temporary form:

    mem_map = {
        t1: 100,
        t2: 101,
        t3: 102,
    }

    disk_map = {
        t1: 900,
        t2: 901,
        t3: 902,
    }

    Then we can safely say:
      common timestamps = [t1, t2, t3]

    And from that we build the final lists.
    """

    # This will temporarily hold one dict per metric:
    #
    # metric_maps["mem.used"]  = {t1: 100, t2: 101, t3: 102}
    # metric_maps["disk.used"] = {t1: 900, t2: 901, t3: 902}
    metric_maps: Dict[str, Dict[int, Any]] = {}

    # --------------------------------------------------
    # Step 1: convert each metric's rows into:
    #         timestamp -> value
    # --------------------------------------------------
    for metric, rows in rows_by_metric.items():
        one_metric_map: Dict[int, Any] = {}

        for row in rows:
            # If entity filter was given, ignore rows from other entities
            if entity is not None and row.get("entity") != entity:
                continue

            # Convert ISO time string to epoch seconds
            row_dt = dt.datetime.fromisoformat(row["time"])
            ts = int(row_dt.timestamp())

            # Store the value under that timestamp
            one_metric_map[ts] = row["value"]

        metric_maps[metric] = one_metric_map

    # --------------------------------------------------
    # Step 2: find timestamps common to ALL metrics
    # --------------------------------------------------
    #
    # Example:
    # mem.used  has {10, 20, 30}
    # disk.used has {20, 30, 40}
    #
    # common_ts becomes {20, 30}
    #
    common_ts: Optional[Set[int]] = None

    for metric, one_metric_map in metric_maps.items():
        metric_timestamps = set(one_metric_map.keys())

        if common_ts is None:
            # First metric initializes the set
            common_ts = metric_timestamps
        else:
            # Keep only timestamps that also exist in this metric
            common_ts = common_ts.intersection(metric_timestamps)

    # If no common timestamps exist, return empty payload
    if common_ts is None or len(common_ts) == 0:
        return {"ts": []} if include_time else {}

    # --------------------------------------------------
    # Step 3: sort timestamps oldest -> newest
    # --------------------------------------------------
    ordered_ts = sorted(common_ts)

    # --------------------------------------------------
    # Step 4: build final dict-of-lists payload
    # --------------------------------------------------
    payload: Dict[str, Any] = {}

    for metric, one_metric_map in metric_maps.items():
        # For each timestamp in ordered_ts, pick the matching value
        payload[metric] = [one_metric_map[ts] for ts in ordered_ts]

    if include_time:
        payload["ts"] = ordered_ts

    return payload


# =========================================================
# 6. MAIN LISTENER LOOP
# =========================================================

def main() -> None:
    # Parse user arguments
    args = parse_args()

    if args.everyTs <= 0:
        raise SystemExit("--everyTs must be a positive integer.")

    # Resolve API URL and token from CLI or env
    api_url = env_or(args.api_url, "LEAF_API_URL")
    token = env_or(args.token, "LEAF_API_TOKEN")

    # Build modeler URL
    # Example:
    #   http://127.0.0.1:8080/model
    endpoint = args.target_endpoint.lstrip("/")
    modeler_url = f"http://{args.target_service}:{args.port}/{endpoint}"

    print(f"modeler_url: {modeler_url}")

    # Reuse one HTTP session for efficiency
    session = requests.Session()

    # -----------------------------------------------------
    # Step 6.1: initial time window = last everyTs seconds
    # -----------------------------------------------------
    # Example:
    #   if now = 12:14:00 and everyTs=10
    #   start = 12:13:50
    #   stop  = 12:14:00
    now_s = int(dt.datetime.now(timezone.utc).timestamp())
    start_s = now_s - args.everyTs
    stop_s = now_s

    # -----------------------------------------------------
    # Step 6.2: cadence timer
    # -----------------------------------------------------
    # This keeps the same idea as your old listener:
    # - if work is fast, wait until next tick
    # - if work is slow, do not wait
    tick = time.monotonic()

    # -----------------------------------------------------
    # Step 6.3: infinite loop
    # -----------------------------------------------------
    while True:
        try:
            # =============================================
            # A. Fetch all requested metrics for same window
            # =============================================
            start_query_time = time.monotonic()
            rows_by_metric = fetch_all_metrics(
                session=session,
                api_url=api_url,
                token=token,
                organisation=args.organisation,
                department=args.department,
                metrics=args.metrics,
                entity=args.entity,
                start_s=start_s,
                stop_s=stop_s,
            )

            elapsed = time.monotonic() - start_query_time
            window_size = stop_s - start_s

            print(f"[listener] fetch_all_metrics took {elapsed:.2f} sec")
            print(f"[listener] queried window size was {window_size} sec")

            # =============================================
            # B. Combine rows into one payload for modeler
            # =============================================
            payload = build_combined_payload(
                rows_by_metric=rows_by_metric,
                entity=args.entity,
                include_time=args.include_time_to_mod,
            )

            # =============================================
            # C. Check whether combined payload has data
            # =============================================
            metric_keys = [m for m in args.metrics if m in payload]
            has_data = bool(metric_keys) and len(payload[metric_keys[0]]) > 0

            if not payload or not has_data:
                print("[listener] No aligned rows found across all requested metrics in this window.")
            else:
                print(f"[listener] Sending payload to modeler: keys={list(payload.keys())}")
                print(f"payload: {payload}")

                # =============================================
                # D. Send payload to modeler
                # =============================================
                response = session.post(
                    modeler_url,
                    json=payload,
                    timeout=(10, 300),
                )
                response.raise_for_status()

                # =============================================
                # E. Print modeler response
                # =============================================
                try:
                    print("[listener] Modeler response JSON:")
                    print(response.json())
                except Exception:
                    print("[listener] Modeler response text:")
                    print(response.text)

        except Exception as e:
            # Keep the listener alive even if one cycle fails
            print(f"[listener] ERROR: {e}")

        # -------------------------------------------------
        # Step 6.4: same cadence logic as old listener
        # -------------------------------------------------
        # We target one cycle every everyTs seconds.
        #
        # If current cycle finished early:
        #   sleep until next tick
        #
        # If current cycle took too long:
        #   do not sleep
        #   start next cycle immediately
        tick += args.everyTs
        sleep_s = tick - time.monotonic()

        if sleep_s > 0:
            print(f"[listener] Sleeping {sleep_s:.2f} sec")
            time.sleep(sleep_s)
        else:
            print("[listener] Loop exceeded cadence; next query starts immediately")
            tick = time.monotonic()

        # -------------------------------------------------
        # Step 6.5: advance rolling time window
        # -------------------------------------------------
        # This is the important old-listener behavior:
        #
        #   next start = previous stop
        #   next stop  = NOW
        #
        # So if LEAF is slow and one cycle takes 2m40s,
        # the next request automatically covers that whole
        # missed period.
        start_s = stop_s
        stop_s = int(dt.datetime.now(timezone.utc).timestamp())


# =========================================================
# 7. ENTRY POINT
# =========================================================

if __name__ == "__main__":
    main()
