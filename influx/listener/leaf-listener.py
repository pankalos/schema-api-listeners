import os
import time
import argparse
import datetime as dt
from datetime import timezone
from typing import Any, Dict, List, Optional

import requests


# ---------------------------------------------------------
# Helper: get value from CLI first, otherwise from env var
# ---------------------------------------------------------
def env_or(cli_val: Optional[str], env_key: str) -> str:
    """
    Use the CLI value if it exists.
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


# ---------------------------------------------------------
# Helper: convert epoch seconds -> ISO 8601 UTC string
# ---------------------------------------------------------
def to_iso_z(epoch_s: int) -> str:
    """
    Convert epoch seconds to a UTC ISO timestamp like:
      2026-04-20T23:59:30Z

    We use this because LEAF accepts exact timestamps and
    we confirmed the API behaves like [from, to).
    """
    return (
        dt.datetime.fromtimestamp(epoch_s, tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


# ---------------------------------------------------------
# Step 1: parse command-line arguments
# ---------------------------------------------------------
def parse_args() -> argparse.Namespace:
    """
    Define all CLI arguments used by the listener.

    We keep this simple:
    - LEAF API read side
    - modeler target
    - polling cadence
    """
    p = argparse.ArgumentParser(description="Simple LEAF listener")

    # -------------------------
    # Modeler service settings
    # -------------------------
    p.add_argument("--target_service", required=True, help="Modeler host or k8s service name")
    p.add_argument("--target_endpoint", required=True, help="Modeler endpoint, e.g. model")
    p.add_argument("--port", required=True, help="Modeler port")

    # -------------------------
    # LEAF API settings
    # -------------------------
    p.add_argument("--api_url", default=None, help="LEAF API URL or env LEAF_API_URL")
    p.add_argument("--token", default=None, help="LEAF API token or env LEAF_API_TOKEN")
    p.add_argument("--organisation", required=True)
    p.add_argument("--department", required=True)
    p.add_argument("--entity", default=None)
    p.add_argument("--metric", required=True)

    # -------------------------
    # Polling interval
    # -------------------------
    p.add_argument("--everyTs", type=int, required=True, help="Poll every N seconds")

    # -------------------------
    # Optional: also send time vector to modeler
    # -------------------------
    p.add_argument(
        "--include_time_to_mod",
        action="store_true",
        help="If set, send timestamps to modeler under key 'ts' as epoch ints",
    )

    return p.parse_args()


# ---------------------------------------------------------
# Step 2: query LEAF API for one rolling time window
# ---------------------------------------------------------
def fetch_leaf_rows(
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
    Query LEAF for the window [start_s, stop_s).

    Example query built by this function:
      ?organisation=UNLOCK
      &department=FDP
      &entity=ichibi.wurnet.nl
      &metric=mem.used
      &from=2026-04-20T23:59:30Z
      &to=2026-04-20T23:59:40Z
    """
    # Build query parameters exactly as LEAF expects them
    params = {
        "organisation": organisation,
        "department": department,
        "metric": metric,
        "from": to_iso_z(start_s),
        "to": to_iso_z(stop_s),
    }

    # Add entity only if the user provided one
    if entity:
        params["entity"] = entity

    # Make the HTTP GET request
    resp = session.get(
        api_url,
        params=params,
        headers={
            "accept": "application/json",
            "Authorization": f"Bearer {token}",
        },
        # Generous timeout because the API can be slow
        timeout=(10, 300),
    )

    # Raise an exception if HTTP status is not 2xx
    resp.raise_for_status()

    # Parse the JSON body
    data = resp.json()

    # We expect a list of rows
    if not isinstance(data, list):
        raise RuntimeError("LEAF API response must be a JSON list.")

    return data


# ---------------------------------------------------------
# Step 3: convert LEAF rows -> modeler payload
# ---------------------------------------------------------
def build_payload(
    rows: List[dict],
    metric: str,
    entity: Optional[str],
    include_time: bool,
) -> Dict[str, Any]:
    """
    Convert LEAF rows into the modeler payload.

    Input rows look like:
      {
        "time": "2026-04-20T23:59:40+00:00",
        "entity": "ichibi.wurnet.nl",
        "metric": "mem.used",
        "value": 21967122432.0,
        ...
      }

    Output payload becomes something like:
      {
        "mem.used": [22445219840.0, 21967122432.0],
        "ts": [1776739170, 1776739180]
      }

    Notes:
    - We sort oldest -> newest before sending to modeler
    - We keep only the requested metric
    - If entity is given, we keep only that entity
    """
    filtered: List[dict] = []

    # Keep only rows that match the requested metric and entity
    for row in rows:
        if row.get("metric") != metric:
            continue
        if entity is not None and row.get("entity") != entity:
            continue
        filtered.append(row)

    # If user did not provide entity, this simple version expects
    # only one entity to appear in the filtered result
    if entity is None:
        entities = {row.get("entity") for row in filtered}
        if len(entities) > 1:
            raise RuntimeError(
                f"Multiple entities returned: {entities}. "
                f"For this simple listener, pass --entity."
            )

    # If nothing matched, return an empty payload
    if not filtered:
        return {"ts": []} if include_time else {}

    # Sort oldest -> newest so the modeler receives time-ordered input
    filtered.sort(key=lambda r: r["time"])

    # Build lists for:
    # - the metric values
    # - the timestamps (optional)
    values: List[Any] = []
    ts_list: List[int] = []

    for row in filtered:
        # Each row has exactly one value
        values.append(row["value"])

        # Convert LEAF time string to epoch seconds
        row_dt = dt.datetime.fromisoformat(row["time"])
        ts_list.append(int(row_dt.timestamp()))

    # Main payload: key is the metric name
    payload: Dict[str, Any] = {
        metric: values
    }

    # Optionally include timestamps under fixed key "ts"
    if include_time:
        payload["ts"] = ts_list

    return payload


# ---------------------------------------------------------
# Step 4: main listener loop
# ---------------------------------------------------------
def main() -> None:
    # Parse CLI args
    args = parse_args()

    # Basic validation
    if args.everyTs <= 0:
        raise SystemExit("--everyTs must be a positive integer.")

    # Resolve API URL and token from CLI or env vars
    api_url = env_or(args.api_url, "LEAF_API_URL")
    token = env_or(args.token, "LEAF_API_TOKEN")

    # Build the final modeler URL
    # Example:
    #   http://modeler-service:8080/model
    endpoint = args.target_endpoint.lstrip("/")
    modeler_url = f"http://{args.target_service}:{args.port}/{endpoint}"

    print(f'modeler_url: {modeler_url}')

    # Reuse one HTTP session for better efficiency
    session = requests.Session()

    # -------------------------------------------------
    # Initial rolling window: last everyTs seconds
    # -------------------------------------------------
    # Example if everyTs=10:
    #   start = now - 10
    #   stop  = now
    now_s = int(dt.datetime.now(timezone.utc).timestamp())
    start_s = now_s - args.everyTs
    stop_s = now_s

    # -------------------------------------------------
    # Cadence timer, same idea as old listener
    # -------------------------------------------------
    # This is the important behavior we want:
    # - if work finishes early, wait until next tick
    # - if work is slow, do not wait; start immediately
    tick = time.monotonic()

    # -------------------------------------------------
    # Infinite polling loop
    # -------------------------------------------------
    while True:
        try:
            # -------------------------
            # Step 4.1: fetch from LEAF
            # -------------------------
            print(f"[listener] Querying LEAF from {to_iso_z(start_s)} to {to_iso_z(stop_s)}")

            rows = fetch_leaf_rows(
                session=session,
                api_url=api_url,
                token=token,
                organisation=args.organisation,
                department=args.department,
                metric=args.metric,
                entity=args.entity,
                start_s=start_s,
                stop_s=stop_s,
            )

            print(f"[listener] Got {len(rows)} row(s) from LEAF")
            # print(f'rows: {rows}')
            print(f'time: {time.monotonic() - tick}')

            # -------------------------
            # Step 4.2: build payload
            # -------------------------
            payload = build_payload(
                rows=rows,
                metric=args.metric,
                entity=args.entity,
                include_time=args.include_time_to_mod,
            )

            # -------------------------
            # Step 4.3: if no data, skip modeler call
            # -------------------------
            if not payload or (args.metric in payload and len(payload[args.metric]) == 0):
                print("[listener] No matching rows in this window.")

            else:
                print(f"[listener] Sending payload to modeler: keys={list(payload.keys())}")
                print(f'payload: {payload}')

                # -------------------------
                # Step 4.4: call modeler
                # -------------------------
                resp = session.post(
                    modeler_url,
                    json=payload,
                    timeout=(10, 300),
                )
                resp.raise_for_status()

                # -------------------------
                # Step 4.5: print modeler response
                # -------------------------
                try:
                    print("[listener] Modeler response JSON:")
                    print(resp.json())
                except Exception:
                    print("[listener] Modeler response text:")
                    print(resp.text)

        except Exception as e:
            # For now, keep it simple: log the error and continue
            print(f"[listener] ERROR: {e}")

        # -------------------------------------------------
        # Step 5: old-listener cadence logic
        # -------------------------------------------------
        # We increase the target tick by everyTs.
        # Then:
        # - if there is time left, sleep
        # - if not, start immediately
        tick += args.everyTs
        sleep_s = tick - time.monotonic()

        if sleep_s > 0:
            print(f"[listener] Sleeping {sleep_s:.2f} sec")
            time.sleep(sleep_s)
        else:
            print("[listener] Loop exceeded cadence; next query starts immediately")
            tick = time.monotonic()

        # -------------------------------------------------
        # Step 6: advance the rolling window
        # -------------------------------------------------
        # This is the key behavior:
        #   next start = previous stop
        #   next stop  = NOW
        #
        # So if the LEAF API takes 2m40s, the next request
        # automatically covers that full missed period.
        start_s = stop_s
        stop_s = int(dt.datetime.now(timezone.utc).timestamp())


# ---------------------------------------------------------
# Script entry point
# ---------------------------------------------------------
if __name__ == "__main__":
    main()
