import os
import time
import argparse
import datetime as dt
from datetime import timezone
from typing import Any, Dict, List, Optional, Set

import requests
import paho.mqtt.client as mqtt


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


def env_or_none(cli_val: Optional[str], env_key: str) -> Optional[str]:
    """
    Return CLI value if provided, otherwise environment variable if present.
    If neither exists, return None.
    """
    if cli_val:
        return cli_val
    return os.getenv(env_key)


def to_iso_z(epoch_s: int) -> str:
    """
    Convert epoch seconds -> ISO UTC string with trailing Z.

    Example:
        1778156030 -> "2026-05-07T12:13:50Z"

    We use this because we tested that LEAF accepts exact
    timestamps with seconds, and that it behaves like [from, to).
    """
    return (
        dt.datetime.fromtimestamp(epoch_s, tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def escape_measurement_or_tag(s: str) -> str:
    """
    Escape measurement names and tag keys/values for Influx line protocol.
    """
    return str(s).replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")


def escape_field_key(s: str) -> str:
    """
    Escape field keys for Influx line protocol.
    """
    return str(s).replace("\\", "\\\\").replace(" ", "\\ ").replace(",", "\\,").replace("=", "\\=")


def encode_field_value(value: Any) -> str:
    """
    Encode a Python value as an Influx line protocol field value.
    """
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int) and not isinstance(value, bool):
        return f"{value}i"
    if isinstance(value, float):
        return str(value)

    escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def parse_output_tags(tag_pairs: List[str]) -> Dict[str, str]:
    """
    Parse:
      ["workflow=test", "producer=leaf-listener"]
    into:
      {"workflow": "test", "producer": "leaf-listener"}
    """
    tags: Dict[str, str] = {}

    for pair in tag_pairs:
        if "=" not in pair:
            raise SystemExit(f"Bad output tag '{pair}'. Expected key=value.")
        k, v = pair.split("=", 1)
        if not k:
            raise SystemExit(f"Bad output tag '{pair}'. Empty key.")
        tags[k] = v

    return tags


# =========================================================
# 2. CLI ARGUMENTS
# =========================================================

def parse_args() -> argparse.Namespace:
    """
    Define all command-line arguments.

    This listener needs:
    - where the modeler lives
    - how to reach LEAF
    - which organization / department / entity / metrics to use
    - how often to poll
    - MQTT write-back settings
    """
    p = argparse.ArgumentParser(
        description="LEAF listener that fetches multiple metrics, combines them, sends them to a modeler, and optionally writes results back to MQTT."
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
        required=True,
        help="Required entity filter, e.g. ssb.bioind4"
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
    # MQTT write-back parameters
    # If mqtt_topic is given, the listener will publish one
    # MQTT message per result point.
    # -----------------------------
    p.add_argument("--mqtt_host", default=None, help="MQTT host")
    p.add_argument("--mqtt_port", type=int, default=443, help="MQTT port")
    p.add_argument("--mqtt_username", default=None, help="MQTT username")
    p.add_argument("--mqtt_password", default=None, help="MQTT password")
    p.add_argument("--mqtt_topic", default=None, help="MQTT topic, e.g. athenarc/test")
    p.add_argument("--mqtt_basepath", default="mqtt", help="MQTT websocket basepath, e.g. mqtt")
    p.add_argument("--mqtt_measurement", default=None, help="Influx line protocol measurement")
    p.add_argument(
        "--output_tags",
        nargs="*",
        default=[],
        help='Optional extra output tags, e.g. --output_tags workflow=test producer=leaf-listener'
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

    The timestamp column is ALWAYS included now.
    The modeler will receive "ts" and must also return "ts" back.
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
        return {"ts": []}

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

    # Timestamp column is always present
    payload["ts"] = ordered_ts

    return payload


# =========================================================
# 5b. MQTT SINGLE-POINT WRITE-BACK HELPERS
# =========================================================

def build_line_protocol_rows(
    modeler_output: Dict[str, Any],
    measurement: str,
    output_tags: List[str],
) -> List[str]:
    """
    Convert the modeler output dict-of-lists into ONE line-protocol row PER POINT.

    Example modeler output:
      {
        "mem.used": [521, 522],
        "disk.used": [605, 606],
        "ts": [1778753890, 1778753900]
      }

    With:
      measurement = "model_predictions"
      output_tags = ["workflow=test", "producer=leaf-listener", "model=dummy-v1"]

    Output:
      [
        'model_predictions,workflow=test,producer=leaf-listener,model=dummy-v1 mem.used=521i,disk.used=605i 1778753890000000000',
        'model_predictions,workflow=test,producer=leaf-listener,model=dummy-v1 mem.used=522i,disk.used=606i 1778753900000000000'
      ]

    Important:
    - organisation / department / entity are NOT written as line-protocol tags.
    - They are used only for selecting LEAF source data and choosing the MQTT topic.
    """
    if not measurement:
        raise RuntimeError("MQTT measurement is required.")

    if "ts" not in modeler_output:
        raise RuntimeError("Modeler output must contain 'ts'.")

    if not isinstance(modeler_output["ts"], list):
        raise RuntimeError("Modeler output key 'ts' must be a list.")

    ts_values = modeler_output["ts"]
    n = len(ts_values)

    # All non-ts columns become fields.
    field_keys = [k for k in modeler_output.keys() if k != "ts"]

    if not field_keys:
        raise RuntimeError("Modeler output must contain at least one non-'ts' field.")

    for k in field_keys:
        if not isinstance(modeler_output[k], list):
            raise RuntimeError(f"Modeler output key '{k}' must be a list.")
        if len(modeler_output[k]) != n:
            raise RuntimeError(
                f"Length mismatch for '{k}'. Expected {n}, got {len(modeler_output[k])}."
            )

    # Only user-provided output_tags are written as tags.
    # No organisation / department / entity tags here.
    tags = parse_output_tags(output_tags)

    tag_part = ",".join(
        f"{escape_measurement_or_tag(k)}={escape_measurement_or_tag(v)}"
        for k, v in tags.items()
    )

    measurement_part = escape_measurement_or_tag(measurement)

    if tag_part:
        measurement_and_tags = f"{measurement_part},{tag_part}"
    else:
        measurement_and_tags = measurement_part

    rows: List[str] = []

    for i in range(n):
        field_part = ",".join(
            f"{escape_field_key(k)}={encode_field_value(modeler_output[k][i])}"
            for k in field_keys
        )

        # Modeler returns ts in UTC epoch seconds.
        # Influx line protocol expects nanoseconds here.
        ts_ns = int(ts_values[i]) * 1_000_000_000

        line = f"{measurement_and_tags} {field_part} {ts_ns}"
        rows.append(line)

    return rows


def publish_rows_to_mqtt(
    mqtt_host: str,
    mqtt_port: int,
    mqtt_username: str,
    mqtt_password: str,
    mqtt_topic: str,
    mqtt_basepath: str,
    rows: List[str],
) -> None:
    """
    Publish ONE MQTT MESSAGE PER LINE-PROTOCOL ROW.

    So:
      1 row  -> 1 publish
      10 rows -> 10 publishes
    """
    client = mqtt.Client(transport="websockets")
    client.username_pw_set(mqtt_username, mqtt_password)
    client.tls_set()
    client.ws_set_options(path=f"/{mqtt_basepath.lstrip('/')}")

    client.connect(mqtt_host, mqtt_port, keepalive=60)
    client.loop_start()

    try:
        for row in rows:
            print(f"[listener] MQTT publish to '{mqtt_topic}': {row}")
            info = client.publish(mqtt_topic, row, qos=0, retain=False)
            info.wait_for_publish()
    finally:
        client.loop_stop()
        client.disconnect()


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

    mqtt_host = env_or_none(args.mqtt_host, "MQTT_HOST")
    mqtt_username = env_or_none(args.mqtt_username, "MQTT_USERNAME")
    mqtt_password = env_or_none(args.mqtt_password, "MQTT_PASSWORD")
    mqtt_topic = env_or_none(args.mqtt_topic, "MQTT_TOPIC")


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
                # E. Read modeler response JSON
                # =============================================
                modeler_output = response.json()
                print("[listener] Modeler response JSON:")
                print(modeler_output)

                # =============================================
                # F. Optional MQTT single-point write-back
                # =============================================
                if mqtt_topic:
                    if not mqtt_host:
                        raise RuntimeError("--mqtt_host or env MQTT_HOST is required when MQTT write-back is enabled.")
                    if not mqtt_username:
                        raise RuntimeError("--mqtt_username or env MQTT_USERNAME is required when MQTT write-back is enabled.")
                    if mqtt_password is None:
                        raise RuntimeError("--mqtt_password or env MQTT_PASSWORD is required when MQTT write-back is enabled.")
                    if not args.mqtt_measurement:
                        raise RuntimeError("--mqtt_measurement is required when MQTT write-back is enabled.")

                    rows = build_line_protocol_rows(
                        modeler_output=modeler_output,
                        measurement=args.mqtt_measurement,
                        output_tags=args.output_tags,
                    )

                    print(f"[listener] Built {len(rows)} line-protocol row(s) for MQTT")

                    publish_rows_to_mqtt(
                        mqtt_host=mqtt_host,
                        mqtt_port=args.mqtt_port,
                        mqtt_username=mqtt_username,
                        mqtt_password=mqtt_password,
                        mqtt_topic=mqtt_topic,
                        mqtt_basepath=args.mqtt_basepath,
                        rows=rows,
                    )

        except Exception as e:
            # Keep the listener alive even if one cycle fails
            print(f"[listener] ERROR: {e}")
            raise

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
