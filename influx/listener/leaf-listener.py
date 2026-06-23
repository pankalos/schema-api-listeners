import os
import time
import json
import argparse
import datetime as dt
from datetime import timezone
from typing import Any, Dict, List, Optional

import requests
import paho.mqtt.client as mqtt


# =========================================================
# 1. SMALL HELPERS
# =========================================================

def env_or(cli_val: Optional[str], env_key: str) -> str:
    """
    Return the CLI value if the user gave one.
    Otherwise fallback to an environment variable.
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
    """
    return (
        dt.datetime.fromtimestamp(epoch_s, tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def time_to_epoch_seconds(value: Any) -> int:
    """
    Convert either ISO time string or epoch seconds to int epoch seconds.
    """
    if isinstance(value, int):
        return value

    if isinstance(value, float):
        return int(value)

    if isinstance(value, str):
        return int(dt.datetime.fromisoformat(value).timestamp())

    raise RuntimeError(f"Unsupported time value: {value}")


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


def parse_entity_metrics_set(raw_json: str) -> List[Dict[str, Any]]:
    """
    Parse entity_metrics_set JSON.

    Expected:
      [
        {"entity": "D0167289", "metrics": ["Temperature.process-value"]},
        {"entity": "ssb.bioind4", "metrics": ["mem.used"]}
      ]
    """
    try:
        data = json.loads(raw_json)
    except json.JSONDecodeError as e:
        raise SystemExit(f"Bad --entity_metrics_set_json: {e}")

    if not isinstance(data, list) or len(data) == 0:
        raise SystemExit("--entity_metrics_set_json must be a non-empty JSON list.")

    seen_pairs = set()

    for item in data:
        if not isinstance(item, dict):
            raise SystemExit("Each entity_metrics_set item must be an object.")

        entity = item.get("entity")
        metrics = item.get("metrics")

        if not entity or not isinstance(entity, str):
            raise SystemExit("Each entity_metrics_set item must have non-empty string 'entity'.")

        if not isinstance(metrics, list) or len(metrics) == 0:
            raise SystemExit("Each entity_metrics_set item must have non-empty list 'metrics'.")

        for metric in metrics:
            if not metric or not isinstance(metric, str):
                raise SystemExit("Each metric must be a non-empty string.")

            pair = (entity, metric)
            if pair in seen_pairs:
                raise SystemExit(f"Duplicate entity/metric pair: {entity} + {metric}")
            seen_pairs.add(pair)

    return data


def requested_entities(entity_metrics_set: List[Dict[str, Any]]) -> List[str]:
    """
    Return unique entities preserving input order.
    """
    out: List[str] = []
    seen = set()

    for item in entity_metrics_set:
        entity = item["entity"]
        if entity not in seen:
            seen.add(entity)
            out.append(entity)

    return out


def requested_metrics(entity_metrics_set: List[Dict[str, Any]]) -> List[str]:
    """
    Return unique metrics preserving input order.
    """
    out: List[str] = []
    seen = set()

    for item in entity_metrics_set:
        for metric in item["metrics"]:
            if metric not in seen:
                seen.add(metric)
                out.append(metric)

    return out


def requested_pairs(entity_metrics_set: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """
    Flatten entity_metrics_set into explicit requested pairs.
    """
    pairs: List[Dict[str, str]] = []

    for item in entity_metrics_set:
        entity = item["entity"]
        for metric in item["metrics"]:
            pairs.append({"entity": entity, "metric": metric})

    return pairs


# =========================================================
# 2. CLI ARGUMENTS
# =========================================================

def parse_args() -> argparse.Namespace:
    """
    Define all command-line arguments.
    """
    p = argparse.ArgumentParser(
        description="LEAF listener that fetches requested entity/metric series, sends them to a modeler, and writes modeler predictions to MQTT."
    )

    # Modeler connection parameters
    p.add_argument("--target_service", required=True)
    p.add_argument("--target_endpoint", required=True)
    p.add_argument("--port", required=True)

    # LEAF API connection parameters
    p.add_argument("--api_url", default=None, help="LEAF API URL, or env LEAF_API_URL")
    p.add_argument("--token", default=None, help="LEAF API token, or env LEAF_API_TOKEN")

    # Required LEAF query fields
    p.add_argument("--organisation", required=True)
    p.add_argument("--department", required=True)
    p.add_argument(
        "--entity_metrics_set_json",
        required=True,
        help="JSON list of {entity, metrics} objects"
    )

    # Polling cadence and LEAF limit
    p.add_argument("--everyTs", type=int, required=True, help="Polling cadence in seconds")
    p.add_argument(
        "--queryDelayS",
        type=int,
        default=0,
        help="Delay query window by N seconds to allow LEAF data to become available"
    )
    p.add_argument("--limit", type=int, default=1000, help="Maximum rows per LEAF polling window")

    # MQTT write-back parameters
    p.add_argument("--mqtt_host", default=None, help="MQTT host")
    p.add_argument("--mqtt_port", type=int, default=443, help="MQTT port")
    p.add_argument("--mqtt_username", default=None, help="MQTT username")
    p.add_argument("--mqtt_password", default=None, help="MQTT password")
    p.add_argument("--mqtt_topic", default=None, help="MQTT topic, e.g. athenarc/test/ilp")
    p.add_argument("--mqtt_basepath", default="mqtt", help="MQTT websocket basepath, e.g. mqtt")
    p.add_argument("--mqtt_measurement", default=None, help="Influx line protocol measurement")
    p.add_argument(
        "--output_tags",
        nargs="*",
        default=[],
        help="Optional extra output tags, e.g. --output_tags workflow=test producer=leaf-listener"
    )

    return p.parse_args()


# =========================================================
# 3. READ FROM LEAF: ONE API CALL PER LOOP
# =========================================================

def fetch_leaf_rows_once(
    session: requests.Session,
    api_url: str,
    token: str,
    organisation: str,
    department: str,
    entity_metrics_set: List[Dict[str, Any]],
    start_s: int,
    stop_s: int,
    limit: int,
) -> List[dict]:
    """
    Query LEAF exactly ONCE per polling window.

    LEAF supports comma-separated query params:
      entity=D0167289,ssb.bioind4
      metric=Temperature.process-value,mem.used
    """
    entities = requested_entities(entity_metrics_set)
    metrics = requested_metrics(entity_metrics_set)

    params = {
        "organisation": organisation,
        "department": department,
        "entity": ",".join(entities),
        "metric": ",".join(metrics),
        "from": to_iso_z(start_s),
        "to": to_iso_z(stop_s),
        "limit": str(limit),
    }

    print(
        f"[listener] Fetching entities={entities}, metrics={metrics} "
        f"from {to_iso_z(start_s)} to {to_iso_z(stop_s)}"
    )

    response = session.get(
        api_url,
        params=params,
        headers={
            "accept": "application/json",
            "Authorization": f"Bearer {token}",
        },
        timeout=(10, 300),
    )

    response.raise_for_status()

    data = response.json()

    if not isinstance(data, list):
        raise RuntimeError("LEAF API response must be a JSON list.")

    print(f"[listener] LEAF returned {len(data)} total row(s)")

    return data


# =========================================================
# 4. BUILD MODELER PAYLOAD
# =========================================================

def build_modeler_payload(
    leaf_rows: List[dict],
    entity_metrics_set: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Convert raw LEAF rows into the modeler payload.

    Output example:
      [
        {
          "entity": "D0167289",
          "metric": "Temperature.process-value",
          "time": [178169...],
          "values": [23.325, 23.34]
        },
        {
          "entity": "ssb.bioind4",
          "metric": "mem.used",
          "time": [178169...],
          "values": [10410549248]
        }
      ]

    Time values are UTC epoch seconds.
    """
    pairs = requested_pairs(entity_metrics_set)
    requested_pair_set = {(pair["entity"], pair["metric"]) for pair in pairs}

    grouped: Dict[tuple, List[dict]] = {
        (pair["entity"], pair["metric"]): []
        for pair in pairs
    }

    for row in leaf_rows:
        entity = row.get("entity")
        metric = row.get("metric")
        pair_key = (entity, metric)

        if pair_key not in requested_pair_set:
            continue

        grouped[pair_key].append(row)

    payload: List[Dict[str, Any]] = []

    for pair in pairs:
        entity = pair["entity"]
        metric = pair["metric"]
        rows = grouped[(entity, metric)]

        # Oldest -> newest
        rows = sorted(rows, key=lambda r: time_to_epoch_seconds(r["time"]))

        time_values: List[int] = []
        values: List[Any] = []

        for row in rows:
            time_values.append(time_to_epoch_seconds(row["time"]))
            values.append(row["value"])

        print(
            f"[listener] Series entity='{entity}', metric='{metric}' "
            f"has {len(values)} value(s)"
        )

        payload.append({
            "entity": entity,
            "metric": metric,
            "time": time_values,
            "values": values,
        })

    return payload


# =========================================================
# 5. MQTT WRITE-BACK HELPERS
# =========================================================

def build_line_protocol_rows(
    modeler_output: Dict[str, Any],
    measurement: str,
    output_tags: List[str],
) -> List[str]:
    """
    Convert modeler output dict-of-lists into ONE line-protocol row PER POINT.

    Expected modeler output:
      {
        "x_pred": [223, 250],
        "y_pred": [104, 108],
        "z_pred": [true, false],
        "ts": [1781690000, 1781690001]
      }

    Timestamp protocol:
    - Listener sends time to modeler in UTC epoch seconds.
    - Modeler must return ts in UTC epoch seconds.
    - Listener converts seconds -> nanoseconds only here, for Influx line protocol.

    Important:
    - Modeler decides the output field names.
    - Every non-ts key becomes an Influx field.
    - organisation / department / entity / metric are NOT written as tags or field names.
    """
    if not measurement:
        raise RuntimeError("MQTT measurement is required.")

    if not isinstance(modeler_output, dict):
        raise RuntimeError("Modeler output must be a JSON object/dict.")

    if "ts" not in modeler_output:
        raise RuntimeError("Modeler output must contain 'ts' in UTC epoch seconds.")

    if not isinstance(modeler_output["ts"], list):
        raise RuntimeError("Modeler output key 'ts' must be a list of UTC epoch seconds.")

    ts_values = modeler_output["ts"]
    n = len(ts_values)

    field_keys = [k for k in modeler_output.keys() if k != "ts"]

    if not field_keys:
        raise RuntimeError("Modeler output must contain at least one non-'ts' field.")

    # Validate timestamps first.
    for i, ts_raw in enumerate(ts_values):
        if isinstance(ts_raw, bool) or not isinstance(ts_raw, (int, float)):
            raise RuntimeError(
                f"Modeler output 'ts' values must be epoch seconds as numbers. "
                f"Bad value at index {i}: {ts_raw!r}"
            )

        ts_int = int(ts_raw)

        if ts_int < 0:
            raise RuntimeError(
                f"Modeler output 'ts' must be non-negative epoch seconds. "
                f"Bad value at index {i}: {ts_raw!r}"
            )

        # 10,000,000,000 seconds is year 2286+.
        # If value is larger, it is probably milliseconds/nanoseconds by mistake.
        if ts_int >= 10_000_000_000:
            raise RuntimeError(
                f"Modeler output 'ts' looks too large for epoch seconds. "
                f"Expected seconds, got {ts_raw!r} at index {i}."
            )

    # Validate all prediction columns.
    for k in field_keys:
        if not isinstance(modeler_output[k], list):
            raise RuntimeError(f"Modeler output key '{k}' must be a list.")

        if len(modeler_output[k]) != n:
            raise RuntimeError(
                f"Length mismatch for '{k}'. Expected {n}, got {len(modeler_output[k])}."
            )

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
        # Convert seconds -> nanoseconds for Influx line protocol.
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
    args = parse_args()

    if args.everyTs <= 0:
        raise SystemExit("--everyTs must be a positive integer.")

    if args.limit <= 0:
        raise SystemExit("--limit must be a positive integer.")

    if args.queryDelayS < 0:
        raise SystemExit("--queryDelayS must be zero or positive.")

    api_url = env_or(args.api_url, "LEAF_API_URL")
    token = env_or(args.token, "LEAF_API_TOKEN")

    mqtt_host = env_or_none(args.mqtt_host, "MQTT_HOST")
    mqtt_username = env_or_none(args.mqtt_username, "MQTT_USERNAME")
    mqtt_password = env_or_none(args.mqtt_password, "MQTT_PASSWORD")
    mqtt_topic = env_or_none(args.mqtt_topic, "MQTT_TOPIC")

    entity_metrics_set = parse_entity_metrics_set(args.entity_metrics_set_json)

    endpoint = args.target_endpoint.lstrip("/")
    modeler_url = f"http://{args.target_service}:{args.port}/{endpoint}"
    print(f"modeler_url: {modeler_url}")

    session = requests.Session()

    now_s = int(dt.datetime.now(timezone.utc).timestamp())
    stop_s = now_s - args.queryDelayS
    start_s = stop_s - args.everyTs

    tick = time.monotonic()

    while True:
        try:
            # =============================================
            # A. Fetch requested rows from LEAF in ONE call
            # =============================================
            start_query_time = time.monotonic()

            leaf_rows = fetch_leaf_rows_once(
                session=session,
                api_url=api_url,
                token=token,
                organisation=args.organisation,
                department=args.department,
                entity_metrics_set=entity_metrics_set,
                start_s=start_s,
                stop_s=stop_s,
                limit=args.limit,
            )

            elapsed = time.monotonic() - start_query_time
            window_size = stop_s - start_s

            print(f"[listener] fetch_leaf_rows_once took {elapsed:.2f} sec")
            print(f"[listener] queried window size was {window_size} sec")

            # =============================================
            # B. Build modeler payload as list of series
            # =============================================
            payload = build_modeler_payload(
                leaf_rows=leaf_rows,
                entity_metrics_set=entity_metrics_set,
            )

            has_data = any(len(series["values"]) > 0 for series in payload)

            if not has_data:
                print("[listener] No rows found for the requested entity/metric pairs in this window.")
            else:
                print(f"[listener] Sending {len(payload)} series to modeler")
                print(f"payload: {payload}")

                # =============================================
                # C. Send payload to modeler
                # =============================================
                response = session.post(
                    modeler_url,
                    json=payload,
                    timeout=(10, 300),
                )
                response.raise_for_status()

                # =============================================
                # D. Read modeler response JSON
                # =============================================
                modeler_output = response.json()
                print("[listener] Modeler response JSON:")
                print(modeler_output)

                # =============================================
                # E. MQTT single-point write-back
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
            print(f"[listener] ERROR: {e}")
            raise

        tick += args.everyTs
        sleep_s = tick - time.monotonic()

        if sleep_s > 0:
            print(f"[listener] Sleeping {sleep_s:.2f} sec")
            time.sleep(sleep_s)
        else:
            print("[listener] Loop exceeded cadence; next query starts immediately")
            tick = time.monotonic()

        start_s = stop_s
        stop_s = int(dt.datetime.now(timezone.utc).timestamp()) - args.queryDelayS


# =========================================================
# 7. ENTRY POINT
# =========================================================

if __name__ == "__main__":
    main()
