"""
InfluxDB streaming listener (InfluxDB 2)
- strict mode (fail fast)
- optional modeler-provided time vector
- auto-create output bucket if missing

Key point about Influx time:
- In InfluxDB 2, the timestamp column in query results is named `_time`.
- You do NOT create/write a column called `_time`.
- You set the timestamp of each point using `Point.time(...)`.
  Influx will expose it as `_time` when you query.

Meaning of --time_col_out:
- Whatever the user puts in --time_col_out is the NAME of the time vector returned by the modeler.
  Example: modeler returns {"ts_pred": [ ... ], "temp_pred": [ ... ]}
  then you call: --time_col_out ts_pred

What the listener does with that:
- It uses out["ts_pred"][i] as the point timestamp (=> shows up as `_time` in Influx queries).
- If you ALSO include ts_pred in --cols_out, then ts_pred is written as a normal FIELD too.
  (This is optional and sometimes handy for debugging.)

Fail-fast (Option B):
- Any mismatch (missing keys, bad types, etc.) raises and crashes the pod.
"""

import os
import time
import argparse
import datetime as dt
from datetime import timezone
from typing import Dict, List, Any, Optional

import requests
import influxdb_client
from influxdb_client import Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


# ----------------------------
# Helpers: tags & env fallback
# ----------------------------

def parse_kv_tags(pairs: Optional[List[str]]) -> Dict[str, str]:
    """
    Parse ["k=v", "a=b"] into {"k": "v", "a": "b"}.

    Influx tags are always strings. We keep strict parsing to fail early.
    """
    out: Dict[str, str] = {}
    if not pairs:
        return out
    for pair in pairs:
        if "=" not in pair:
            raise SystemExit(f"Bad tag '{pair}'. Expected key=value.")
        k, v = pair.split("=", 1)
        if not k:
            raise SystemExit(f"Bad tag '{pair}'. Empty key.")
        out[k] = v
    return out


def env_or(cli_val: Optional[str], env_key: str) -> str:
    """
    Prefer CLI arg if set, else fallback to environment variable.

    In k8s you should inject INFLUX_TOKEN via Secret -> env var.
    """
    if cli_val:
        return cli_val
    val = os.getenv(env_key)
    if not val:
        raise SystemExit(f"Missing {env_key} (env) and missing corresponding CLI flag.")
    return val


# ----------------------------
# Flux query builder
# ----------------------------

def build_flux_query(
    bucket: str,
    measurement: str,
    start_s: int,
    stop_s: int,
    fields: List[str],
    tags: Dict[str, str],
) -> str:
    """
    Build a Flux query for window [start, stop), filtered by measurement + optional fields + optional tags.

    NOTE:
    - query_api.query() can return many Flux "tables" even if you're querying one measurement,
      because Flux partitions results by group keys (tag sets, etc.).
    - We iterate all tables safely.
    - We pivot so each output record is one timestamp with multiple field columns.
    """
    start_iso = dt.datetime.fromtimestamp(start_s, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    stop_iso = dt.datetime.fromtimestamp(stop_s, tz=timezone.utc).isoformat().replace("+00:00", "Z")

    q: List[str] = []
    q.append(f'from(bucket: "{bucket}")')
    q.append(f'|> range(start: time(v: "{start_iso}"), stop: time(v: "{stop_iso}"))')
    q.append(f'|> filter(fn: (r) => r._measurement == "{measurement}")')

    if fields:
        field_expr = " or ".join([f'r._field == "{f}"' for f in fields])
        q.append(f'|> filter(fn: (r) => {field_expr})')

    if tags:
        tag_expr = " and ".join([f'r.{k} == "{v}"' for k, v in tags.items()])
        q.append(f'|> filter(fn: (r) => {tag_expr})')

    q.append('|> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")')
    return "\n".join(q)


# ----------------------------
# Output bucket management
# ----------------------------

def ensure_bucket_exists(client: influxdb_client.InfluxDBClient, org_name: str, bucket_name: str) -> None:
    """
    Ensure output bucket exists; create it if missing.

    - Buckets must exist before writing.
    - Measurements do NOT need to be created; they appear when you write points.

    Requires token permissions to manage buckets.
    """
    buckets_api = client.buckets_api()
    existing = buckets_api.find_bucket_by_name(bucket_name)
    if existing is not None:
        print(f"[listener] Output bucket exists: {bucket_name}")
        return

    print(f"[listener] Output bucket missing; creating: {bucket_name}")

    orgs_api = client.organizations_api()
    orgs = orgs_api.find_organizations(org=org_name)
    if not orgs:
        raise RuntimeError(f"Influx org '{org_name}' not found; cannot create bucket '{bucket_name}'.")

    org_id = orgs[0].id
    buckets_api.create_bucket(bucket_name=bucket_name, org_id=org_id)

    if buckets_api.find_bucket_by_name(bucket_name) is None:
        raise RuntimeError(f"Bucket creation attempted but '{bucket_name}' still not found.")

    print(f"[listener] Created output bucket: {bucket_name}")


# ----------------------------
# CLI args
# ----------------------------

def cmd_parser() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="InfluxDB listener (InfluxDB 2, strict, bucket auto-create)")

    # Modeler service
    # Your chosen format works for both:
    # - local: --target_service 127.0.0.1
    # - k8s:   --target_service <service-name>
    p.add_argument("--target_service", required=True, help="Modeler host or k8s service name")
    p.add_argument("--port", required=True, help="Modeler port")
    p.add_argument("--target_endpoint", required=True, help="Endpoint without leading '/', e.g. model")
    # p.add_argument("--namespace", required=False, default="", help="Unused (kept for compatibility)")

    # Influx creds (prefer env in k8s)
    p.add_argument("--influx_url", default=None, help="Influx URL or env INFLUX_URL")
    p.add_argument("--org", default=None, help="Influx org or env INFLUX_ORG")
    p.add_argument("--token", default=None, help="Influx token or env INFLUX_TOKEN")

    # Scheduling
    p.add_argument("--everyTs", type=int, required=True, help="Run loop every N seconds")

    # Input selection
    p.add_argument("--bucket_in", required=True)
    p.add_argument("--measurement_in", required=True)
    p.add_argument("--tags_in", nargs="*", default=[], help="Input tag filters: key=value ...")
    p.add_argument("--cols_in", nargs="*", default=[], help="Input fields to fetch (recommended: non-empty)")

    # Include input time to modeler (fixed key "ts")
    p.add_argument(
        "--include_time_to_mod",
        action="store_true",
        help="If set, send input timestamps to modeler under fixed key 'ts' (epoch int).",
    )

    # Output selection
    p.add_argument("--bucket_out", required=True)
    p.add_argument("--measurement_out", required=True)
    p.add_argument("--tags_out", nargs="*", default=[], help="Output tags: key=value ...")
    p.add_argument("--cols_out", nargs="*", default=[], help="Expected output field names (optional strict schema)")

    # Modeler output time key (optional, strict if provided)
    p.add_argument(
        "--time_col_out",
        default=None,
        help=(
            "Optional: name of the time list returned by the modeler (epoch int). "
            "If provided, modeler MUST return it as a list. "
            "Listener uses it to set the Influx point timestamp (which appears as `_time` in queries). "
            "If you also include it in --cols_out, it will be written as a FIELD too."
        ),
    )

    # Write precision: interpret timestamps accordingly
    # - If your time vectors are epoch seconds, use 's'
    # - If epoch milliseconds, use 'ms', etc.
    p.add_argument("--write_precision", default="s", choices=["s", "ms", "us", "ns"])

    return p.parse_args()


# ----------------------------
# Main loop
# ----------------------------

def main() -> None:
    args = cmd_parser()

    # --- CHANGE: enforce contract between --time_col_out and --cols_out ---
    # New rule: if user sets --time_col_out, they MUST also include that same key in --cols_out.
    # This makes the contract explicit and ensures the time vector is also persisted as a field.
    if args.time_col_out is not None:
        if not args.cols_out:
            raise SystemExit(
                "When --time_col_out is set, you must also provide --cols_out and include the time key there."
            )
        if args.time_col_out not in args.cols_out:
            raise SystemExit(
                f"When --time_col_out is set to '{args.time_col_out}', it must also appear in --cols_out."
            )

    if args.everyTs <= 0:
        raise SystemExit("--everyTs must be a positive integer.")

    # Resolve creds
    influx_url = env_or(args.influx_url, "INFLUX_URL")
    influx_org = env_or(args.org, "INFLUX_ORG")
    influx_token = env_or(args.token, "INFLUX_TOKEN")

    # Parse tags
    tags_in = parse_kv_tags(args.tags_in)
    tags_out = parse_kv_tags(args.tags_out)

    # Build modeler URL (works locally and in k8s if service name resolves)
    endpoint = args.target_endpoint.lstrip("/")
    modeler_url = f"http://{args.target_service}:{args.port}/{endpoint}"

    # HTTP session + timeouts
    session = requests.Session()
    DEFAULT_TIMEOUT = (3.0, 20.0)

    # Influx client
    client = influxdb_client.InfluxDBClient(url=influx_url, token=influx_token, org=influx_org)
    query_api = client.query_api()
    write_api = client.write_api(write_options=SYNCHRONOUS)

    # Ensure output bucket exists
    ensure_bucket_exists(client=client, org_name=influx_org, bucket_name=args.bucket_out)

    precision_map = {"s": WritePrecision.S, "ms": WritePrecision.MS, "us": WritePrecision.US, "ns": WritePrecision.NS}
    wp = precision_map[args.write_precision]

    # Rolling window (epoch seconds)
    now_s = int(dt.datetime.now(timezone.utc).timestamp())
    start_s = now_s - args.everyTs
    stop_s = now_s + 1  # small buffer

    # Align loop cadence with monotonic time
    tick = time.monotonic()

    while True:
        try:
            # 1) Build Flux query for current window
            flux = build_flux_query(
                bucket=args.bucket_in,
                measurement=args.measurement_in,
                start_s=start_s,
                stop_s=stop_s,
                fields=args.cols_in,
                tags=tags_in,
            )

            # 2) Execute query (can return multiple Flux tables)
            tables = query_api.query(org=influx_org, query=flux)

            # 3) Convert query results to dict-of-lists
            #    - input_times: timestamps aligned with each row (epoch seconds)
            #    - payload_cols: each requested field -> list of values aligned with input_times
            input_times: List[int] = []
            payload_cols: Dict[str, List[Any]] = {c: [] for c in args.cols_in}

            row_count = 0
            for table in tables:
                for rec in table.records:
                    row_count += 1

                    # rec.get_time() returns a datetime -> convert to epoch seconds for JSON
                    input_times.append(int(rec.get_time().timestamp()))

                    # After pivot, field names are columns inside rec.values
                    # Use .get(...) to avoid KeyError if a field is missing at this timestamp
                    for c in args.cols_in:
                        payload_cols[c].append(rec.values.get(c))

            if row_count == 0:
                print("[listener] No new results in this window.")
            else:
                print(f"[listener] Got {row_count} record(s) from Influx window {start_s}->{stop_s}")
                payload: Dict[str, Any] = dict(payload_cols)

                # Optionally include time for modeler under fixed name 'ts'
                if args.include_time_to_mod:
                    payload["ts"] = input_times

                # 4) Call modeler
                resp = session.post(modeler_url, json=payload, timeout=DEFAULT_TIMEOUT)
                resp.raise_for_status()
                out = resp.json()

                if not isinstance(out, dict):
                    raise RuntimeError("Modeler response must be a JSON object (dict-of-lists).")

                # 5) Determine the OUTPUT timestamp vector (used to set the Influx point timestamp => `_time`)
                out_times = input_times  # default: reuse input `_time`

                if args.time_col_out is not None:
                    # strict contract: modeler must return this key as a list
                    if args.time_col_out not in out:
                        raise RuntimeError(
                            f"--time_col_out='{args.time_col_out}' was set, but modeler did not return that key."
                        )
                    if not isinstance(out[args.time_col_out], list):
                        raise RuntimeError(
                            f"--time_col_out='{args.time_col_out}' was set, but modeler returned "
                            f"{type(out[args.time_col_out])}, expected list."
                        )
                    # IMPORTANT: We use these values as the point timestamp.
                    # Influx will show them in queries as `_time` automatically.
                    out_times = [int(x) for x in out[args.time_col_out]]

                # 6) Decide which keys will be written as FIELDS
                # If the user gave cols_out, we enforce that schema.
                # Otherwise, we write every key returned by the modeler.
                #
                # Special rule:
                # - If time_col_out was provided, that key normally represents "time".
                # - BUT if the user included that key in cols_out, we keep it as a field too.
                keep_time_as_field = (
                    args.time_col_out is not None
                    and args.cols_out
                    and args.time_col_out in args.cols_out
                )

                if args.time_col_out is not None and not keep_time_as_field:
                    # treat modeler time vector as time-only (not a field)
                    out_fields_all = {k: v for k, v in out.items() if k != args.time_col_out}
                else:
                    # keep everything as fields (including time key if requested in cols_out)
                    out_fields_all = dict(out)

                if args.cols_out:
                    out_fields: Dict[str, List[Any]] = {}
                    for k in args.cols_out:
                        if k not in out_fields_all:
                            raise RuntimeError(f"Modeler did not return expected output field '{k}'.")
                        out_fields[k] = out_fields_all[k]
                else:
                    out_fields = out_fields_all

                # 7) Validate alignment: all field lists must match out_times length
                n = len(out_times)
                for k, v in out_fields.items():
                    if not isinstance(v, list):
                        raise RuntimeError(f"Output field '{k}' must be a list, got {type(v)}.")
                    if len(v) != n:
                        raise RuntimeError(f"Length mismatch for '{k}': expected {n}, got {len(v)}.")

                # 8) Write points to Influx
                # NOTE: The timestamp is set via pt.time(...). It becomes `_time` in query results.
                points: List[Point] = []
                for i in range(n):
                    pt = Point(args.measurement_out)

                    # constant output tags
                    for tk, tv in tags_out.items():
                        pt = pt.tag(tk, tv)

                    # fields from modeler output
                    for fk, fvals in out_fields.items():
                        # If this field is also the time vector key, store as integer field for consistency.
                        if args.time_col_out is not None and fk == args.time_col_out:
                            pt = pt.field(fk, int(fvals[i]))
                        else:
                            pt = pt.field(fk, fvals[i])

                    # point timestamp -> shows as `_time` in InfluxDB 2
                    pt = pt.time(out_times[i], wp)

                    points.append(pt)

                write_api.write(bucket=args.bucket_out, org=influx_org, record=points, write_precision=wp)
                print(f"[listener] Wrote {n} point(s) to bucket='{args.bucket_out}', measurement='{args.measurement_out}'")

        except Exception as e:
            # Option B: crash the pod (fail fast)
            print(f"[listener] FATAL ERROR: {e}")
            raise

        # Sleep until next tick
        tick += args.everyTs
        sleep_s = tick - time.monotonic()
        if sleep_s > 0:
            time.sleep(sleep_s)
        else:
            tick = time.monotonic()

        # Advance window
        start_s = stop_s
        stop_s = int(dt.datetime.now(timezone.utc).timestamp()) + 1


if __name__ == "__main__":
    main()
