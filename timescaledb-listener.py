import psycopg
from psycopg import sql
import time
import argparse
from datetime import datetime, timezone, timedelta
from typing import Iterable, List, Union
import sys, os
import requests
from urllib.parse import urlparse



# {
#     "streaming": "timescale",
#     "data": {
#         "db_profile": "timescaleDB",
#         "psql_in": {
#             "table_in": "metrics",
#             "everyTs": 30,
#             "cols_in": ["ts", "device_id", "value"],
#             "time_col_in": "ts"
#         },
#         "psql_out": {
#             "table_out": "test_predictions_2",
#             "time_col_out": "ts_pred",
#             "cols_types_out": ["ts_pred:int8", "device_id:text", "value_pred:float8"],
#             "make_hypertable": true
#             // "chunk_interval": "1 hour",
#             // "migrate_existing": false
#         },
#         "modeler": {
#             "image": "pankalos/modeler:timescaledb-dummy-latest",
#             "port": 8080,
#             // "include_time_to_mod": false,
#             // "rename_inp_time_col_to_mod_as": "...",
#             "endpoint": "model",
#             "args": ["python", "modeler.py"]
#         }
#     }
# }


def host_from_arg(s: str) -> str:
    p = urlparse(s)
    return p.hostname or s



# If Modeler User decides to include ts column in the Modeler, Listener expects a ts_column as a Response!

def cmd_parser():
    p = argparse.ArgumentParser(description="TimescaleDB listener")

    # Modeler
    p.add_argument('--modeler_service', required=True)
    p.add_argument('--modeler_endpoint', required=True, help="Endpoint without leading slash, e.g. model")
    # p.add_argument('--modeler_namespace', required=True)
    p.add_argument('--modeler_port', required=True)

    p.add_argument('--include_time_to_mod', action='store_true', help='If set, include the time column as sec in the JSON payload sent to the modeler.')
    p.add_argument('--rename_inp_time_col_to_mod_as', default=None, help='Optional key name for the time vector in the modeler payload. Defaults to --time_col_in if not provided.')

    # PostgreSQL Input
    p.add_argument('--psql_host', required=True)
    p.add_argument('--psql_port', required=True)
    p.add_argument('--psql_dbname', required=True)
    p.add_argument('--psql_user', required=True)
    p.add_argument('--psql_password', required=True)

    # PostegreSQL Output
    p.add_argument('--psql_dbtable_out', required=True)
    p.add_argument('--time_col_out', required=True, help="Time column in output table. PostgreSQL(Timestamptz) -> Listener(Timestamptz -> INT) -> Modeler(INT) --> Listener (INT -> TimestampTZ) --> PostgreSQL(Timestamptz)")
    p.add_argument('--cols_types_out', nargs='+', required=True, help=('Output columns as name:type (types must be single tokens). '
                                                                 'Use aliases like float8,int4,int8,bool,text,jsonb,real. '
                                                                 'Example: xhat:float8 yhat:float8 zhat:int8'))

    p.add_argument('--make_hypertable', action='store_true', help='If set, calls timescaledb create_hypertable on the output table.')
    p.add_argument('--chunk_interval', default='1 hour', help='Timescale chunk interval if make_hypertable is set.')
    p.add_argument('--migrate_existing', action='store_true', help='bool, set TRUE only if converting a non-empty table')

    # Query params
    p.add_argument('--psql_dbtable_in', required=True)
    p.add_argument('--everyTs', type=int, required=True)
    p.add_argument('--cols_in', nargs='+', required=True, help="e.g. hum temp ts")
    p.add_argument('--time_col_in', required=True, help="Name of the time column used for the SELECT FROM WHERE query. e.g: ts")

    args = p.parse_args()


    print("Modeler")
    print(f"  service: {args.modeler_service}, endpoint: {args.modeler_endpoint}, port: {args.modeler_port}, include_time_to_mod: {args.include_time_to_mod}, rename_inp_time_col_to_mod_as: {args.rename_inp_time_col_to_mod_as} ")
    print("PostgreSQL")
    print(f"  host: {args.psql_host} ({urlparse(args.psql_host).hostname}), port: {args.psql_port}, db: {args.psql_dbname}, table_in: {args.psql_dbtable_in}, user: {args.psql_user}, password: {'*' * len(args.psql_password)}")
    print("Query Params")
    print(f"everyTs: {args.everyTs}, cols_in: {args.cols_in}, time_col_in: {args.time_col_in}")
    print(f'Output')
    print(f'psql_dbtable_out: {args.psql_dbtable_out}, time_col_out: {args.time_col_out}, cols_types_out: {args.cols_types_out}')
    print(f'make_hypertable: {args.make_hypertable}, chunk_interval: {args.chunk_interval}, migrate_existing: {args.migrate_existing}')
    print()

    return args



def parse_out_cols(specs):
    out = []
    for s in specs:
        if ':' not in s:
            raise ValueError(f'Bad out_cols entry "{s}". Use "name:type".')
        name, typ = s.split(':', 1)
        name, typ = name.strip(), typ.strip()
        if ' ' in typ:
            raise ValueError(f'Type "{typ}" contains spaces. Use a single-token alias (e.g., float8).')
        out.append((name, typ))
    return out



Number = Union[int, float]

def epoch_seconds_to_utc(values: Iterable[Number]) -> List[datetime]:
    """
    Convert epoch **seconds** to tz-aware UTC datetimes (for timestamptz).
    Accepts ints or floats (floats keep sub-second precision).
    """
    return [datetime.fromtimestamp(float(s), tz=timezone.utc) for s in values]




def encode_time_vector(dts):
    """
    dts: list[datetime]; mode in {'iso','epoch_ms','epoch_s'}
    returns list[str|int]
    """
    out = []
    for dt in dts:
        # ensure tz-aware UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)

        out.append(int(dt.timestamp()))

    return out



def _qualified_ident(name: str) -> sql.Composable:  # Or sql.SQL:
    """Quote schema-qualified names safely: 'schema.table' -> "schema"."table"."""
    parts = [p.strip() for p in name.split('.')]
    return sql.SQL('.').join(sql.Identifier(p) for p in parts)


def ensure_output_table(conn, table, time_col, cols_types,
                        make_hypertable=False,
                        chunk_interval='1 hour',
                        migrate_existing=False):
    """
    Behavior:
      - If table does NOT exist: CREATE TABLE with PK(time_col) inline.
      - If table exists: DO NOT recreate; DO NOT add PK; only (optionally) apply hypertable.
    Assumption: After the first call, the table already has a PK on time_col.
    """

    def table_exists(cur) -> bool:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL", (table,))
        return cur.fetchone()[0]

    def pk_exists(cur) -> bool:
        cur.execute("""
            SELECT 1
            FROM pg_constraint
            WHERE conrelid = to_regclass(%s)
              AND contype = 'p'
        """, (table,))
        return cur.fetchone() is not None

    # --- Build CREATE TABLE (PK inline) ---
    cols = [sql.SQL("{} timestamptz NOT NULL").format(sql.Identifier(time_col))]
    for name, typ in cols_types:
        cols.append(sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(typ)))
    cols.append(sql.SQL("PRIMARY KEY ({})").format(sql.Identifier(time_col)))

    create_q = sql.SQL("CREATE TABLE IF NOT EXISTS {tbl} ({cols})").format(
        tbl=_qualified_ident(table),
        cols=sql.SQL(", ").join(cols),
    )

    # --- Hypertable command (idempotent) ---
    migrate_clause = sql.SQL("migrate_data => TRUE,") if migrate_existing else sql.SQL("")
    create_hyper_q = sql.SQL("""
        SELECT create_hypertable({tbl}, {time_col},
            if_not_exists => TRUE,
            {migrate_clause}
            chunk_time_interval => INTERVAL {chunk})
    """).format(
        tbl=sql.Literal(table),                # regclass/text arg
        time_col=sql.Literal(time_col),        # column name as text
        migrate_clause=migrate_clause,
        chunk=sql.Literal(chunk_interval),     # e.g. '1 hour'
    )

    with conn.cursor() as cur:
        if not table_exists(cur):
            # First call: create table with PK inline
            cur.execute(create_q)
        else:
            # Subsequent calls: assume PK already exists; assert for safety
            if not pk_exists(cur):
                raise RuntimeError(
                    f"Table '{table}' exists but has no PRIMARY KEY. "
                    "This function will not add it by design."
                )

        # Optional hypertable step (safe to call repeatedly)
        if make_hypertable:
            cur.execute(create_hyper_q)

    # Commit DDL/Timescale changes explicitly
    conn.commit()



def write_model_output(conn,
                       table: str,
                       time_col: str,
                       cols_types: list[tuple[str, str]],
                       ts_vec: list[datetime],
                       out_dict: dict) -> int:
    """
    Minimal bulk insert using executemany.
    - Requires a PK/UNIQUE on time_col (so ON CONFLICT works).
    - Inserts (time_col + data columns) in the order provided by cols_types.
    - Ignores any time key present in out_dict.
    """

    names = [name for name, _ in cols_types]
    n = len(ts_vec)
    if n == 0:
        return 0


    # validate presence and lengths
    for name in names:
        if name not in out_dict:
            raise KeyError(f"Missing output column '{name}' in modeler response")
        if len(out_dict[name]) != n:
            raise ValueError(f"Length mismatch: ts({n}) vs {name}({len(out_dict[name])})")


    # build rows: (ts, col1, col2, ...)
    rows = list(zip(ts_vec, *[out_dict[name] for name in names]))


    # INSERT ... ON CONFLICT (time_col) DO NOTHING
    col_idents = [sql.Identifier(time_col)] + [sql.Identifier(nm) for nm in names]
    placeholders = sql.SQL(", ").join(sql.Placeholder() * (1 + len(names)))
    insert_q = sql.SQL("""
        INSERT INTO {tbl} ({cols}) VALUES ({ph}) 
        ON CONFLICT ({tc}) DO NOTHING"""
    ).format(
        tbl=_qualified_ident(table),
        cols=sql.SQL(", ").join(col_idents),
        ph=placeholders,
        tc=sql.Identifier(time_col),
    )

    with conn.cursor() as cur:
        cur.executemany(insert_q.as_string(conn), rows)
    conn.commit()

    return n




def main():
    args = cmd_parser()

    # --- CHANGE: enforce contract between --time_col_out and --cols_types_out ---
    # New rule: the output time column name must ALSO be declared in --cols_types_out as name:type.
    # Example: --time_col_out ts_pred --cols_types_out ts_pred:int8 value_pred:float8 ...
    # Note: this listener still creates the time column as timestamptz internally; the presence here is for explicitness.
    out_col_names = [s.split(':', 1)[0].strip() for s in args.cols_types_out]
    if args.time_col_out not in out_col_names:
        raise SystemExit(
            f"--time_col_out was set to '{args.time_col_out}' but it is missing from --cols_types_out. "
            "Add it as '<time_col_out>:timestamptz'."
        )

    cols_types_out = parse_out_cols(args.cols_types_out)
    cols_types_out = [(col, typ) for col, typ in cols_types_out if col != args.time_col_out]

    print(f'cols_types_out: {cols_types_out}')

    # Build modeler URL (works locally and in k8s if service name resolves)
    endpoint = args.modeler_endpoint.lstrip("/")
    # url = f"http://{args.modeler_service}:{args.modeler_port}/{endpoint}"

    ms = urlparse(args.modeler_service)
    scheme = ms.scheme if ms.scheme else "http"
    mhost = ms.hostname or args.modeler_service
    url = f"{scheme}://{mhost}:{args.modeler_port}/{endpoint}"

    # K8s
    # url = f"http://{args.modeler_service}.{args.modeler_namespace}.svc.cluster.local:{args.modeler_port}/{args.modeler_endpoint.lstrip('/')}"
    # url = f"http://{args.modeler_service}:{args.modeler_port}/{args.modeler_endpoint.lstrip('/')}"

    # Local
    # url = f"http://127.0.0.1:8080/{args.modeler_endpoint.lstrip('/')}"

    print(f'modeler url: {url}')

    psql_host = host_from_arg(args.psql_host)
    print(f'psql host: {psql_host}')

    # Postgres connection
    conn = psycopg.connect(
        # host=urlparse(args.psql_host).hostname,
        host=psql_host,
        port=args.psql_port,
        dbname=args.psql_dbname,
        user=args.psql_user,
        password=args.psql_password,
        sslmode="require",
        application_name="listener"
    )

    conn.autocommit = True

    ensure_output_table(conn,
                        args.psql_dbtable_out,
                        args.time_col_out,
                        cols_types_out,
                        make_hypertable=args.make_hypertable,
                        chunk_interval=args.chunk_interval,
                        migrate_existing=args.migrate_existing)

    # HTTP session with timeout
    session = requests.Session()
    DEFAULT_TIMEOUT = (3.0, 10.0)  # (connect, read) seconds

    # Initial window: previous everyTs seconds until now
    dt_now = datetime.now(timezone.utc)
    start_time = dt_now - timedelta(seconds=args.everyTs)
    stop_time  = dt_now

    # Prebuild SELECT: SELECT col1, col2, ... FROM table WHERE ts >= %s AND ts < %s ORDER BY ts ASC
    # Build SELECT field list: ALWAYS include time_col_in once.
    # If the user also put it in --cols_in, we de-dup it here.
    fields_list = [args.time_col_in] + [c for c in args.cols_in if c != args.time_col_in]
    fields = sql.SQL(", ").join(map(sql.Identifier, fields_list))
    select_q = sql.SQL("""
        SELECT {fields}
        FROM {table}
        WHERE {time_col_in} >= %s AND {time_col_in} < %s
        ORDER BY {time_col_in} ASC
    """).format(fields=fields, table=sql.Identifier(args.psql_dbtable_in), time_col_in=sql.Identifier(args.time_col_in))

    tick = time.monotonic()

    with conn.cursor() as cur:
        while True:
            print(select_q.as_string(conn))
            print("params:", start_time, stop_time, "\n")

            cur.execute(select_q, (start_time, stop_time))
            rows = cur.fetchall()

            if rows:
                print(f"Results: {len(rows)} row(s)")
                # Rebuild per-iteration dict-of-lists
                col_dic_curr_in = {col: [] for col in fields_list}
                for r in rows:  # r: (v1, v2, v3, ...)
                    for i, col in enumerate(fields_list): # [(0, v1), (1, v2), ...
                        col_dic_curr_in[col].append(r[i])

                print(f'col_dic_curr_in: {col_dic_curr_in}')


                # Base payload: all selected columns except the time column
                payload = {k: v for k, v in col_dic_curr_in.items() if k != args.time_col_in}

                # Optionally include time column for the modeler
                if args.include_time_to_mod:
                    time_key = args.rename_inp_time_col_to_mod_as or args.time_col_in
                    ts_vec = col_dic_curr_in[args.time_col_in]
                    payload[time_key] = encode_time_vector(ts_vec)

                print(f"Payload to Modeler: {payload}")

                # Send to modeler
                resp = session.post(url, json=payload, timeout=DEFAULT_TIMEOUT)
                resp.raise_for_status()
                col_dic_curr_out = resp.json()

                print(f'col_dic_curr_out: {col_dic_curr_out}')

                # Pick timestamps for output table:
                # - if the modeler returned epoch seconds under --time_col_out, convert them
                # - else reuse input timestamps
                ts_vec_out = col_dic_curr_in[args.time_col_in]
                if args.include_time_to_mod and args.time_col_out in col_dic_curr_out:
                    ts_vec_out = epoch_seconds_to_utc(col_dic_curr_out[args.time_col_out])

                # Ensure we only insert data columns (exclude any time key from modeler)
                data_out = {k: v for k, v in col_dic_curr_out.items() if k != args.time_col_out}

                # Write
                rows_written = write_model_output(conn,
                                                  args.psql_dbtable_out,
                                                  args.time_col_out,
                                                  cols_types_out,  # from parse_out_cols, filtered earlier
                                                  ts_vec_out,
                                                  data_out)

                print(f"Inserted {rows_written} row(s) into {args.psql_dbtable_out}.")


            else:
                print("No results...")


            # Wait until next tick (fixed cadence)
            tick += args.everyTs
            sleep_s = tick - time.monotonic()
            if sleep_s > 0:
                time.sleep(sleep_s)
            else:
                # we're behind; skip sleeping this cycle
                tick = time.monotonic()

            # Compute next window
            start_time = stop_time
            stop_time = datetime.now(timezone.utc)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
