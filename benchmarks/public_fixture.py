"""Build the clean public OCEL fixture used by ``public_common_pm.py``.

The loader downloads the SAP IDES O2C and P2P OCEL 2.0 SQLite archives from
Zenodo, verifies the published archive bytes, loads the same normalized facts
into two PostgreSQL databases, and removes the relational staging copy from the
pg_ocpm database after finalization.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sqlite3
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values

DATASETS = (
    {
        "name": "sap_o2c",
        "archive": "o2c.zip",
        "sqlite": "o2c.sqlite",
        "url": "https://zenodo.org/records/8261133/files/o2c.zip",
        "archive_sha256": (
            "8a5fab0af3f4b6e714dc7508fc0e99d10197eed466b57757cf55cdcbf9ba786f"
        ),
        "sqlite_sha256": (
            "8e0036cc461a0a0b77338cae5530f4edc5e13bf1538417baf384db3b364847d0"
        ),
    },
    {
        "name": "sap_p2p",
        "archive": "p2p.zip",
        "sqlite": "p2p.sqlite",
        "url": "https://zenodo.org/records/8261133/files/p2p.zip",
        "archive_sha256": (
            "32d4195ce8ebf838404f9c642a01a8dc218606eb08d9139e90aace82ae0b76dc"
        ),
        "sqlite_sha256": (
            "9fd163001e73b175d5424fbb773f2b3665b22421add25ae388c79ef8bdc11bdb"
        ),
    },
)

RAW_SCHEMA = """
CREATE SCHEMA ocel;
CREATE TABLE ocel.dataset (
    dataset_id bigint PRIMARY KEY,
    dataset_name text NOT NULL UNIQUE,
    metadata jsonb NOT NULL
);
CREATE TABLE ocel.event (
    dataset_id bigint NOT NULL,
    event_key bigint NOT NULL,
    external_event_id text NOT NULL,
    activity text NOT NULL,
    event_timestamp timestamptz NOT NULL,
    attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (dataset_id, event_key),
    UNIQUE (dataset_id, external_event_id)
);
CREATE TABLE ocel.object (
    dataset_id bigint NOT NULL,
    object_key bigint NOT NULL,
    external_object_id text NOT NULL,
    object_type text NOT NULL,
    attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (dataset_id, object_key),
    UNIQUE (dataset_id, external_object_id)
);
CREATE TABLE ocel.event_object (
    dataset_id bigint NOT NULL,
    event_key bigint NOT NULL,
    object_key bigint NOT NULL,
    object_type text NOT NULL,
    qualifier text NOT NULL DEFAULT '',
    PRIMARY KEY (dataset_id, event_key, object_key, qualifier)
);
CREATE TABLE ocel.object_object (
    dataset_id bigint NOT NULL,
    source_object_key bigint NOT NULL,
    source_object_type text NOT NULL,
    target_object_key bigint NOT NULL,
    target_object_type text NOT NULL,
    qualifier text NOT NULL DEFAULT '',
    PRIMARY KEY (dataset_id, source_object_key, target_object_key, qualifier)
);
"""

RAW_INDEXES = """
CREATE INDEX ocel_event_time
    ON ocel.event (dataset_id, event_timestamp, event_key)
    INCLUDE (activity, external_event_id);
CREATE INDEX ocel_event_activity_time
    ON ocel.event (dataset_id, activity, event_timestamp, event_key);
CREATE INDEX ocel_object_type
    ON ocel.object (dataset_id, object_type, object_key)
    INCLUDE (external_object_id);
CREATE INDEX ocel_e2o_object
    ON ocel.event_object (dataset_id, object_type, object_key, event_key)
    INCLUDE (qualifier);
CREATE INDEX ocel_e2o_event
    ON ocel.event_object (dataset_id, event_key, object_key)
    INCLUDE (object_type, qualifier);
CREATE INDEX ocel_o2o_source
    ON ocel.object_object (
        dataset_id, source_object_type, source_object_key, target_object_key
    );
CREATE INDEX ocel_o2o_target
    ON ocel.object_object (
        dataset_id, target_object_type, target_object_key, source_object_key
    );
"""

NORMALIZE_EVENTS = """
INSERT INTO ocpm.event_fact (
    dataset_id, tenant_id, case_id, object_id, external_object_id,
    object_type, activity, event_timestamp, context, updated_by, attributes,
    event_id
)
SELECT
    %(ocpm_dataset_id)s, 1, eo.object_key, eo.object_key,
    object.external_object_id, eo.object_type, event.activity,
    event.event_timestamp, NULL, NULL,
    jsonb_build_object(
        'external_event_id', event.external_event_id,
        'event_qualifier', eo.qualifier,
        'event_attributes', event.attributes
    ),
    event.event_key
FROM ocel.event_object eo
JOIN ocel.event event
  ON event.dataset_id=eo.dataset_id AND event.event_key=eo.event_key
JOIN ocel.object object
  ON object.dataset_id=eo.dataset_id AND object.object_key=eo.object_key
WHERE eo.dataset_id=%(raw_dataset_id)s
"""

NORMALIZE_EDGES = """
WITH ordered AS (
    SELECT
        eo.object_key, eo.object_type,
        event.external_event_id AS source_event_id,
        event.activity AS source_activity,
        event.event_timestamp AS source_timestamp,
        lead(event.external_event_id) OVER lifecycle AS target_event_id,
        lead(event.activity) OVER lifecycle AS target_activity,
        lead(event.event_timestamp) OVER lifecycle AS target_timestamp
    FROM ocel.event_object eo
    JOIN ocel.event event
      ON event.dataset_id=eo.dataset_id AND event.event_key=eo.event_key
    WHERE eo.dataset_id=%(raw_dataset_id)s
    WINDOW lifecycle AS (
        PARTITION BY eo.object_key
        ORDER BY event.event_timestamp, event.event_key
    )
)
INSERT INTO ocpm.edge_fact (
    dataset_id, tenant_id, case_id,
    source_activity, source_object_id, source_object_type, source_timestamp,
    target_activity, target_object_id, target_object_type, target_timestamp,
    execution_time, edge_type, context, updated_by, attributes
)
SELECT
    %(ocpm_dataset_id)s, 1, object_key,
    source_activity, object_key, object_type, source_timestamp,
    target_activity, object_key, object_type, target_timestamp,
    extract(epoch FROM target_timestamp-source_timestamp)::double precision,
    'directly_follows', NULL, NULL,
    jsonb_build_object(
        'source_event_id', source_event_id,
        'target_event_id', target_event_id
    )
FROM ordered
WHERE target_timestamp IS NOT NULL
"""

NORMALIZE_ADJACENCY = """
INSERT INTO ocpm.link_adjacency (
    dataset_id, tenant_id,
    from_object_id, from_object_type, to_object_id, to_object_type,
    source_timestamp, target_timestamp
)
SELECT DISTINCT
    %(ocpm_dataset_id)s, 1,
    source.object_key, source.object_type,
    target.object_key, target.object_type,
    event.event_timestamp, event.event_timestamp
FROM ocel.event_object source
JOIN ocel.event_object target
  ON target.dataset_id=source.dataset_id
 AND target.event_key=source.event_key
 AND target.object_key<>source.object_key
JOIN ocel.event event
  ON event.dataset_id=source.dataset_id AND event.event_key=source.event_key
WHERE source.dataset_id=%(raw_dataset_id)s
UNION
SELECT
    %(ocpm_dataset_id)s, 1,
    source_object_key, source_object_type,
    target_object_key, target_object_type,
    bounds.minimum_time, bounds.maximum_time
FROM ocel.object_object relation
CROSS JOIN (
    SELECT min(event_timestamp) AS minimum_time,
           max(event_timestamp) AS maximum_time
    FROM ocel.event WHERE dataset_id=%(raw_dataset_id)s
) bounds
WHERE relation.dataset_id=%(raw_dataset_id)s
UNION
SELECT
    %(ocpm_dataset_id)s, 1,
    target_object_key, target_object_type,
    source_object_key, source_object_type,
    bounds.minimum_time, bounds.maximum_time
FROM ocel.object_object relation
CROSS JOIN (
    SELECT min(event_timestamp) AS minimum_time,
           max(event_timestamp) AS maximum_time
    FROM ocel.event WHERE dataset_id=%(raw_dataset_id)s
) bounds
WHERE relation.dataset_id=%(raw_dataset_id)s
"""

NORMALIZE_CASES = """
WITH paths AS (
    SELECT
        eo.object_key AS case_id,
        min(eo.object_type) AS object_type,
        min(event.event_timestamp) AS start_time,
        max(event.event_timestamp) AS end_time,
        extract(epoch FROM max(event.event_timestamp)-min(event.event_timestamp))
            ::double precision AS execution_time,
        jsonb_agg(event.activity ORDER BY event.event_timestamp, event.event_key)
            AS activity_path,
        json_agg(event.activity ORDER BY event.event_timestamp, event.event_key)
            ::text AS path_text,
        array_agg(DISTINCT event.activity ORDER BY event.activity) AS activities,
        array_agg(event.event_timestamp ORDER BY event.event_timestamp, event.event_key)
            AS event_timestamps
    FROM ocel.event_object eo
    JOIN ocel.event event
      ON event.dataset_id=eo.dataset_id AND event.event_key=eo.event_key
    WHERE eo.dataset_id=%(raw_dataset_id)s
    GROUP BY eo.object_key
)
INSERT INTO ocpm.case_summary (
    dataset_id, tenant_id, case_id, object_type, status,
    start_time, end_time, execution_time,
    activity_path, path_text, path_hash, activities, event_timestamps
)
SELECT
    %(ocpm_dataset_id)s, 1, case_id, object_type, NULL,
    start_time, end_time, execution_time,
    activity_path, path_text, substring(md5(path_text),1,10), activities,
    event_timestamps
FROM paths
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-host", default="postgres_vanilla")
    parser.add_argument("--extension-host", default="postgres_ocpm")
    parser.add_argument("--baseline-db", default="ocel_benchmark")
    parser.add_argument("--extension-db", default="ocel_benchmark")
    parser.add_argument("--data-dir", default=".benchmarks/public-data")
    parser.add_argument("--output", default=".benchmarks/public-prepare.json")
    return parser.parse_args()


def connect(host: str, database: str):
    connection = psycopg2.connect(
        host=host,
        port=5432,
        dbname=database,
        user="postgres",
        password="pg",
        connect_timeout=15,
    )
    connection.autocommit = True
    return connection


def reset_database(host: str, database: str) -> None:
    if not re.fullmatch(r"[a-z][a-z0-9_]*", database):
        raise SystemExit(f"unsafe database name: {database}")
    admin = connect(host, "postgres")
    cursor = admin.cursor()
    cursor.execute(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
        "WHERE datname=%s AND pid<>pg_backend_pid()",
        (database,),
    )
    cursor.execute(f'DROP DATABASE IF EXISTS "{database}"')
    cursor.execute(f'CREATE DATABASE "{database}" TEMPLATE template0')
    cursor.close()
    admin.close()


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def obtain_dataset(data_dir: Path, specification: dict) -> Path:
    data_dir.mkdir(parents=True, exist_ok=True)
    archive = data_dir / specification["archive"]
    sqlite_path = data_dir / specification["sqlite"]
    if not archive.exists():
        temporary = archive.with_suffix(".download")
        print(f"downloading {specification['url']}", flush=True)
        with urllib.request.urlopen(specification["url"], timeout=120) as source:
            with temporary.open("wb") as target:
                while block := source.read(1024 * 1024):
                    target.write(block)
        temporary.replace(archive)
    archive_digest = sha256(archive)
    if archive_digest != specification["archive_sha256"]:
        raise SystemExit(f"archive digest mismatch for {archive}: {archive_digest}")
    if not sqlite_path.exists():
        with zipfile.ZipFile(archive) as compressed:
            members = compressed.namelist()
            if members != [specification["sqlite"]]:
                raise SystemExit(f"unexpected archive members for {archive}: {members}")
            compressed.extractall(data_dir)
    sqlite_digest = sha256(sqlite_path)
    if sqlite_digest != specification["sqlite_sha256"]:
        raise SystemExit(f"SQLite digest mismatch for {sqlite_path}: {sqlite_digest}")
    return sqlite_path


def quote_sqlite(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def utc_timestamp(value):
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def typed_rows(connection: sqlite3.Connection, kind: str) -> dict:
    mapping = connection.execute(
        f"SELECT ocel_type, ocel_type_map FROM {kind}_map_type"
    ).fetchall()
    details = {}
    for ocel_type, mapped_type in mapping:
        table = f"{kind}_{mapped_type}"
        columns = [
            row[1]
            for row in connection.execute(
                f"PRAGMA table_info({quote_sqlite(table)})"
            ).fetchall()
        ]
        for row in connection.execute(f"SELECT * FROM {quote_sqlite(table)}"):
            values = dict(zip(columns, row))
            external_id = str(values.pop("ocel_id"))
            event_time = values.pop("ocel_time", None)
            values = {key: value for key, value in values.items() if value is not None}
            details[external_id] = {
                "type": ocel_type,
                "time": event_time,
                "attributes": values,
            }
    return details


def read_ocel(
    path: Path,
    specification: dict,
    *,
    allow_orphan_object_relations: bool = False,
    event_id_tiebreak: bool = False,
) -> dict:
    source = sqlite3.connect(path)
    event_details = typed_rows(source, "event")
    object_details = typed_rows(source, "object")
    event_base = source.execute("SELECT ocel_id, ocel_type FROM event").fetchall()
    if event_id_tiebreak:
        event_base.sort(key=lambda row: str(row[0]))
    object_base = source.execute("SELECT ocel_id, ocel_type FROM object").fetchall()

    event_keys = {}
    events = []
    for event_key, (external_id, activity) in enumerate(event_base, 1):
        external_id = str(external_id)
        detail = event_details.get(external_id)
        if detail is None or detail["time"] is None:
            raise ValueError(f"missing typed event data for {external_id}")
        event_keys[external_id] = event_key
        events.append(
            (
                event_key,
                external_id,
                str(activity),
                utc_timestamp(detail["time"]),
                detail["attributes"],
            )
        )

    object_keys = {}
    objects = []
    for object_key, (external_id, object_type) in enumerate(object_base, 1):
        external_id = str(external_id)
        detail = object_details.get(external_id, {"attributes": {}})
        object_keys[external_id] = object_key
        objects.append(
            (
                object_key,
                external_id,
                str(object_type),
                detail["attributes"],
            )
        )

    event_objects = []
    for event_id, object_id, qualifier in source.execute(
        "SELECT ocel_event_id, ocel_object_id, ocel_qualifier FROM event_object"
    ):
        object_id = str(object_id)
        event_objects.append(
            (
                event_keys[str(event_id)],
                object_keys[object_id],
                objects[object_keys[object_id] - 1][2],
                qualifier or "",
            )
        )

    object_objects = []
    ignored_orphan_object_relations = 0
    for source_id, target_id, qualifier in source.execute(
        "SELECT ocel_source_id, ocel_target_id, ocel_qualifier FROM object_object"
    ):
        if str(source_id) not in object_keys or str(target_id) not in object_keys:
            if allow_orphan_object_relations:
                ignored_orphan_object_relations += 1
                continue
            raise ValueError(
                "object_object relationship references an object absent from the "
                f"object table: {source_id!r} -> {target_id!r}"
            )
        source_row = objects[object_keys[str(source_id)] - 1]
        target_row = objects[object_keys[str(target_id)] - 1]
        object_objects.append(
            (
                source_row[0],
                source_row[2],
                target_row[0],
                target_row[2],
                qualifier or "",
            )
        )
    source.close()
    return {
        "name": specification["name"],
        "source_url": specification["url"],
        "archive_sha256": specification["archive_sha256"],
        "sqlite_sha256": specification["sqlite_sha256"],
        "events": events,
        "objects": objects,
        "event_objects": event_objects,
        "object_objects": object_objects,
        "ignored_orphan_object_relations": ignored_orphan_object_relations,
    }


def batches(values: list, size: int = 10_000):
    for start in range(0, len(values), size):
        yield values[start : start + size]


def insert_values(cursor, statement: str, template: str, rows: list) -> None:
    for batch in batches(rows):
        execute_values(
            cursor, statement, batch, template=template, page_size=len(batch)
        )


def load_raw(connection, dataset_id: int, dataset: dict) -> None:
    cursor = connection.cursor()
    cursor.execute(
        "INSERT INTO ocel.dataset VALUES (%s,%s,%s::jsonb)",
        (
            dataset_id,
            dataset["name"],
            json.dumps(
                {
                    "format": "OCEL 2.0 relational SQLite",
                    "source_url": dataset["source_url"],
                    "sqlite_sha256": dataset["sqlite_sha256"],
                }
            ),
        ),
    )
    insert_values(
        cursor,
        "INSERT INTO ocel.event VALUES %s",
        "(%s,%s,%s,%s,%s,%s::jsonb)",
        [
            (dataset_id, key, external_id, activity, stamp, json.dumps(attrs))
            for key, external_id, activity, stamp, attrs in dataset["events"]
        ],
    )
    insert_values(
        cursor,
        "INSERT INTO ocel.object VALUES %s",
        "(%s,%s,%s,%s,%s::jsonb)",
        [
            (dataset_id, key, external_id, object_type, json.dumps(attrs))
            for key, external_id, object_type, attrs in dataset["objects"]
        ],
    )
    insert_values(
        cursor,
        "INSERT INTO ocel.event_object VALUES %s",
        "(%s,%s,%s,%s,%s)",
        [
            (dataset_id, event_key, object_key, object_type, qualifier)
            for event_key, object_key, object_type, qualifier in dataset[
                "event_objects"
            ]
        ],
    )
    insert_values(
        cursor,
        "INSERT INTO ocel.object_object VALUES %s",
        "(%s,%s,%s,%s,%s,%s)",
        [
            (dataset_id, source_id, source_type, target_id, target_type, qualifier)
            for source_id, source_type, target_id, target_type, qualifier in dataset[
                "object_objects"
            ]
        ],
    )
    cursor.close()


def relation_bytes(connection, schema: str) -> dict:
    cursor = connection.cursor()
    cursor.execute(
        """
        SELECT coalesce(sum(pg_total_relation_size(c.oid)),0)::bigint,
               coalesce(sum(pg_indexes_size(c.oid)),0)::bigint
        FROM pg_class c
        JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname=%s AND c.relkind IN ('r','m','p')
        """,
        (schema,),
    )
    total, indexes = cursor.fetchone()
    cursor.close()
    return {"total_bytes": int(total), "index_bytes": int(indexes)}


def prepare(args: argparse.Namespace) -> dict:
    reset_database(args.baseline_host, args.baseline_db)
    reset_database(args.extension_host, args.extension_db)
    baseline = connect(args.baseline_host, args.baseline_db)
    extension = connect(args.extension_host, args.extension_db)
    baseline.cursor().execute(RAW_SCHEMA)
    extension.cursor().execute("CREATE EXTENSION pg_ocpm")
    extension.cursor().execute(RAW_SCHEMA)

    parsed_datasets = []
    output = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "title": "Collection of Object-Centric Event Logs",
            "doi": "10.5281/zenodo.8261133",
            "license": "CC BY 4.0",
        },
        "datasets": [],
    }
    data_dir = Path(args.data_dir)
    for dataset_id, specification in enumerate(DATASETS, 1):
        path = obtain_dataset(data_dir, specification)
        print(f"reading {specification['name']} from {path}", flush=True)
        dataset = read_ocel(path, specification)
        parsed_datasets.append((dataset_id, dataset))
        started = time.perf_counter()
        load_raw(baseline, dataset_id, dataset)
        baseline_load = time.perf_counter() - started
        started = time.perf_counter()
        load_raw(extension, dataset_id, dataset)
        extension_load = time.perf_counter() - started
        output["datasets"].append(
            {
                "dataset_id": dataset_id,
                "name": dataset["name"],
                "source_url": dataset["source_url"],
                "archive_sha256": dataset["archive_sha256"],
                "sqlite_sha256": dataset["sqlite_sha256"],
                "counts": {
                    "events": len(dataset["events"]),
                    "objects": len(dataset["objects"]),
                    "event_object_links": len(dataset["event_objects"]),
                    "object_object_links": len(dataset["object_objects"]),
                },
                "raw_load_s": {
                    "baseline": round(baseline_load, 3),
                    "extension_staging": round(extension_load, 3),
                },
            }
        )

    for label, connection in (("baseline", baseline), ("extension", extension)):
        started = time.perf_counter()
        connection.cursor().execute(RAW_INDEXES)
        for table in ("event", "object", "event_object", "object_object"):
            connection.cursor().execute(f"VACUUM (ANALYZE) ocel.{table}")
        output[f"{label}_index_build_s"] = round(time.perf_counter() - started, 3)

    cursor = extension.cursor()
    for row, (raw_dataset_id, dataset) in zip(
        output["datasets"], parsed_datasets, strict=True
    ):
        cursor.execute(
            "SELECT ocpm.register_dataset(%s,1,%s::jsonb)",
            (
                dataset["name"],
                json.dumps(
                    {
                        "source": "OCEL 2.0 relational SQLite",
                        "source_url": dataset["source_url"],
                    }
                ),
            ),
        )
        ocpm_dataset_id = int(cursor.fetchone()[0])
        parameters = {
            "raw_dataset_id": raw_dataset_id,
            "ocpm_dataset_id": ocpm_dataset_id,
        }
        phases = {}
        for name, statement in (
            ("event_facts", NORMALIZE_EVENTS),
            ("directly_follows_edges", NORMALIZE_EDGES),
            ("object_adjacency", NORMALIZE_ADJACENCY),
            ("case_summaries", NORMALIZE_CASES),
        ):
            started = time.perf_counter()
            cursor.execute(statement, parameters)
            phases[name] = {
                "rows": int(cursor.rowcount),
                "elapsed_s": round(time.perf_counter() - started, 3),
            }
        started = time.perf_counter()
        cursor.execute("SELECT ocpm.finish_load(%s)", (ocpm_dataset_id,))
        row["normalized_counts"] = cursor.fetchone()[0]
        row["ocpm_dataset_id"] = ocpm_dataset_id
        row["phases"] = phases
        row["finalization_s"] = round(time.perf_counter() - started, 3)
        print(f"finalized {dataset['name']}", flush=True)

    cursor.execute("DROP SCHEMA ocel CASCADE")
    cursor.execute("SELECT ocpm.version(), current_setting('server_version')")
    output["pg_ocpm_version"], output["postgres_version"] = cursor.fetchone()
    cursor.close()
    output["storage"] = {
        "vanilla_postgres": relation_bytes(baseline, "ocel"),
        "pg_ocpm": relation_bytes(extension, "ocpm"),
    }
    baseline.close()
    extension.close()
    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(output, indent=2, default=str) + "\n")
    print(f"wrote {target}", flush=True)
    return output


if __name__ == "__main__":
    prepare(parse_args())
