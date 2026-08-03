#!/usr/bin/env python3
"""Load the public OCPQ BPIC 2017 OCEL into a clean pg_ocpm database."""

from __future__ import annotations

import argparse
import csv
import sqlite3
from pathlib import Path

import psycopg2

RAW_SCHEMA = """
DROP SCHEMA IF EXISTS bench CASCADE;
CREATE SCHEMA bench;
CREATE TABLE bench.object_dim (
    object_key bigint PRIMARY KEY,
    external_id text NOT NULL,
    object_type text NOT NULL
);
CREATE TABLE bench.event_dim (
    event_key bigint PRIMARY KEY,
    external_id text NOT NULL,
    activity text NOT NULL,
    event_timestamp timestamptz NOT NULL,
    resource text
);
CREATE TABLE bench.event_object (
    event_key bigint NOT NULL,
    object_key bigint NOT NULL,
    qualifier text NOT NULL
);
CREATE TABLE bench.object_object (
    source_object_key bigint NOT NULL,
    target_object_key bigint NOT NULL,
    qualifier text NOT NULL
);
CREATE INDEX bench_eo_object_event
    ON bench.event_object(object_key,event_key);
CREATE INDEX bench_eo_event_object
    ON bench.event_object(event_key,object_key);
CREATE INDEX bench_oo_source_target
    ON bench.object_object(source_object_key,target_object_key);
ANALYZE bench.object_dim;
ANALYZE bench.event_dim;
ANALYZE bench.event_object;
ANALYZE bench.object_object;
"""

NORMALIZE = """
CREATE EXTENSION pg_ocpm;
SELECT ocpm.register_dataset(
    'bpic2017-ocpq',1,
    '{"source":"OCPQ evaluation BPIC 2017 OCEL"}'::jsonb
);

DO $ocpq_invariants$
DECLARE
    invalid_resource_events bigint;
    root_applications_without_children bigint;
BEGIN
    WITH resource_counts AS (
        SELECT
            event.event_key,
            count(DISTINCT object.object_key) FILTER (
                WHERE object.object_type = 'Case_R'
            ) AS resource_count
        FROM bench.event_dim event
        LEFT JOIN bench.event_object relation USING (event_key)
        LEFT JOIN bench.object_dim object USING (object_key)
        WHERE event.activity IN ('A_Accepted', 'O_Created')
        GROUP BY event.event_key
    )
    SELECT count(*) INTO invalid_resource_events
    FROM resource_counts
    WHERE resource_count <> 1;

    IF invalid_resource_events <> 0 THEN
        RAISE EXCEPTION
            'OCPQ Q5 Case_R cardinality invariant failed; invalid events: %',
            invalid_resource_events;
    END IF;

    WITH roots AS (
        SELECT DISTINCT application.object_key AS application_id
        FROM bench.event_dim event
        JOIN bench.event_object relation USING (event_key)
        JOIN bench.object_dim application USING (object_key)
        WHERE event.activity = 'A_Accepted'
          AND application.object_type = 'Application'
    ), applications_with_created_offers AS (
        SELECT DISTINCT relation.source_object_key AS application_id
        FROM bench.object_object relation
        JOIN bench.object_dim application
          ON application.object_key = relation.source_object_key
        JOIN bench.object_dim offer
          ON offer.object_key = relation.target_object_key
        JOIN bench.event_object offer_event
          ON offer_event.object_key = offer.object_key
        JOIN bench.event_dim event USING (event_key)
        WHERE application.object_type = 'Application'
          AND offer.object_type = 'Offer'
          AND event.activity = 'O_Created'
    )
    SELECT count(*) INTO root_applications_without_children
    FROM roots
    LEFT JOIN applications_with_created_offers USING (application_id)
    WHERE applications_with_created_offers.application_id IS NULL;

    IF root_applications_without_children <> 0 THEN
        RAISE EXCEPTION
            'OCPQ Q5 root-child invariant failed; invalid roots: %',
            root_applications_without_children;
    END IF;
END
$ocpq_invariants$;

INSERT INTO ocpm.event_fact (
    dataset_id,tenant_id,case_id,object_id,external_object_id,
    object_type,activity,event_timestamp,context,updated_by,attributes,event_id
)
SELECT ocpm.dataset_id('bpic2017-ocpq'),1,
       eo.object_key,eo.object_key,object.external_id,
       object.object_type,event.activity,event.event_timestamp,NULL,event.resource,
       jsonb_build_object(
           'external_event_id',event.external_id,
           'event_qualifier',eo.qualifier
       ),event.event_key
FROM bench.event_object eo
JOIN bench.event_dim event USING(event_key)
JOIN bench.object_dim object USING(object_key);

WITH paths AS (
    SELECT eo.object_key AS case_id,
           min(object.object_type) AS object_type,
           min(event.event_timestamp) AS start_time,
           max(event.event_timestamp) AS end_time,
           extract(epoch FROM max(event.event_timestamp)-min(event.event_timestamp))
               ::double precision AS execution_time,
           jsonb_agg(event.activity ORDER BY event.event_timestamp,event.event_key)
               AS activity_path,
           json_agg(event.activity ORDER BY event.event_timestamp,event.event_key)
               ::text AS path_text,
           array_agg(DISTINCT event.activity ORDER BY event.activity) AS activities,
           array_agg(event.event_timestamp ORDER BY
                     event.event_timestamp,event.event_key)
               AS event_timestamps
    FROM bench.event_object eo
    JOIN bench.event_dim event USING(event_key)
    JOIN bench.object_dim object USING(object_key)
    GROUP BY eo.object_key
)
INSERT INTO ocpm.case_summary (
    dataset_id,tenant_id,case_id,object_type,status,start_time,end_time,
    execution_time,activity_path,path_text,path_hash,activities,event_timestamps
)
SELECT ocpm.dataset_id('bpic2017-ocpq'),1,case_id,object_type,NULL,
       start_time,end_time,execution_time,activity_path,path_text,
       substring(md5(path_text),1,10),activities,event_timestamps
FROM paths;

WITH bounds AS (
    SELECT min(event_timestamp) AS minimum_time,
           max(event_timestamp) AS maximum_time
    FROM bench.event_dim
), relations AS (
    SELECT relation.source_object_key AS from_object_id,
           source.object_type AS from_object_type,
           relation.target_object_key AS to_object_id,
           target.object_type AS to_object_type
    FROM bench.object_object relation
    JOIN bench.object_dim source
      ON source.object_key=relation.source_object_key
    JOIN bench.object_dim target
      ON target.object_key=relation.target_object_key
)
INSERT INTO ocpm.link_adjacency (
    dataset_id,tenant_id,from_object_id,from_object_type,
    to_object_id,to_object_type,source_timestamp,target_timestamp
)
SELECT ocpm.dataset_id('bpic2017-ocpq'),1,
       relations.from_object_id,relations.from_object_type,
       relations.to_object_id,relations.to_object_type,
       bounds.minimum_time,bounds.maximum_time
FROM relations CROSS JOIN bounds;

SELECT ocpm.finish_load(ocpm.dataset_id('bpic2017-ocpq'));
SELECT ocpm.rebuild_binding_index(
    ocpm.dataset_id('bpic2017-ocpq'),
    ARRAY['Application','Offer','Case_R']::text[],
    ARRAY[
        'A_Submitted','O_Created','O_Returned','A_Accepted','O_Accepted'
    ]::text[],
    ARRAY['O_Returned']::text[],
    '[{"source_object_type":"Application",'
    '"target_object_type":"Offer",'
    '"activities":["O_Accepted","O_Created"]}]'::jsonb,
    '[{"source_object_type":"Application",'
    '"source_activity":"A_Accepted",'
    '"target_object_type":"Offer",'
    '"target_activity":"O_Created",'
    '"related_object_type":"Case_R"}]'::jsonb
);

-- All serving and correctness lookups now come from the compact ocpm schema.
-- Remove the transient relational import so storage measures the complete
-- retained database representation rather than omitting support tables.
DROP SCHEMA bench CASCADE;
"""


def arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sqlite", required=True, type=Path)
    parser.add_argument("--csv-dir", type=Path, default=Path(".benchmarks/ocpq-csv"))
    parser.add_argument("--host", default="postgres_ocpm")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--database", default="postgres")
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password", default="pg")
    parser.add_argument(
        "--raw-only",
        action="store_true",
        help="retain only the benchmark-owned relational import for vanilla PG",
    )
    return parser.parse_args()


def quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def export_csv(source_path: Path, target: Path) -> None:
    target.mkdir(parents=True, exist_ok=True)
    source = sqlite3.connect(source_path)
    source.row_factory = sqlite3.Row
    objects = list(
        source.execute("SELECT ocel_id,ocel_type FROM object ORDER BY ocel_id")
    )
    object_keys = {row["ocel_id"]: index for index, row in enumerate(objects, 1)}
    with (target / "object_dim.csv").open("w", newline="") as output:
        csv.writer(output).writerows(
            (index, row["ocel_id"], row["ocel_type"])
            for index, row in enumerate(objects, 1)
        )

    events = list(
        source.execute("SELECT ocel_id,ocel_type FROM event ORDER BY ocel_id")
    )
    event_keys = {row["ocel_id"]: index for index, row in enumerate(events, 1)}
    with (target / "event_dim.csv").open("w", newline="") as output:
        writer = csv.writer(output)
        for (event_type,) in source.execute(
            "SELECT ocel_type FROM event_map_type ORDER BY ocel_type"
        ):
            table = quoted(f"event_{event_type}")
            for event_id, event_time, resource in source.execute(
                f"SELECT ocel_id,ocel_time,resource FROM {table}"
            ):
                writer.writerow(
                    (
                        event_keys[event_id],
                        event_id,
                        event_type,
                        event_time,
                        resource,
                    )
                )

    with (target / "event_object.csv").open("w", newline="") as output:
        writer = csv.writer(output)
        for event_id, object_id, qualifier in source.execute(
            "SELECT ocel_event_id,ocel_object_id,ocel_qualifier FROM event_object"
        ):
            writer.writerow((event_keys[event_id], object_keys[object_id], qualifier))

    with (target / "object_object.csv").open("w", newline="") as output:
        writer = csv.writer(output)
        for source_id, target_id, qualifier in source.execute(
            "SELECT ocel_source_id,ocel_target_id,ocel_qualifier FROM object_object"
        ):
            writer.writerow((object_keys[source_id], object_keys[target_id], qualifier))
    source.close()


def load(args: argparse.Namespace) -> None:
    export_csv(args.sqlite, args.csv_dir)
    connection = psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.database,
        user=args.user,
        password=args.password,
    )
    connection.autocommit = True
    with connection.cursor() as cursor:
        cursor.execute(RAW_SCHEMA)
        for table in (
            "object_dim",
            "event_dim",
            "event_object",
            "object_object",
        ):
            with (args.csv_dir / f"{table}.csv").open() as source:
                cursor.copy_expert(
                    f"COPY bench.{table} FROM STDIN WITH (FORMAT csv)", source
                )
        if args.raw_only:
            cursor.execute(
                "ANALYZE bench.object_dim; ANALYZE bench.event_dim; "
                "ANALYZE bench.event_object; ANALYZE bench.object_object"
            )
        else:
            cursor.execute(NORMALIZE)
    connection.close()


if __name__ == "__main__":
    load(arguments())
