"""Live differential coverage for selective dynamic DFG planning."""

from __future__ import annotations

import os
from datetime import datetime

import pytest

from ocpm_engine import (
    DynamicDfgRequest,
    DynamicFilter,
    EdgeFilter,
    EventAttributeFilter,
    OcpmEngine,
)
from ocpm_engine.engine import score_dynamic_dfg_rows

DATABASE_URL = os.environ.get("OCPM_DYNAMIC_TEST_DATABASE_URL")


@pytest.mark.skipif(
    not DATABASE_URL,
    reason="OCPM_DYNAMIC_TEST_DATABASE_URL is required for live PostgreSQL coverage",
)
def test_preselection_matches_broad_lifecycle_expansion_on_sap_o2c() -> None:
    """Keep mixed-filter semantics exact while reducing lifecycle expansion."""

    psycopg2 = pytest.importorskip("psycopg2")
    connection = psycopg2.connect(DATABASE_URL)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT item.attributes->>'event_attributes'
                FROM ocpm.event_chunk chunk
                CROSS JOIN LATERAL unnest(
                    chunk.case_ids, chunk.attributes
                ) AS item(case_id, attributes)
                WHERE chunk.dataset_id=1
                  AND chunk.tenant_id=1
                  AND chunk.object_type='MATERIAL'
                  AND item.case_id=1
                  AND item.attributes->>'event_attributes' IS NOT NULL
                LIMIT 1
                """
            )
            attribute_value = cursor.fetchone()[0]

            request = DynamicDfgRequest(
                backbone_type="MATERIAL",
                from_date=datetime.fromisoformat("1992-06-23T23:59:59.999999+00:00"),
                to_date=datetime.fromisoformat("2020-04-28T00:00:00.000001+00:00"),
                filter=DynamicFilter(
                    included_activities=("Create Purchase Requisition",),
                    included_event_attributes=(
                        EventAttributeFilter("event_attributes", attribute_value),
                    ),
                    included_related_object_types=("EINKBELEG",),
                    included_edges=(
                        EdgeFilter(
                            "Create Purchase Requisition",
                            "Create Purchase Requisition",
                            0.0,
                            0.0,
                        ),
                    ),
                    excluded_edges=(
                        EdgeFilter("__absent_source__", "__absent_target__", 0.0, 1.0),
                    ),
                ),
            )
            engine = OcpmEngine(dataset_id=1, tenant_id=1)

            case_plan = engine.build_dynamic_case_ids(request)
            assert case_plan.sql.count("JOIN preselected USING (case_id)") == 1
            broad_case_sql = case_plan.sql.replace(
                "JOIN preselected USING (case_id)", "JOIN base USING (case_id)"
            )
            cursor.execute(case_plan.sql, case_plan.params)
            optimized_case_ids = tuple(cursor.fetchone()[0])
            cursor.execute(broad_case_sql, case_plan.params)
            broad_case_ids = tuple(cursor.fetchone()[0])

            assert optimized_case_ids == broad_case_ids
            assert 1 in optimized_case_ids

            dfg_plan = engine.build_dynamic_dfg(request)
            assert dfg_plan.sql.count("JOIN preselected USING (case_id)") == 1
            broad_dfg_sql = dfg_plan.sql.replace(
                "JOIN preselected USING (case_id)", "JOIN base USING (case_id)"
            )
            cursor.execute(dfg_plan.sql, dfg_plan.params)
            optimized_dfg = score_dynamic_dfg_rows(list(cursor.fetchall()))
            cursor.execute(broad_dfg_sql, dfg_plan.params)
            broad_dfg = score_dynamic_dfg_rows(list(cursor.fetchall()))

            assert optimized_dfg == broad_dfg
            assert optimized_dfg["selected_count"] == len(optimized_case_ids)
            assert optimized_dfg["dfg"]
    finally:
        connection.close()
