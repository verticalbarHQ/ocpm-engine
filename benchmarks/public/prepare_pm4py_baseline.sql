\set ON_ERROR_STOP on

-- Retain relational integrity indexes and one workload-specific secondary
-- B-tree for case lifecycle extraction. Remove indexes for unrelated query
-- families so this remains the lightly indexed PM4Py baseline.
DROP INDEX IF EXISTS ocel.ocel_event_time;
DROP INDEX IF EXISTS ocel.ocel_event_activity_time;
DROP INDEX IF EXISTS ocel.ocel_object_type;
DROP INDEX IF EXISTS ocel.ocel_e2o_event;
DROP INDEX IF EXISTS ocel.ocel_o2o_source;
DROP INDEX IF EXISTS ocel.ocel_o2o_target;

VACUUM (ANALYZE) ocel.event;
VACUUM (ANALYZE) ocel.object;
VACUUM (ANALYZE) ocel.event_object;
VACUUM (ANALYZE) ocel.object_object;
