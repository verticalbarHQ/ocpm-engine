use futures_util::TryStreamExt;
use ocpm_core::binding::BindingSchema;
use ocpm_postgres::{
    ActivityProfileFilter, AdapterError, LifecycleDfgFilter, PreparedBindingQuery,
    PreparedBindingTreeQuery, PreparedEventLogQuery, PreparedEventWindowBatchQuery,
    RelationBindingSpec, activity_profile, binding_index_coverage,
    binding_relation_universal_equal, dfg_counts, dfg_window_counts, event_log_summary,
    event_log_window_summaries, lifecycle_dfg_window_counts, pg_ocpm_capabilities, variant_counts,
    variant_window_counts,
    load_canonical_snapshot,
};
use std::time::{Duration, SystemTime};

#[tokio::test]
async fn canonical_pg_ocpm_1_snapshot_round_trips() {
    let Ok(database_url) = std::env::var("OCPM_TEST_DATABASE_URL") else {
        return;
    };
    let (client, connection) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
        .await
        .expect("connect to pg_ocpm canonical provider database");
    tokio::spawn(async move {
        connection.await.expect("drive PostgreSQL connection");
    });
    let version: String = client
        .query_one("SELECT ocpm.version()", &[])
        .await
        .expect("read pg_ocpm version")
        .get(0);
    if version.split('.').next().and_then(|value| value.parse::<u32>().ok()) != Some(1) {
        return;
    }
    let tenant_id = 91_i64;
    client
        .execute(
            "SELECT set_config('ocpm.tenant_id', $1, false)",
            &[&tenant_id.to_string()],
        )
        .await
        .unwrap();
    let dataset_name = format!("ocpm-engine-v1-snapshot-{}", std::process::id());
    let dataset_id: i64 = client
        .query_one(
            "SELECT ocpm.register_dataset($1, $2, '{}'::jsonb)",
            &[&dataset_name, &tenant_id],
        )
        .await
        .unwrap()
        .get(0);
    let generation_id: i64 = client
        .query_one(
            "SELECT ocpm.begin_generation($1, $2, NULL)",
            &[&tenant_id, &dataset_id],
        )
        .await
        .unwrap()
        .get(0);
    client
        .execute(
            "INSERT INTO ocpm.object_entity
                 (tenant_id,dataset_id,external_object_id,object_type,last_changed_generation)
             VALUES ($1,$2,'o1','order',$3)",
            &[&tenant_id, &dataset_id, &generation_id],
        )
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO ocpm.event_entity
                 (tenant_id,dataset_id,external_event_id,activity,event_timestamp,last_changed_generation)
             VALUES
                 ($1,$2,'e1','create','2026-01-01T00:00:01Z',$3),
                 ($1,$2,'e2','approve','2026-01-01T00:00:02Z',$3)",
            &[&tenant_id, &dataset_id, &generation_id],
        )
        .await
        .unwrap();
    client
        .execute(
            "INSERT INTO ocpm.event_object_relation
                 (tenant_id,dataset_id,event_id,object_id,qualifier,last_changed_generation)
             SELECT $1,$2,event.event_id,object.object_id,'order',$3
             FROM ocpm.event_entity AS event
             CROSS JOIN ocpm.object_entity AS object
             WHERE event.tenant_id=$1 AND event.dataset_id=$2
               AND object.tenant_id=$1 AND object.dataset_id=$2",
            &[&tenant_id, &dataset_id, &generation_id],
        )
        .await
        .unwrap();
    client
        .execute(
            "SELECT ocpm.validate_generation($1,$2,$3,'{}'::jsonb,'{}'::jsonb)",
            &[&tenant_id, &dataset_id, &generation_id],
        )
        .await
        .unwrap();
    client
        .execute(
            "SELECT ocpm.publish_generation($1,$2,$3)",
            &[&tenant_id, &dataset_id, &generation_id],
        )
        .await
        .unwrap();

    let log = load_canonical_snapshot(&client, tenant_id, dataset_id)
        .await
        .expect("load canonical snapshot");
    assert_eq!(log.events.len(), 2);
    assert_eq!(log.objects.len(), 1);
    assert_eq!(log.event_object_relations.len(), 2);
    assert_eq!(log.events[0].activity, "create");

    client
        .execute("DELETE FROM ocpm.dataset WHERE dataset_id=$1", &[&dataset_id])
        .await
        .unwrap();
}

#[tokio::test]
async fn public_adapters_prepare_and_bind_against_pg_ocpm_0_7() {
    let Ok(database_url) = std::env::var("OCPM_TEST_DATABASE_URL") else {
        return;
    };
    let (client, connection) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
        .await
        .expect("connect to pg_ocpm test database");
    tokio::spawn(async move {
        connection.await.expect("drive PostgreSQL connection");
    });

    let start = SystemTime::UNIX_EPOCH;
    let end = start + Duration::from_secs(1);
    assert!(
        dfg_counts(&client, 0, 0, start, end)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        variant_counts(&client, 0, 0, start, end)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        dfg_window_counts(&client, 0, 0, vec![start], vec![end])
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        variant_window_counts(&client, 0, 0, vec![start], vec![end])
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        activity_profile(&client, 0, 0, start, end, &ActivityProfileFilter::default(),)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn native_event_adapter_streams_pg_ocpm_0_8_rows_in_case_order() {
    let Ok(database_url) = std::env::var("OCPM_TEST_DATABASE_URL") else {
        return;
    };
    let (client, connection) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
        .await
        .expect("connect to pg_ocpm event-stream test database");
    tokio::spawn(async move {
        connection.await.expect("drive PostgreSQL connection");
    });
    let version: String = client
        .query_one("SELECT ocpm.version()", &[])
        .await
        .expect("read pg_ocpm version")
        .get(0);
    let minor_version = version
        .split('.')
        .nth(1)
        .and_then(|value| value.parse::<u32>().ok());
    if version
        .split('.')
        .next()
        .and_then(|value| value.parse::<u32>().ok())
        == Some(0)
        && version
            .split('.')
            .nth(1)
            .and_then(|value| value.parse::<u32>().ok())
            .is_some_and(|minor| minor < 8)
    {
        return;
    }

    let dataset_key = format!("ocpm-engine-event-stream-{}", std::process::id());
    let dataset_id: i64 = client
        .query_one(
            "SELECT ocpm.register_dataset($1, 0, '{}'::jsonb)",
            &[&dataset_key],
        )
        .await
        .expect("register event-stream test dataset")
        .get(0);
    client
        .execute(
            r#"
            INSERT INTO ocpm.case_summary (
                dataset_id, tenant_id, case_id, object_type, status,
                start_time, end_time, execution_time,
                activity_path, path_text, path_hash,
                activities, event_timestamps
            ) VALUES (
                $1, 0, 11, 'Order', 'complete',
                '2026-01-01 00:00:00+00', '2026-01-01 00:00:01+00', 1.0,
                '["Create","Complete"]', 'Create > Complete', 'stream-path',
                ARRAY['Create','Complete'],
                ARRAY['2026-01-01 00:00:00+00',
                      '2026-01-01 00:00:01+00']::timestamptz[]
            )
            "#,
            &[&dataset_id],
        )
        .await
        .expect("insert event-stream case");
    client
        .execute("SELECT ocpm.finish_load($1)", &[&dataset_id])
        .await
        .expect("finalize event-stream case");

    let query = PreparedEventLogQuery::prepare(&client)
        .await
        .expect("prepare native event stream");
    let mut stream = std::pin::pin!(
        query
            .query(
                &client,
                dataset_id,
                0,
                "Order",
                SystemTime::UNIX_EPOCH,
                SystemTime::UNIX_EPOCH + Duration::from_secs(4_102_444_800),
            )
            .await
            .expect("start native event stream")
    );
    let mut events = Vec::new();
    while let Some(row) = stream.try_next().await.expect("read native event row") {
        events.push(PreparedEventLogQuery::decode(&row).expect("decode event row"));
    }
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].case_id, 11);
    assert_eq!(events[0].activity, "Create");
    assert_eq!(events[0].event_ordinal, 1);
    assert_eq!(events[1].activity, "Complete");
    assert_eq!(events[1].event_ordinal, 2);

    let capabilities = pg_ocpm_capabilities(&client)
        .await
        .expect("detect pg_ocpm event export capabilities");
    assert_eq!(capabilities.version, version);
    let factorized = minor_version.is_some_and(|minor| minor >= 9);
    assert_eq!(capabilities.factorized_event_export(), factorized);
    assert_eq!(capabilities.factorized_multi_window_export(), factorized);
    let lifecycle_pushdown = minor_version.is_some_and(|minor| minor >= 10);
    assert_eq!(capabilities.lifecycle_dfg_pushdown(), lifecycle_pushdown);
    assert_eq!(
        capabilities.lifecycle_variant_pushdown(),
        lifecycle_pushdown
    );

    let execution = event_log_summary(
        &client,
        dataset_id,
        0,
        "Order",
        SystemTime::UNIX_EPOCH,
        SystemTime::UNIX_EPOCH + Duration::from_secs(4_102_444_800),
    )
    .await
    .expect("summarize the event export selected by capabilities");
    assert_eq!(
        execution.strategy,
        if factorized {
            ocpm_postgres::EventLogStrategy::FactorizedBatch
        } else {
            ocpm_postgres::EventLogStrategy::NativeRowFallback
        }
    );
    assert_eq!(execution.database_rows, if factorized { 1 } else { 2 });
    assert_eq!(
        (execution.summary.case_count, execution.summary.event_count),
        (1, 2)
    );
    assert_eq!(
        execution.summary.variants[0].activity_path,
        ["Create", "Complete"]
    );
    assert_eq!(execution.summary.dfg[0].frequency, 1);
    assert_eq!(execution.summary.dfg[0].mean_duration_seconds, 1.0);

    let from = [SystemTime::UNIX_EPOCH, SystemTime::UNIX_EPOCH];
    let to = [
        SystemTime::UNIX_EPOCH + Duration::from_secs(4_102_444_800),
        SystemTime::UNIX_EPOCH + Duration::from_secs(4_102_444_800),
    ];
    let summaries = event_log_window_summaries(&client, dataset_id, 0, "Order", &from, &to)
        .await
        .expect("summarize aligned windows with the selected export");
    assert_eq!(summaries.len(), 2);
    assert!(
        summaries
            .iter()
            .all(|item| item.database_rows == if factorized { 1 } else { 2 })
    );
    assert!(summaries.iter().all(|item| item.summary.event_count == 2));

    if factorized {
        assert!(capabilities.factorized_event_export());
        assert!(capabilities.factorized_multi_window_export());

        let window_query = PreparedEventWindowBatchQuery::prepare(&client)
            .await
            .expect("prepare multi-window batch export");
        let mut batches = std::pin::pin!(
            window_query
                .query(&client, dataset_id, 0, "Order", &from, &to)
                .await
                .expect("start multi-window batch export")
        );
        let mut ordinals = Vec::new();
        while let Some(row) = batches.try_next().await.expect("read window batch") {
            let batch = PreparedEventWindowBatchQuery::decode(&row)
                .expect("decode a factorized window batch");
            ordinals.push(batch.window_ordinal);
            assert_eq!(batch.batch.case_count(), 1);
        }
        assert_eq!(ordinals, [1, 2]);

        let from_many = vec![SystemTime::UNIX_EPOCH; 257];
        let to_many = vec![SystemTime::UNIX_EPOCH + Duration::from_secs(4_102_444_800); 257];
        let many_summaries =
            event_log_window_summaries(&client, dataset_id, 0, "Order", &from_many, &to_many)
                .await
                .expect("chunk more than 256 windows in one statement snapshot");
        assert_eq!(many_summaries.len(), 257);
        assert!(
            many_summaries
                .iter()
                .all(|item| { item.database_rows == 1 && item.summary.event_count == 2 })
        );

        let coverage = binding_index_coverage(&client, dataset_id, 0)
            .await
            .expect("read observable binding-index coverage");
        assert!(coverage.refreshed_at.is_some());
        assert!(coverage.object_types.is_empty());
        assert!(!coverage.covers_object_type("Order"));
    }

    if lifecycle_pushdown {
        let counts = lifecycle_dfg_window_counts(
            &client,
            dataset_id,
            0,
            &from,
            &to,
            &LifecycleDfgFilter {
                object_types: Some(vec!["Order".to_owned()]),
                ..LifecycleDfgFilter::default()
            },
        )
        .await
        .expect("read exact lifecycle DFG counts");
        assert_eq!(counts.len(), 1);
        assert_eq!(counts[0].transition.source, "Create");
        assert_eq!(counts[0].transition.target, "Complete");
        assert_eq!(counts[0].frequencies, [1, 1]);

        let from_many = vec![SystemTime::UNIX_EPOCH; 257];
        let to_many = vec![SystemTime::UNIX_EPOCH + Duration::from_secs(4_102_444_800); 257];
        let thresholded = lifecycle_dfg_window_counts(
            &client,
            dataset_id,
            0,
            &from_many,
            &to_many,
            &LifecycleDfgFilter {
                object_types: Some(vec!["Order".to_owned()]),
                minimum_total_frequency: 2,
                ..LifecycleDfgFilter::default()
            },
        )
        .await
        .expect("apply a frequency threshold after aligning all chunks");
        assert_eq!(thresholded.len(), 1);
        assert_eq!(thresholded[0].frequencies, vec![1; 257]);
    }

    client
        .execute("SELECT ocpm.clear_dataset($1)", &[&dataset_id])
        .await
        .expect("clear event-stream test dataset");
    client
        .execute(
            "DELETE FROM ocpm.dataset WHERE dataset_id=$1",
            &[&dataset_id],
        )
        .await
        .expect("delete event-stream test dataset");
}

#[tokio::test]
async fn relation_adapter_distinguishes_empty_from_missing_summary() {
    let Ok(database_url) = std::env::var("OCPM_TEST_DATABASE_URL") else {
        return;
    };
    let (client, connection) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
        .await
        .expect("connect to relation-adapter test database");
    tokio::spawn(async move {
        connection.await.expect("drive PostgreSQL connection");
    });

    let dataset_key = format!("ocpm-engine-live-{}", std::process::id());
    let dataset_id: i64 = client
        .query_one(
            "SELECT ocpm.register_dataset($1, 0, '{}'::jsonb)",
            &[&dataset_key],
        )
        .await
        .expect("register relation-adapter test dataset")
        .get(0);
    let declared = RelationBindingSpec {
        source_object_type: "Case",
        source_activity: "Approve",
        target_object_type: "Task",
        target_activity: "Create",
        related_object_type: "Actor",
    };
    client
        .execute(
            r#"
            INSERT INTO ocpm.binding_relation_summary (
                dataset_id, tenant_id,
                source_object_type, source_activity,
                target_object_type, target_activity, related_object_type,
                source_binding_count, target_binding_count, payload
            ) VALUES (
                $1, 0, $2, $3, $4, $5, $6, 0, 0,
                ocpm.binding_relation_pack(
                    ARRAY[]::bigint[], ARRAY[]::bigint[],
                    ARRAY[]::bigint[], ARRAY[]::bigint[],
                    ARRAY[]::bigint[]
                )
            )
            "#,
            &[
                &dataset_id,
                &declared.source_object_type,
                &declared.source_activity,
                &declared.target_object_type,
                &declared.target_activity,
                &declared.related_object_type,
            ],
        )
        .await
        .expect("insert a valid empty relation summary");

    let empty = binding_relation_universal_equal(&client, dataset_id, 0, &declared)
        .await
        .expect("decode a declared empty relation summary");
    assert_eq!(empty.row_count(), 0);

    let missing = RelationBindingSpec {
        target_activity: "Missing",
        ..declared
    };
    assert!(matches!(
        binding_relation_universal_equal(&client, dataset_id, 0, &missing).await,
        Err(AdapterError::MissingRelationSummary)
    ));

    client
        .execute(
            "DELETE FROM ocpm.dataset WHERE dataset_id = $1",
            &[&dataset_id],
        )
        .await
        .expect("delete relation-adapter test dataset");
}

#[tokio::test]
async fn prepared_binding_query_reuses_a_plan_and_rejects_invalid_shapes() {
    let Ok(database_url) = std::env::var("OCPM_TEST_DATABASE_URL") else {
        return;
    };
    let (client, connection) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
        .await
        .expect("connect to prepared binding-query test database");
    tokio::spawn(async move {
        connection.await.expect("drive PostgreSQL connection");
    });

    let query = PreparedBindingQuery::prepare(&client, "SELECT decode('4f435042010400','hex')")
        .await
        .expect("prepare a valid binding query");
    let borrowed = query
        .execute_with(&client, &[], |bytes| {
            assert_eq!(bytes, b"OCPB\x01\x04\x00");
            bytes.len()
        })
        .await
        .expect("consume a borrowed binding query result");
    assert_eq!(borrowed.encoded_bytes, 7);
    assert_eq!(borrowed.value, 7);
    for _ in 0..2 {
        let result = query
            .execute(&client, &[])
            .await
            .expect("reuse the prepared binding query");
        assert_eq!(result.encoded_bytes, 7);
        assert_eq!(result.capsule.row_count(), 0);
    }

    assert!(matches!(
        PreparedBindingQuery::prepare(&client, "SELECT 1::integer").await,
        Err(AdapterError::InvalidBindingResultShape)
    ));
    assert!(matches!(
        PreparedBindingQuery::prepare(
            &client,
            "SELECT decode('4f435042010400','hex'), decode('4f435042010400','hex')",
        )
        .await,
        Err(AdapterError::InvalidBindingResultShape)
    ));

    let null_query = PreparedBindingQuery::prepare(&client, "SELECT NULL::bytea")
        .await
        .expect("prepare a nullable bytea query");
    assert!(matches!(
        null_query.execute(&client, &[]).await,
        Err(AdapterError::NullBindingCapsule)
    ));
}

#[tokio::test]
async fn prepared_binding_tree_query_decodes_order_and_rejects_invalid_nodes() {
    let Ok(database_url) = std::env::var("OCPM_TEST_DATABASE_URL") else {
        return;
    };
    let (client, connection) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
        .await
        .expect("connect to prepared binding-tree test database");
    tokio::spawn(async move {
        connection.await.expect("drive PostgreSQL connection");
    });

    const TWO_IDS_HEX: &str = "4f4350420108011428";
    const FIVE_IDS_VIOLATION_HEX: &str = "4f435042010b01020406080a01";
    let query = PreparedBindingTreeQuery::prepare(
        &client,
        &format!(
            "SELECT decode('{TWO_IDS_HEX}', 'hex'), decode('{FIVE_IDS_VIOLATION_HEX}', 'hex')"
        ),
    )
    .await
    .expect("prepare a valid two-node binding-tree query");
    assert_eq!(query.node_count(), 2);

    let result = query
        .execute(&client, &[])
        .await
        .expect("decode every prepared binding-tree node");
    assert_eq!(
        result.encoded_bytes,
        TWO_IDS_HEX.len() / 2 + FIVE_IDS_VIOLATION_HEX.len() / 2
    );
    assert_eq!(result.capsules[0].schema(), BindingSchema::TwoIds);
    assert_eq!(result.capsules[1].schema(), BindingSchema::FiveIdsViolation);
    assert_eq!(result.capsules[0].rows().next().unwrap().ids(), &[10, 20]);
    let violation = result.capsules[1].rows().next().unwrap();
    assert_eq!(violation.ids(), &[1, 2, 3, 4, 5]);
    assert_eq!(violation.violated, Some(true));

    assert!(matches!(
        PreparedBindingTreeQuery::prepare(&client, "SELECT 1::integer").await,
        Err(AdapterError::InvalidBindingTreeResultShape)
    ));
    assert!(matches!(
        PreparedBindingTreeQuery::prepare(
            &client,
            &format!("SELECT decode('{TWO_IDS_HEX}', 'hex'), 1::integer"),
        )
        .await,
        Err(AdapterError::InvalidBindingTreeResultShape)
    ));

    let null_query = PreparedBindingTreeQuery::prepare(
        &client,
        &format!("SELECT decode('{TWO_IDS_HEX}', 'hex'), NULL::bytea"),
    )
    .await
    .expect("prepare a nullable binding-tree node");
    assert!(matches!(
        null_query.execute(&client, &[]).await,
        Err(AdapterError::NullBindingTreeCapsule(1))
    ));
}
