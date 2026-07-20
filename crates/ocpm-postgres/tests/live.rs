use futures_util::TryStreamExt;
use ocpm_core::binding::BindingSchema;
use ocpm_postgres::{
    ActivityProfileFilter, AdapterError, PreparedBindingQuery, PreparedBindingTreeQuery,
    PreparedEventLogQuery, RelationBindingSpec, activity_profile, binding_relation_universal_equal,
    dfg_counts, dfg_window_counts, variant_counts, variant_window_counts,
};
use std::time::{Duration, SystemTime};

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
