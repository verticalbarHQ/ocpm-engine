use ocpm_postgres::{
    ActivityProfileFilter, AdapterError, PreparedBindingQuery, RelationBindingSpec,
    activity_profile, binding_relation_universal_equal, dfg_counts, dfg_window_counts,
    variant_counts, variant_window_counts,
};
use std::time::{Duration, SystemTime};

#[tokio::test]
async fn public_adapters_prepare_and_bind_against_pg_ocpm_0_6() {
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
                ocpm.binding_relation_universal_equal(
                    ocpm.binding_relation_pack(
                        ARRAY[]::bigint[], ARRAY[]::bigint[],
                        ARRAY[]::bigint[], ARRAY[]::bigint[],
                        ARRAY[]::bigint[]
                    )
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
