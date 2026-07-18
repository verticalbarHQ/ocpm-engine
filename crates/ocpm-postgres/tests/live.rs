use ocpm_postgres::{
    ActivityProfileFilter, activity_profile, dfg_counts, dfg_window_counts, variant_counts,
    variant_window_counts,
};
use std::time::{Duration, SystemTime};

#[tokio::test]
async fn public_adapters_prepare_and_bind_against_pg_ocpm_0_4() {
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
