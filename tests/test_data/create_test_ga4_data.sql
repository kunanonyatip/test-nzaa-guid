-- Create test events for today
INSERT INTO `${ga4_project}.${ga4_dataset}.events_${date_suffix}`
(event_date, event_timestamp, event_name, user_pseudo_id, user_properties, event_params, items)
VALUES
  -- User 1: Complete profile with all platforms
  (
    '${event_date}',
    UNIX_MILLIS(CURRENT_TIMESTAMP()),
    'page_view',
    'test_user_001',
    [
      STRUCT('guid_email' AS key, 
             STRUCT('a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3' AS string_value,
                    CAST(NULL AS INT64) AS int_value,
                    CAST(NULL AS FLOAT64) AS float_value,
                    CAST(NULL AS FLOAT64) AS double_value,
                    UNIX_MILLIS(CURRENT_TIMESTAMP()) AS set_timestamp_micros) AS value),
      STRUCT('guid_fb_id' AS key,
             STRUCT('FB_1234567890' AS string_value,
                    CAST(NULL AS INT64) AS int_value,
                    CAST(NULL AS FLOAT64) AS float_value,
                    CAST(NULL AS FLOAT64) AS double_value,
                    UNIX_MILLIS(CURRENT_TIMESTAMP()) AS set_timestamp_micros) AS value),
      STRUCT('guid_tiktok_id' AS key,
             STRUCT('TT_ABCDEF123456' AS string_value,
                    CAST(NULL AS INT64) AS int_value,
                    CAST(NULL AS FLOAT64) AS float_value,
                    CAST(NULL AS FLOAT64) AS double_value,
                    UNIX_MILLIS(CURRENT_TIMESTAMP()) AS set_timestamp_micros) AS value)
    ],
    [],
    []
  ),
  
  -- User 2: Partial profile
  (
    '${event_date}',
    UNIX_MILLIS(CURRENT_TIMESTAMP()),
    'form_submit',
    'test_user_002',
    [
      STRUCT('guid_email' AS key,
             STRUCT('b493d48364afe44d11c0165cf470a4164d1e2609911ef998be868d46ade3de4e' AS string_value,
                    CAST(NULL AS INT64) AS int_value,
                    CAST(NULL AS FLOAT64) AS float_value,
                    CAST(NULL AS FLOAT64) AS double_value,
                    UNIX_MILLIS(CURRENT_TIMESTAMP()) AS set_timestamp_micros) AS value)
    ],
    [],
    []
  ),
  
  -- User 3: Same email as User 1 but different device (cross-device scenario)
  (
    '${event_date}',
    UNIX_MILLIS(CURRENT_TIMESTAMP()),
    'page_view',
    'test_user_003',
    [
      STRUCT('guid_email' AS key,
             STRUCT('a665a45920422f9d417e4867efdc4fb8a04a1f3fff1fa07e998e86f7f7a27ae3' AS string_value,
                    CAST(NULL AS INT64) AS int_value,
                    CAST(NULL AS FLOAT64) AS float_value,
                    CAST(NULL AS FLOAT64) AS double_value,
                    UNIX_MILLIS(CURRENT_TIMESTAMP()) AS set_timestamp_micros) AS value),
      STRUCT('guid_reddit_id' AS key,
             STRUCT('RD_TEST_USER_123' AS string_value,
                    CAST(NULL AS INT64) AS int_value,
                    CAST(NULL AS FLOAT64) AS float_value,
                    CAST(NULL AS FLOAT64) AS double_value,
                    UNIX_MILLIS(CURRENT_TIMESTAMP()) AS set_timestamp_micros) AS value)
    ],
    [],
    []
  );