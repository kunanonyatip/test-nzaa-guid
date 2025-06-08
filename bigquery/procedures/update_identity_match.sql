CREATE OR REPLACE PROCEDURE `${project_id}.${dataset_id}.update_identity_match`()
BEGIN
  DECLARE last_update_date STRING;
  SET last_update_date = (
    SELECT IFNULL(
      FORMAT_DATETIME("%Y%m%d", MAX(updated_date[OFFSET(ARRAY_LENGTH(updated_date) - 1)])),
      "20250603"
    )
    FROM `${project_id}.${dataset_id}.identity_match`
  );

  -- Update existing identity_match records
  UPDATE `${project_id}.${dataset_id}.identity_match` identity_match
  SET identity_match.updated_date = ARRAY_CONCAT(identity_match.updated_date, [PARSE_DATETIME("%Y%m%d", result.latest_date)])
  FROM (
    SELECT 
      email.value.string_value as hashed_email,
      user_pseudo_id as ga_id,
      MAX(event_date) as latest_date
    FROM `${ga4_project}.${ga4_dataset}.events_*`,
    UNNEST(user_properties) email
    WHERE _TABLE_SUFFIX > last_update_date
      AND email.key = 'guid_email'
      AND email.value.string_value IS NOT NULL
    GROUP BY 1, 2
  ) result
  WHERE result.hashed_email = identity_match.hashed_email
    AND result.ga_id = identity_match.ga_id;

  -- Insert new identity_match records
  INSERT INTO `${project_id}.${dataset_id}.identity_match`(
    id,
    hashed_email,
    ga_id,
    created_date,
    updated_date
  )
  SELECT
    GENERATE_UUID() AS id,
    email.value.string_value AS hashed_email,
    user_pseudo_id AS ga_id,
    CURRENT_DATETIME() AS created_date,
    ARRAY<DATETIME>[PARSE_DATETIME("%Y%m%d", MAX(event_date))] AS updated_date
  FROM
    `${ga4_project}.${ga4_dataset}.events_*`,
    UNNEST(user_properties) email
  LEFT JOIN
    `${project_id}.${dataset_id}.identity_match` AS identity_match
  ON
    email.value.string_value = identity_match.hashed_email
    AND user_pseudo_id = identity_match.ga_id
  WHERE
    _TABLE_SUFFIX > last_update_date
    AND email.key = 'guid_email'
    AND email.value.string_value IS NOT NULL
    AND identity_match.id IS NULL
  GROUP BY 2, 3;

  -- Update existing alternate_identity_match records
  -- Take the MAX alternate ID value for each email/type to ensure one row per combination
  UPDATE `${project_id}.${dataset_id}.alternate_identity_match` alternate_identity_match
  SET 
    alternate_identity_match.updated_date = ARRAY_CONCAT(alternate_identity_match.updated_date, [PARSE_DATETIME("%Y%m%d", result.latest_date)]),
    alternate_identity_match.alternate_id = ARRAY_CONCAT(alternate_identity_match.alternate_id, result.alternate_id)
  FROM (
    SELECT
      hashed_email,
      ARRAY<STRING>[MAX(alternate_value)] AS alternate_id,
      alternate_id_type,
      MAX(latest_date) as latest_date
    FROM (
      SELECT DISTINCT
        email.value.string_value AS hashed_email,
        alt.value.string_value AS alternate_value,
        REPLACE(alt.key, 'guid_', '') AS alternate_id_type,
        event_date as latest_date
      FROM `${ga4_project}.${ga4_dataset}.events_*`,
        UNNEST(user_properties) email,
        UNNEST(user_properties) alt
      WHERE _TABLE_SUFFIX > last_update_date
        AND email.key = 'guid_email'
        AND alt.key IN ('guid_floodlight_id', 'guid_gads_id', 'guid_floodlight_gads_id', 
                        'guid_fb_id', 'guid_tiktok_id', 'guid_reddit_id', 'guid_rws_id')
        AND email.value.string_value IS NOT NULL
        AND alt.value.string_value IS NOT NULL
    )
    GROUP BY hashed_email, alternate_id_type
  ) as result
  WHERE result.hashed_email = alternate_identity_match.hashed_email
    AND result.alternate_id_type = alternate_identity_match.alternate_id_type
    AND result.alternate_id[SAFE_OFFSET(0)] != ARRAY_REVERSE(alternate_identity_match.alternate_id)[SAFE_OFFSET(0)];

  -- Insert new alternate_identity_match records
  INSERT INTO `${project_id}.${dataset_id}.alternate_identity_match`(
    hashed_email,
    alternate_id,
    alternate_id_type,
    updated_date
  )
  SELECT
    email.value.string_value AS hashed_email,
    ARRAY<STRING>[alt.value.string_value] AS alternate_id,
    REPLACE(alt.key, 'guid_', '') AS alternate_id_type,
    ARRAY<DATETIME>[PARSE_DATETIME("%Y%m%d", MAX(event_date))] AS updated_date
  FROM `${ga4_project}.${ga4_dataset}.events_*`,
    UNNEST(user_properties) email,
    UNNEST(user_properties) alt
  LEFT JOIN
    `${project_id}.${dataset_id}.alternate_identity_match` AS alternate_identity_match
  ON
    email.value.string_value = alternate_identity_match.hashed_email
    AND REPLACE(alt.key, 'guid_', '') = alternate_identity_match.alternate_id_type
  WHERE _TABLE_SUFFIX > last_update_date
    AND email.key = 'guid_email'
    AND alt.key IN ('guid_floodlight_id', 'guid_gads_id', 'guid_floodlight_gads_id',
                    'guid_fb_id', 'guid_tiktok_id', 'guid_reddit_id', 'guid_rws_id')
    AND email.value.string_value IS NOT NULL
    AND alt.value.string_value IS NOT NULL
    AND alternate_identity_match.hashed_email IS NULL
  GROUP BY email.value.string_value, alt.value.string_value, alt.key;

END;