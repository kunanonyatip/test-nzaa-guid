CREATE OR REPLACE PROCEDURE `${project_id}.${dataset_id}.update_identity_match`(
  IN latest_update_date STRING
)
BEGIN
  -- If no date provided, get the last processed date
  IF latest_update_date IS NULL THEN
    SET latest_update_date = (
      SELECT IFNULL(
        FORMAT_DATETIME("%Y%m%d", MAX(updated_date[OFFSET(ARRAY_LENGTH(updated_date) - 1)])),
        "20250602"  -- Default to a past date if no records exist
      )
      FROM `${project_id}.${dataset_id}.identity_match`
    );
  END IF;

  -- Update existing identity_match records
  UPDATE `${project_id}.${dataset_id}.identity_match` im
  SET im.updated_date = ARRAY_CONCAT(im.updated_date, [PARSE_DATETIME("%Y%m%d", result.latest_date)])
  FROM (
    SELECT 
      email.value.string_value as hashed_email,
      user_pseudo_id as ga_id,
      MAX(event_date) as latest_date
    FROM `${ga4_project}.${ga4_dataset}.events_*`,
    UNNEST(user_properties) email
    WHERE _TABLE_SUFFIX > latest_update_date
      AND email.key = 'guid_email'
      AND email.value.string_value IS NOT NULL
    GROUP BY 1, 2
  ) result
  WHERE result.hashed_email = im.hashed_email
    AND result.ga_id = im.ga_id;

  -- Insert new identity_match records
  INSERT INTO `${project_id}.${dataset_id}.identity_match`(
    id, hashed_email, ga_id, created_date, updated_date
  )
  SELECT
    GENERATE_UUID() AS id,
    email.value.string_value AS hashed_email,
    user_pseudo_id AS ga_id,
    CURRENT_DATETIME() AS created_date,
    ARRAY<DATETIME>[PARSE_DATETIME("%Y%m%d", MAX(event_date))] AS updated_date
  FROM `${ga4_project}.${ga4_dataset}.events_*`,
  UNNEST(user_properties) email
  LEFT JOIN `${project_id}.${dataset_id}.identity_match` im
    ON email.value.string_value = im.hashed_email
    AND user_pseudo_id = im.ga_id
  WHERE _TABLE_SUFFIX > latest_update_date
    AND email.key = 'guid_email'
    AND email.value.string_value IS NOT NULL
    AND im.id IS NULL
  GROUP BY 2, 3;

  -- Update existing alternate_identity_match records
  UPDATE `${project_id}.${dataset_id}.alternate_identity_match` aim
  SET 
    aim.updated_date = ARRAY_CONCAT(aim.updated_date, [PARSE_DATETIME("%Y%m%d", result.latest_date)]),
    aim.alternate_id = ARRAY_CONCAT(aim.alternate_id, result.alternate_id)
  FROM (
    SELECT
      email.value.string_value AS hashed_email,
      ARRAY<STRING>[alt.value.string_value] AS alternate_id,
      REPLACE(alt.key, 'guid_', '') AS alternate_id_type,
      MAX(event_date) as latest_date
    FROM `${ga4_project}.${ga4_dataset}.events_*`,
    UNNEST(user_properties) email,
    UNNEST(user_properties) alt
    WHERE _TABLE_SUFFIX > latest_update_date
      AND email.key = 'guid_email'
      AND alt.key IN ('guid_floodlight_id', 'guid_gads_id', 'guid_floodlight_gads_id', 
                      'guid_fb_id', 'guid_tiktok_id', 'guid_reddit_id','guid_rws_id')
      AND email.value.string_value IS NOT NULL
      AND alt.value.string_value IS NOT NULL
    GROUP BY 1, 2, 3
  ) result
  WHERE result.hashed_email = aim.hashed_email
    AND result.alternate_id_type = aim.alternate_id_type
    AND result.alternate_id[SAFE_OFFSET(0)] != ARRAY_REVERSE(aim.alternate_id)[SAFE_OFFSET(0)];

  -- Insert new alternate_identity_match records
  INSERT INTO `${project_id}.${dataset_id}.alternate_identity_match`(
    hashed_email, alternate_id, alternate_id_type, updated_date
  )
  SELECT
    email.value.string_value AS hashed_email,
    ARRAY<STRING>[alt.value.string_value] AS alternate_id,
    REPLACE(alt.key, 'guid_', '') AS alternate_id_type,
    ARRAY<DATETIME>[PARSE_DATETIME("%Y%m%d", MAX(event_date))] AS updated_date
  FROM `${ga4_project}.${ga4_dataset}.events_*`,
  UNNEST(user_properties) email,
  UNNEST(user_properties) alt
  LEFT JOIN `${project_id}.${dataset_id}.alternate_identity_match` aim
    ON email.value.string_value = aim.hashed_email
    AND REPLACE(alt.key, 'guid_', '') = aim.alternate_id_type
  WHERE _TABLE_SUFFIX > latest_update_date
    AND email.key = 'guid_email'
    AND alt.key IN ('guid_floodlight_id', 'guid_gads_id', 'guid_floodlight_gads_id',
                    'guid_fb_id', 'guid_tiktok_id', 'guid_reddit_id','guid_rws_id')
    AND email.value.string_value IS NOT NULL
    AND alt.value.string_value IS NOT NULL
    AND aim.hashed_email IS NULL
  GROUP BY 1, 2, 3;

END;
-- End of procedure
-- This procedure updates the identity_match and alternate_identity_match tables
-- based on the latest data from GA4 events, ensuring that both existing records are updated
-- and new records are inserted as necessary.
-- The procedure takes an optional parameter for the latest update date,
-- defaulting to the most recent date in the identity_match table if not provided.
-- It handles hashed emails and various alternate IDs, ensuring that the data remains consistent
-- and up-to-date across both tables.
-- The procedure also ensures that duplicate entries are avoided by checking existing records
-- before inserting new ones.