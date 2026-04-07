# global_dynamics

## get_report_data
```sql
WITH mobile_top AS (
  SELECT
    country_region,
    game_id,
    COALESCE(game_name, '') AS game_name,
    rank,
    rank_change
  FROM intelligence.game_change_report_top_mobile
  WHERE 1 = 1
  AND `date` = @ReportDate
), mobile_anomaly AS (
  SELECT
    country_region,
    game_id,
    COALESCE(game_name, '') AS game_name,
    COALESCE(change_type, '') AS change_type,
    COALESCE(change_reason, '') AS change_reason,
    COALESCE(summary_zh, '') AS summary_zh,
    COALESCE(summary_en, '') AS summary_en
  FROM intelligence.game_change_report_anomaly_mobile
  WHERE 1 = 1
  AND `date` = @ReportDate
), mobile_regions AS (
  SELECT country_region
  FROM mobile_top
  UNION DISTINCT
  SELECT country_region
  FROM mobile_anomaly
), mobile_game_ids AS (
  SELECT game_id
  FROM mobile_top
  UNION DISTINCT
  SELECT game_id
  FROM mobile_anomaly
), mobile_app_detail_base AS (
  SELECT
    a.app_id AS game_id,
    COALESCE(a.entity_name, '') AS game_name,
    COALESCE(a.cover, '') AS cover,
    COALESCE(a.iegg_genre, '') AS genre,
    COALESCE(a.iegg_sub_genre, '') AS sub_genre,
    COALESCE(SPLIT(COALESCE(a.publisher, ''), '|')[SAFE_OFFSET(0)], '') AS publisher,
    SPLIT(COALESCE(a.publisher_id, ''), '|')[SAFE_OFFSET(0)] AS company_id
  FROM mobile_game_ids ids
  JOIN common.app_detail a
    ON a.app_id = ids.game_id
   AND a.entity_type = 'mobile'
   AND a.id_type = 'app_id'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.app_id ORDER BY a.update_time DESC, a.create_time DESC) = 1
), mobile_company_ids AS (
  SELECT DISTINCT company_id
  FROM mobile_app_detail_base
  WHERE company_id IS NOT NULL
  AND company_id != ''
), mobile_company_detail AS (
  SELECT
    uuid AS company_id,
    COALESCE(location_identifiers_country, '') AS publisher_country_abbre
  FROM common.company_details
  WHERE uuid IN (SELECT company_id FROM mobile_company_ids)
), mobile_game_detail AS (
  SELECT
    b.game_id AS game_id,
    b.game_name AS game_name,
    b.cover AS cover,
    b.genre AS genre,
    b.sub_genre AS sub_genre,
    b.publisher AS publisher,
    COALESCE(c.publisher_country_abbre, '') AS publisher_country_abbre
  FROM mobile_app_detail_base b
  LEFT JOIN mobile_company_detail c
    ON b.company_id = c.company_id
), mobile_top_grouped AS (
  SELECT
    country_region,
    ARRAY_AGG(
      STRUCT(
        game_id AS game_id,
        COALESCE(NULLIF(mobile_top.game_name, ''), gd.game_name, '') AS game_name,
        COALESCE(gd.cover, '') AS cover,
        COALESCE(gd.genre, '') AS genre,
        COALESCE(gd.sub_genre, '') AS sub_genre,
        COALESCE(gd.publisher, '') AS publisher,
        COALESCE(gd.publisher_country_abbre, '') AS publisher_country_abbre,
        rank AS appstore__rank,
        rank_change AS appstore__rank_growth
      )
      ORDER BY rank ASC
    ) AS top_list
  FROM mobile_top
  LEFT JOIN mobile_game_detail gd
    USING (game_id)
  GROUP BY country_region
), mobile_anomaly_grouped AS (
  SELECT
    country_region,
    ARRAY_AGG(
      STRUCT(
        game_id AS game_id,
        COALESCE(NULLIF(mobile_anomaly.game_name, ''), gd.game_name, '') AS game_name,
        COALESCE(gd.cover, '') AS cover,
        COALESCE(gd.genre, '') AS genre,
        COALESCE(gd.sub_genre, '') AS sub_genre,
        COALESCE(gd.publisher, '') AS publisher,
        COALESCE(gd.publisher_country_abbre, '') AS publisher_country_abbre,
        change_type AS change_type,
        change_reason AS change_reason,
        summary_zh AS summary_zh,
        summary_en AS summary_en
      )
      ORDER BY game_id ASC
    ) AS anomalies
  FROM mobile_anomaly
  LEFT JOIN mobile_game_detail gd
    USING (game_id)
  GROUP BY country_region
), steam_top AS (
  SELECT
    game_id,
    COALESCE(game_name, '') AS game_name,
    pcu_rank,
    peak_pcu,
    pcu_change,
    rank_change,
    COALESCE(CAST(release_time AS STRING), '') AS release_time
  FROM intelligence.game_change_report_top_steam
  WHERE 1 = 1
  AND `date` = @ReportDate
), steam_anomaly AS (
  SELECT
    country_region,
    game_id,
    COALESCE(game_name, '') AS game_name,
    COALESCE(change_type, '') AS change_type,
    COALESCE(change_reason, '') AS change_reason,
    COALESCE(summary_zh, '') AS summary_zh,
    COALESCE(summary_en, '') AS summary_en
  FROM intelligence.game_change_report_anomaly_steam
  WHERE 1 = 1
  AND `date` = @ReportDate
), roblox_top AS (
  SELECT
    game_id,
    COALESCE(game_name, '') AS game_name,
    ccu_rank,
    peak_ccu,
    prev_ccu,
    ccu_change,
    COALESCE(change_type, '') AS change_type
  FROM intelligence.game_change_report_top_roblox
  WHERE 1 = 1
  AND `date` = @ReportDate
), roblox_anomaly AS (
  SELECT
    a.game_id,
    COALESCE(t.game_name, '') AS game_name,
    COALESCE(a.change_type, '') AS change_type
  FROM intelligence.game_change_report_anomaly_roblox a
  LEFT JOIN intelligence.game_change_report_top_roblox t
    ON a.`date` = t.`date`
   AND a.game_id = t.game_id
  WHERE 1 = 1
  AND a.`date` = @ReportDate
), steam_game_ids AS (
  SELECT game_id
  FROM steam_top
  UNION DISTINCT
  SELECT game_id
  FROM steam_anomaly
), steam_app_detail_base AS (
  SELECT
    a.steam_id AS game_id,
    a.app_id AS app_id,
    COALESCE(a.entity_name, '') AS game_name,
    COALESCE(a.cover, '') AS cover,
    COALESCE(a.iegg_genre, '') AS genre,
    COALESCE(a.iegg_sub_genre, '') AS sub_genre
  FROM steam_game_ids ids
  JOIN common.app_detail a
    ON a.steam_id = ids.game_id
  QUALIFY ROW_NUMBER() OVER (PARTITION BY a.steam_id ORDER BY a.update_time DESC, a.create_time DESC) = 1
), steam_publisher_filter AS (
  SELECT
    unified_id AS app_id,
    ext1 AS publisher_country_abbre,
    ext2 AS publisher
  FROM common.app_detail_filter
  WHERE data_type = 'publisher_flags'
  AND unified_id IN (SELECT app_id FROM steam_app_detail_base)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY unified_id ORDER BY create_time DESC, data_value DESC, ext1 DESC) = 1
), steam_game_detail AS (
  SELECT
    b.game_id AS game_id,
    b.game_name AS game_name,
    b.cover AS cover,
    b.genre AS genre,
    b.sub_genre AS sub_genre,
    COALESCE(f.publisher, '') AS publisher,
    COALESCE(f.publisher_country_abbre, '') AS publisher_country_abbre
  FROM steam_app_detail_base b
  LEFT JOIN steam_publisher_filter f
    ON b.app_id = f.app_id
), steam_top_grouped AS (
  SELECT
    ARRAY_AGG(
      STRUCT(
        game_id AS game_id,
        COALESCE(NULLIF(steam_top.game_name, ''), gd.game_name, '') AS game_name,
        COALESCE(gd.cover, '') AS cover,
        COALESCE(gd.genre, '') AS genre,
        COALESCE(gd.sub_genre, '') AS sub_genre,
        COALESCE(gd.publisher, '') AS publisher,
        COALESCE(gd.publisher_country_abbre, '') AS publisher_country_abbre,
        pcu_rank AS steam__pcu_rank,
        peak_pcu AS steam__peak_pcu,
        pcu_change AS steam__peak_pcu_growth,
        rank_change AS steam__pcu_rank_growth,
        release_time AS release_time
      )
      ORDER BY pcu_rank ASC
    ) AS top_list
  FROM steam_top
  LEFT JOIN steam_game_detail gd
    USING (game_id)
), steam_anomaly_grouped AS (
  SELECT
    ARRAY_AGG(
      STRUCT(
        game_id AS game_id,
        COALESCE(NULLIF(steam_anomaly.game_name, ''), gd.game_name, '') AS game_name,
        COALESCE(gd.cover, '') AS cover,
        COALESCE(gd.genre, '') AS genre,
        COALESCE(gd.sub_genre, '') AS sub_genre,
        COALESCE(gd.publisher, '') AS publisher,
        COALESCE(gd.publisher_country_abbre, '') AS publisher_country_abbre,
        country_region AS country_region,
        change_type AS change_type,
        change_reason AS change_reason,
        summary_zh AS summary_zh,
        summary_en AS summary_en
      )
      ORDER BY country_region ASC, game_id ASC
    ) AS anomalies
  FROM steam_anomaly
  LEFT JOIN steam_game_detail gd
    USING (game_id)
), roblox_game_ids AS (
  SELECT game_id
  FROM roblox_top
  UNION DISTINCT
  SELECT game_id
  FROM roblox_anomaly
), roblox_game_detail AS (
  SELECT
    entity_id AS game_id,
    COALESCE(entity_name, '') AS game_name,
    COALESCE(cover, '') AS cover,
    COALESCE(main_genre, '') AS genre,
    COALESCE(sub_genre, '') AS sub_genre,
    COALESCE(publisher, '') AS publisher,
    '' AS publisher_country_abbre
  FROM roblox_game_ids ids
  JOIN roblox.roblox_game_detail d
    ON d.entity_id = ids.game_id
  QUALIFY ROW_NUMBER() OVER (PARTITION BY d.entity_id ORDER BY d.update_time DESC, d.create_time DESC) = 1
), roblox_top_grouped AS (
  SELECT
    ARRAY_AGG(
      STRUCT(
        game_id AS game_id,
        COALESCE(NULLIF(roblox_top.game_name, ''), gd.game_name, '') AS game_name,
        COALESCE(gd.cover, '') AS cover,
        COALESCE(gd.genre, '') AS genre,
        COALESCE(gd.sub_genre, '') AS sub_genre,
        COALESCE(gd.publisher, '') AS publisher,
        COALESCE(gd.publisher_country_abbre, '') AS publisher_country_abbre,
        ARRAY<STRING>[] AS gameplay_tags,
        ARRAY<STRING>[] AS mechanics_tags,
        ccu_rank AS roblox__ccu_rank,
        peak_ccu AS roblox__peak_ccu,
        prev_ccu AS roblox__prev_peak_ccu,
        ccu_change AS roblox__peak_ccu_growth,
        change_type AS change_type
      )
      ORDER BY ccu_rank ASC
    ) AS top_list
  FROM roblox_top
  LEFT JOIN roblox_game_detail gd
    USING (game_id)
), roblox_anomaly_grouped AS (
  SELECT
    ARRAY_AGG(
      STRUCT(
        game_id AS game_id,
        COALESCE(NULLIF(roblox_anomaly.game_name, ''), gd.game_name, '') AS game_name,
        COALESCE(gd.cover, '') AS cover,
        COALESCE(gd.genre, '') AS genre,
        COALESCE(gd.sub_genre, '') AS sub_genre,
        COALESCE(gd.publisher, '') AS publisher,
        COALESCE(gd.publisher_country_abbre, '') AS publisher_country_abbre,
        change_type AS change_type
      )
      ORDER BY game_id ASC
    ) AS anomalies
  FROM roblox_anomaly
  LEFT JOIN roblox_game_detail gd
    USING (game_id)
)
SELECT
  TO_JSON_STRING(ARRAY(
    SELECT AS STRUCT
      mr.country_region AS country_region,
      IFNULL(mtg.top_list, ARRAY<STRUCT<
        game_id STRING,
        game_name STRING,
        cover STRING,
        genre STRING,
        sub_genre STRING,
        publisher STRING,
        publisher_country_abbre STRING,
        appstore__rank INT64,
        appstore__rank_growth INT64
      >>[]) AS top_list,
      IFNULL(mag.anomalies, ARRAY<STRUCT<
        game_id STRING,
        game_name STRING,
        cover STRING,
        genre STRING,
        sub_genre STRING,
        publisher STRING,
        publisher_country_abbre STRING,
        change_type STRING,
        change_reason STRING,
        summary_zh STRING,
        summary_en STRING
      >>[]) AS anomalies
    FROM mobile_regions mr
    LEFT JOIN mobile_top_grouped mtg
      ON mr.country_region = mtg.country_region
    LEFT JOIN mobile_anomaly_grouped mag
      ON mr.country_region = mag.country_region
    ORDER BY CASE mr.country_region WHEN '国区' THEN 1 WHEN '美区' THEN 2 WHEN '日区' THEN 3 WHEN '韩区' THEN 4 ELSE 99 END
  )) AS mobile_json,
  TO_JSON_STRING(STRUCT(
    IFNULL(stg.top_list, ARRAY<STRUCT<
      game_id STRING,
      game_name STRING,
      cover STRING,
      genre STRING,
      sub_genre STRING,
      publisher STRING,
      publisher_country_abbre STRING,
      steam__pcu_rank INT64,
      steam__peak_pcu INT64,
      steam__peak_pcu_growth INT64,
      steam__pcu_rank_growth INT64,
      release_time STRING
    >>[]) AS top_list,
    IFNULL(sag.anomalies, ARRAY<STRUCT<
      game_id STRING,
      game_name STRING,
      cover STRING,
      genre STRING,
      sub_genre STRING,
      publisher STRING,
      publisher_country_abbre STRING,
      country_region STRING,
      change_type STRING,
      change_reason STRING,
      summary_zh STRING,
      summary_en STRING
    >>[]) AS anomalies
  )) AS steam_json,
  TO_JSON_STRING(STRUCT(
    IFNULL(rtg.top_list, ARRAY<STRUCT<
      game_id STRING,
      game_name STRING,
      cover STRING,
      genre STRING,
      sub_genre STRING,
      publisher STRING,
      publisher_country_abbre STRING,
      gameplay_tags ARRAY<STRING>,
      mechanics_tags ARRAY<STRING>,
      roblox__ccu_rank INT64,
      roblox__peak_ccu INT64,
      roblox__prev_peak_ccu INT64,
      roblox__peak_ccu_growth INT64,
      change_type STRING
    >>[]) AS top_list,
    IFNULL(rag.anomalies, ARRAY<STRUCT<
      game_id STRING,
      game_name STRING,
      cover STRING,
      genre STRING,
      sub_genre STRING,
      publisher STRING,
      publisher_country_abbre STRING,
      change_type STRING
    >>[]) AS anomalies
  )) AS roblox_json
FROM steam_top_grouped stg
CROSS JOIN steam_anomaly_grouped sag
CROSS JOIN roblox_top_grouped rtg
CROSS JOIN roblox_anomaly_grouped rag
```

## get_all_report_date_real_time
```sql
SELECT
  FORMAT_DATE('%F', report_date) AS report_date
FROM (
  SELECT `date` AS report_date
  FROM intelligence.game_change_report_top_mobile
  UNION DISTINCT
  SELECT `date` AS report_date
  FROM intelligence.game_change_report_top_steam
  UNION DISTINCT
  SELECT `date` AS report_date
  FROM intelligence.game_change_report_top_roblox
)
WHERE 1 = 1
@if StartDate != "" {
  AND report_date >= @StartDate
}
@if EndDate != "" {
  AND report_date <= @EndDate
}
ORDER BY report_date DESC
```
