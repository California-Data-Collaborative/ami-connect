-- Irrigation detection: carry each hour's real moment (time zone included) in the detection table.
--
-- Today WAVELET.GLOBAL_IRRIGATION_DETECTION.TIMESTAMP is TIMESTAMP_NTZ: the clock reading of the hour with its UTC
-- offset dropped. Adapters store readings in different offsets (Pacific for most, UTC for Neptune, Moulton Niguel and
-- Coastside), so a consumer cannot know what that clock means. ami-connect PR #238 works around it by joining back to
-- READINGS; this script removes the need for that: the table gets a FLOWTIME column with the real moment, the
-- procedure fills it, and existing rows are backfilled once.
--
-- Run under the owning role (USE ROLE AMI_CONNECT_ADMIN_ROLE as PRIMARY; see docs/irrigation edit conventions), in this
-- order, before deploying the ami-connect change that reads the column. Nothing here changes TIMESTAMP, the MERGE key,
-- or any detection value.

-- 1. The column. TIMESTAMP stays as the merge key; FLOWTIME is the same hour as an instant.
ALTER TABLE CADC_AMI.WAVELET.GLOBAL_IRRIGATION_DETECTION ADD COLUMN IF NOT EXISTS FLOWTIME TIMESTAMP_TZ;

-- 2. The procedure writes it. Identical to the live body except: FLOWTIME in the select and insert lists, and the
--    detector's month/hour features computed in the agency's local time instead of the stored offset.
CREATE OR REPLACE PROCEDURE CADC_AMI.WAVELET.RUN_ALL_IRRIGATION()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS $$
DECLARE
  rows_inserted INTEGER DEFAULT 0;
BEGIN
    MERGE INTO CADC_AMI.WAVELET.GLOBAL_IRRIGATION_DETECTION t
    USING (
        WITH district_overrides AS (
            SELECT 'cadc_moulton_niguel' AS org, 16.0::FLOAT AS override UNION ALL
            SELECT 'cadc_long_beach',          9.0    UNION ALL
            SELECT 'cadc_thousand_oaks',       11.0   UNION ALL
            SELECT 'cadc_valley_county',       9.5    UNION ALL
            SELECT 'cadc_crescent',            19.0   UNION ALL
            SELECT 'cadc_hills',               22.0   UNION ALL
            SELECT 'cadc_roseville',           25.0   UNION ALL
            SELECT 'cadc_south_tahoe',         21.0
        ),
        watermarks AS (
            SELECT d.org,
                   d.override,
                   COALESCE(g.last_ts,
                            DATEADD('day', -30, CURRENT_TIMESTAMP())::TIMESTAMP_NTZ) AS last_ts
            FROM district_overrides d
            LEFT JOIN (
                SELECT SOURCE, MAX(TIMESTAMP) AS last_ts
                FROM CADC_AMI.WAVELET.GLOBAL_IRRIGATION_DETECTION
                GROUP BY SOURCE
            ) g ON g.SOURCE = d.org
        ),
        src AS (
            SELECT r.ORG_ID, r.DEVICE_ID, r.FLOWTIME, r.INTERVAL_VALUE,
                   -- Seasonal and hour features in the agency's local time, whatever offset the adapter stores
                   MONTH(CONVERT_TIMEZONE('America/Los_Angeles', r.FLOWTIME)) AS month_num,
                   HOUR(CONVERT_TIMEZONE('America/Los_Angeles', r.FLOWTIME))  AS hour_num,
                   w.override        AS district_override
            FROM CADC_AMI.AMI_CONNECT.READINGS r
            JOIN watermarks w ON w.org = r.ORG_ID
            WHERE r.INTERVAL_VALUE IS NOT NULL
              AND r.FLOWTIME >= DATEADD('day', -15, w.last_ts)::TIMESTAMP_TZ
              AND r.FLOWTIME <  LEAST(DATEADD('day', 21, w.last_ts)::TIMESTAMP_TZ,
                                      CURRENT_TIMESTAMP()::TIMESTAMP_TZ)
        )
        SELECT src.ORG_ID                     AS SOURCE,
               res.DEVICE_ID                  AS DEVICE_ID,
               res.TIMESTAMP::TIMESTAMP_NTZ   AS TIMESTAMP,
               res.TIMESTAMP                  AS FLOWTIME,
               res.TOTAL_READING              AS TOTAL_READING,
               res.NON_IRRIGATION_READING     AS NON_IRRIGATION_READING,
               res.IRRIGATION_READING         AS IRRIGATION_READING
        FROM src,
             TABLE(CADC_AMI.WAVELET.DETECT_IRRIGATION_V4(
                     src.DEVICE_ID, src.FLOWTIME, src.INTERVAL_VALUE,
                     src.MONTH_NUM, src.HOUR_NUM, src.DISTRICT_OVERRIDE
                   ) OVER (PARTITION BY src.ORG_ID, src.DEVICE_ID)) res
    ) s
    ON t.SOURCE = s.SOURCE
       AND t.DEVICE_ID = s.DEVICE_ID
       AND t.TIMESTAMP = s.TIMESTAMP
    WHEN NOT MATCHED THEN INSERT
        (SOURCE, DEVICE_ID, TIMESTAMP, FLOWTIME, TOTAL_READING, NON_IRRIGATION_READING, IRRIGATION_READING)
        VALUES
        (s.SOURCE, s.DEVICE_ID, s.TIMESTAMP, s.FLOWTIME, s.TOTAL_READING, s.NON_IRRIGATION_READING, s.IRRIGATION_READING);
    rows_inserted := SQLROWCOUNT;
    RETURN 'irrigation detection complete: ' || rows_inserted || ' new rows inserted';
END;
$$;

-- 3. Backfill existing rows, one agency at a time (LARGE warehouse; Long Beach alone is 1.4B rows). The reading with
--    the same clock time gives the instant; the DST fall-back hour has two such readings, the earlier one is taken.
--    Re-runnable: only rows still NULL are touched.
UPDATE CADC_AMI.WAVELET.GLOBAL_IRRIGATION_DETECTION t
   SET FLOWTIME = s.FLOWTIME
  FROM (
        SELECT g.SOURCE, g.DEVICE_ID, g.TIMESTAMP, r.FLOWTIME
          FROM CADC_AMI.WAVELET.GLOBAL_IRRIGATION_DETECTION g
          JOIN CADC_AMI.AMI_CONNECT.READINGS r
            ON r.ORG_ID = g.SOURCE AND r.DEVICE_ID = g.DEVICE_ID AND r.FLOWTIME::TIMESTAMP_NTZ = g.TIMESTAMP
         WHERE g.SOURCE = '<org_id>' AND g.FLOWTIME IS NULL AND r.ORG_ID = '<org_id>'
        QUALIFY ROW_NUMBER() OVER (PARTITION BY g.SOURCE, g.DEVICE_ID, g.TIMESTAMP ORDER BY r.FLOWTIME) = 1
       ) s
 WHERE t.SOURCE = s.SOURCE AND t.DEVICE_ID = s.DEVICE_ID AND t.TIMESTAMP = s.TIMESTAMP AND t.SOURCE = '<org_id>';

-- 4. Check, per agency: rows still without FLOWTIME are detection rows whose reading no longer exists.
SELECT SOURCE, COUNT(*) AS rows_total, COUNT_IF(FLOWTIME IS NULL) AS rows_without_flowtime,
       COUNT_IF(FLOWTIME IS NOT NULL AND FLOWTIME::TIMESTAMP_NTZ <> TIMESTAMP) AS rows_where_clocks_disagree
  FROM CADC_AMI.WAVELET.GLOBAL_IRRIGATION_DETECTION GROUP BY 1 ORDER BY 1;
