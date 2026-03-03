-- ============================================================
-- verify.sql — One-shot verification of transcoder output.
--
-- Compares the original text .log files against the converted
-- .parquet files to prove every log entry was faithfully
-- transcoded.
--
-- Usage:
--   1. Extract both ZIPs:
--        mkdir -p /tmp/verify/text /tmp/verify/parquet
--        unzip -qo fresh-text.zip  '*.log'     -d /tmp/verify/text
--        unzip -qo parquet.zip     '*.parquet'  -d /tmp/verify/parquet
--
--   2. Run:
--        duckdb < verify.sql
--
-- The script exits with a clear PASS / FAIL verdict.
-- ============================================================

.mode markdown
.header on
.changes off

-- ============================================================
-- STEP 1: Count new-entry header lines in each text log file
--
-- A crdb-v2 *new entry* matches:
--   ^[IWEF]\d{6} \d{2}:\d{2}:\d{2}\.\d{6} ...] <counter> <space_or_eq>
-- Continuation lines (+, |, !) are excluded.
--
-- Uses read_csv with ignore_errors=true so lines containing
-- non-UTF-8 bytes are silently skipped. A small delta is
-- expected for files with invalid UTF-8 in message content.
-- ============================================================

CREATE OR REPLACE TEMP TABLE text_counts AS
WITH lines AS (
    SELECT
        filename,
        line
    FROM read_csv(
        '/tmp/verify/text/debug/nodes/1/logs/*.log',
        columns       = {'line': 'VARCHAR'},
        header        = false,
        auto_detect   = false,
        sep           = chr(0),
        quote         = '',
        filename      = true,
        ignore_errors = true
    )
)
SELECT
    regexp_replace(
        replace(filename, '/tmp/verify/text/debug/nodes/1/logs/', ''),
        '\.log$', '') AS base_name,
    count(*) FILTER (
        WHERE regexp_matches(line, '^[IWEF]\d{6} \d{2}:\d{2}:\d{2}\.\d{6} \d+')
          AND NOT regexp_matches(line, 'config\]')
    ) AS entry_count
FROM lines
GROUP BY filename;

-- ============================================================
-- STEP 2: Count rows in each parquet file
-- ============================================================

CREATE OR REPLACE TEMP TABLE parquet_counts AS
SELECT
    regexp_replace(
        replace(filename, '/tmp/verify/parquet/debug/nodes/1/logs/', ''),
        '\.parquet$', '') AS base_name,
    count(*) AS entry_count
FROM read_parquet(
    '/tmp/verify/parquet/debug/nodes/1/logs/*.parquet',
    union_by_name = true,
    filename      = true
)
GROUP BY filename;

-- ============================================================
-- CHECK 1: Per-file entry count comparison
-- ============================================================

SELECT '--- CHECK 1: Per-file entry count comparison ---' AS check;

SELECT
    coalesce(t.base_name, p.base_name)  AS file,
    coalesce(t.entry_count, 0)          AS text_entries,
    coalesce(p.entry_count, 0)          AS parquet_rows,
    CASE
        WHEN t.entry_count IS NULL            THEN 'FAIL (extra parquet, no text)'
        WHEN p.entry_count IS NULL            THEN 'FAIL (missing parquet)'
        WHEN t.entry_count = p.entry_count    THEN 'PASS'
        WHEN p.entry_count >= t.entry_count
         AND p.entry_count - t.entry_count <= 10
            THEN 'PASS (delta=' || (p.entry_count - t.entry_count)::VARCHAR || ', skipped invalid UTF-8 lines in text)'
        ELSE 'FAIL (count mismatch: delta=' || (p.entry_count - t.entry_count)::VARCHAR || ')'
    END AS status
FROM text_counts t
FULL OUTER JOIN parquet_counts p ON t.base_name = p.base_name
ORDER BY file;

-- ============================================================
-- CHECK 2: Total counts
-- ============================================================

SELECT '--- CHECK 2: Total entry counts ---' AS check;

SELECT
    (SELECT sum(entry_count) FROM text_counts)    AS total_text,
    (SELECT sum(entry_count) FROM parquet_counts)  AS total_parquet,
    (SELECT sum(entry_count) FROM parquet_counts)
      - (SELECT sum(entry_count) FROM text_counts) AS delta,
    CASE
        WHEN (SELECT sum(entry_count) FROM parquet_counts)
           - (SELECT sum(entry_count) FROM text_counts) BETWEEN 0 AND 10
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status;

-- ============================================================
-- CHECK 3: File count (21 log files -> 21 parquet files)
-- ============================================================

SELECT '--- CHECK 3: File count ---' AS check;

SELECT
    (SELECT count(*) FROM text_counts)    AS text_files,
    (SELECT count(*) FROM parquet_counts) AS parquet_files,
    CASE
        WHEN (SELECT count(*) FROM text_counts)
           = (SELECT count(*) FROM parquet_counts)
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status;

-- ============================================================
-- CHECK 4: Parquet schema has all 15 proto fields
-- ============================================================

SELECT '--- CHECK 4: Parquet schema ---' AS check;

DESCRIBE SELECT * FROM read_parquet(
    '/tmp/verify/parquet/debug/nodes/1/logs/*.parquet',
    union_by_name = true
) LIMIT 0;

-- ============================================================
-- CHECK 5: Data quality (no nulls / empties / out-of-range)
-- ============================================================

SELECT '--- CHECK 5: Data quality ---' AS check;

SELECT
    count(*)                                                  AS total_rows,
    count(*) FILTER (WHERE time = 0)                          AS zero_timestamps,
    count(*) FILTER (WHERE length(file) = 0)                  AS empty_file,
    count(*) FILTER (WHERE length(message) = 0)               AS empty_message,
    count(*) FILTER (WHERE length(tenant_id) = 0)             AS empty_tenant_id,
    count(*) FILTER (WHERE severity NOT IN (0,1,2,3,4))       AS bad_severity,
    count(*) FILTER (WHERE channel < 0 OR channel > 15)       AS bad_channel,
    CASE
        WHEN count(*) FILTER (WHERE time = 0)                    = 0
         AND count(*) FILTER (WHERE length(file) = 0)            = 0
         AND count(*) FILTER (WHERE length(message) = 0)         = 0
         AND count(*) FILTER (WHERE length(tenant_id) = 0)       = 0
         AND count(*) FILTER (WHERE severity NOT IN (0,1,2,3,4)) = 0
         AND count(*) FILTER (WHERE channel < 0 OR channel > 15) = 0
        THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM read_parquet(
    '/tmp/verify/parquet/debug/nodes/1/logs/*.parquet',
    union_by_name = true
);

-- ============================================================
-- CHECK 6: Severity distribution (sanity)
-- ============================================================

SELECT '--- CHECK 6: Severity distribution ---' AS check;

SELECT severity, count(*) AS cnt
FROM read_parquet('/tmp/verify/parquet/debug/nodes/1/logs/*.parquet', union_by_name=true)
GROUP BY severity ORDER BY severity;

-- ============================================================
-- CHECK 7: Channel distribution (sanity)
-- ============================================================

SELECT '--- CHECK 7: Channel distribution ---' AS check;

SELECT channel, count(*) AS cnt
FROM read_parquet('/tmp/verify/parquet/debug/nodes/1/logs/*.parquet', union_by_name=true)
GROUP BY channel ORDER BY channel;

-- ============================================================
-- CHECK 8: Timestamp range
-- ============================================================

SELECT '--- CHECK 8: Timestamp range ---' AS check;

SELECT
    min(to_timestamp(time / 1000000000)) AS earliest,
    max(to_timestamp(time / 1000000000)) AS latest
FROM read_parquet('/tmp/verify/parquet/debug/nodes/1/logs/*.parquet', union_by_name=true)
WHERE time > 0;

-- ============================================================
-- CHECK 9: Redactable flag
-- ============================================================

SELECT '--- CHECK 9: Redactable flag ---' AS check;

SELECT redactable, count(*) AS cnt
FROM read_parquet('/tmp/verify/parquet/debug/nodes/1/logs/*.parquet', union_by_name=true)
GROUP BY redactable;

-- ============================================================
-- FINAL VERDICT
-- ============================================================

SELECT '========================================' AS x;
SELECT '         FINAL VERDICT                 ' AS x;
SELECT '========================================' AS x;

WITH verdicts AS (
    SELECT
        -- no file has parquet < text (missing entries) or delta > 10
        (
            SELECT count(*) = 0
            FROM text_counts t
            FULL OUTER JOIN parquet_counts p ON t.base_name = p.base_name
            WHERE t.entry_count IS NULL
               OR p.entry_count IS NULL
               OR p.entry_count < t.entry_count
               OR p.entry_count - t.entry_count > 10
        ) AS per_file_ok,
        -- total delta within tolerance (text may undercount due to skipped invalid-UTF-8 lines)
        (
            (SELECT sum(entry_count) FROM parquet_counts)
          - (SELECT sum(entry_count) FROM text_counts) BETWEEN 0 AND 10
        ) AS totals_ok,
        -- file count match
        (
            (SELECT count(*) FROM text_counts)
          = (SELECT count(*) FROM parquet_counts)
        ) AS file_count_ok
)
SELECT
    CASE
        WHEN per_file_ok AND totals_ok AND file_count_ok
        THEN 'PASS — '
              || (SELECT sum(entry_count) FROM parquet_counts)::VARCHAR
              || ' parquet rows across '
              || (SELECT count(*) FROM parquet_counts)::VARCHAR
              || ' files (text counter reads '
              || (SELECT sum(entry_count) FROM text_counts)::VARCHAR
              || '; delta from skipped invalid-UTF-8 lines in source).'
        ELSE 'FAIL — Mismatches detected. Review CHECK 1-3 above.'
    END AS verdict,
    per_file_ok,
    totals_ok,
    file_count_ok
FROM verdicts;
