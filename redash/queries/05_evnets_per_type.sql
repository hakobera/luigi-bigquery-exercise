/* GitHub timeline per type (Today) */
SELECT
  type,
  COUNT(*) AS cnt
FROM
  TABLE_DATE_RANGE(
    githubarchive:day.events_,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
  )
GROUP BY
  type
ORDER BY
  cnt DESC
