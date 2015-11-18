/* GitHub timeline count per day (last 7 days) */
SELECT
  STRFTIME_UTC_USEC(created_at, "%Y-%m-%d") AS day,
  COUNT(*) AS cnt
FROM
  TABLE_DATE_RANGE(
    githubarchive:day.events_,
    DATE_ADD(CURRENT_TIMESTAMP(), -6, 'DAY'),
    CURRENT_TIMESTAMP()
  )
GROUP BY
  day
ORDER BY
  day
