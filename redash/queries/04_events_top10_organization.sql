/* GitHub timeline top10 organization (Today) */
SELECT
  org.login AS github_id,
  COUNT(*) AS cnt
FROM
  TABLE_DATE_RANGE(
    githubarchive:day.events_,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
  )
WHERE
  org.id IS NOT NULL
GROUP BY
  github_id
ORDER BY
  cnt DESC
LIMIT
  10
