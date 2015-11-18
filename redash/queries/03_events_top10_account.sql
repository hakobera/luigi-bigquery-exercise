/* GitHub timeline top10 account (Today) */
SELECT
  actor.login AS github_id,
  COUNT(*) AS cnt
FROM
  TABLE_DATE_RANGE(
    githubarchive:day.events_,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
  )
GROUP BY
  github_id
ORDER BY
  cnt DESC
LIMIT
  10
