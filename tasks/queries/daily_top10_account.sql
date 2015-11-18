SELECT
  '{{ task.day }}' AS day,
  actor.login AS github_id,
  COUNT(*) AS cnt
FROM
  githubarchive:day.events_{{ task.day.strftime("%Y%m%d") }}
GROUP BY
  day,
  github_id
ORDER BY
  cnt DESC
LIMIT
  10
