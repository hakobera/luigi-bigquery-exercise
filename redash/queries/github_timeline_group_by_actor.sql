SELECT
  actor.login AS github_id,
  COUNT(*) AS cnt
FROM
  githubarchive:day.events_20151117
GROUP BY
  github_id
