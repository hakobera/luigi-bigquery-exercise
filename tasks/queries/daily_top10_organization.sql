SELECT
  '{{ task.day }}' AS day,
  RANK() OVER (PARTITION BY day ORDER BY cnt DESC) AS rank,
  github_id,
  cnt
FROM (
  SELECT
    org.login AS github_id,
    COUNT(*) AS cnt
  FROM
    githubarchive:day.events_{{ task.day.strftime("%Y%m%d") }}
  WHERE
    org.id IS NOT NULL
  GROUP BY
    github_id
  ORDER BY
    cnt DESC
  LIMIT
    10
)
