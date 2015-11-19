SELECT
  t1.rank AS rank,
  t1.github_id AS account,
  t2.github_id AS organization
FROM
  {{ task.input_table('account') }} t1
JOIN
  {{ task.input_table('organization') }} t2
ON
  t1.rank = t2.rank
ORDER BY
  t1.rank
