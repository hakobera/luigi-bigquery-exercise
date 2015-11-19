SELECT
  t1.rank AS rank,
{% for t in task.input()[1:] -%}
  t{{ loop.index }}.github_id AS {{ t.table_id }}{% if not loop.last %},{% endif %}
{% endfor -%}
FROM
  {{ "{0}.{1}".format(task.input()[1].dataset_id, task.input()[1].table_id) }} t1
{% for t in task.input()[2:] -%}
JOIN
  {{ "{0}.{1}".format(t.dataset_id, t.table_id) }} t{{ loop.index + 1 }}
ON
  t1.rank = t{{ loop.index + 1 }}.rank
{% endfor -%}
ORDER BY
  t1.rank
