# Luigi と BigQuery

Luigi は公式の [cntrib パッケージに BigQuery モジュール](https://github.com/spotify/luigi/blob/master/luigi/contrib/bigquery.py)があり、なぜか twitter の人がコミットしている。が、自分が Luigi を使い始めた (2015年4月くらい）頃、これがなかったので、自分で Luigi と BigQuery を連携させるための便利モジュールを書いた。GitHub として公開済みである。

https://github.com/hakobera/luigi-bigquery

見る人が見ればわかると思うが、これは Treasure Data の [luigi-td](https://github.com/treasure-data/luigi-td) の設計とコードを大部分流用している。おかげでほぼ1日で組み上がった。

ということで、Quipper における Luigi-BigQuery 連携は公式のものではなく、luigi-bigquery のことだということを覚えておいて欲しい。

## luigi-bigquery 入門

### 認証情報の設定

実行するディレクトリに `client.cfg` という名前のファイルを作り、`project_id`, `service_account`, `private_key_file` を設定する。

```
# configuration for Luigi
[core]
error-email: you@example.com

# configuration for Luigi-BigQuery
[bigquery]
project_id: your_project_id
service_account: your_service_account
private_key_file: /path/to/key.p12
```

### 単純な Query の実行

`luigi_bigquery.Query` クラスを継承した Task クラスを作り、`query` メソッドでクエリを返すようにし、`run` メソッドを以下のように書くと、クエリ結果をコンソールに出力できる。

```python
class MyQueryRun(luigi_bigquery.Query):

    def query(self):
        return "SELECT count(*) cnt FROM [publicdata:samples.github_nested]"

    def run(self):
        result = self.run_query(self.query())
        print "Job ID     :", result.job_id
        print "Result size:", result.size
        print "Result     :"
        print "\t".join([c['name'] for i, c in result.description])
        print "----"
        for row in result:
            print "\t".join([str(c) for c in row])
        print '===================='
```

### CSV 出力

`run_query` の戻り値は、クエリの結果を CSV 出力できるメソッド `to_csv` を用意しているので、クエリの結果を CSV に出力するのは以下のように簡単に書ける。

```python
class MyQuerySave(luigi_bigquery.Query):

    def query(self):
        return "SELECT count(*) cnt FROM [publicdata:samples.github_nested]"

    def output(self):
        return luigi.LocalTarget('MyQuerySave.csv')

    def run(self):
        result = self.run_query(self.query())
        with self.output().open('w') as f:
            result.to_csv(f)
```

### クエリテンプレート機能

多分、これが luigi-bigquery の最も便利な機能で、公式の Contrib にない機能。クエリを jinja2 のテンプレート措定記述することができる。これにより、クエリの再利用性が極端に高まる。

```sql
SELECT
  STRFTIME_UTC_USEC(TIMESTAMP(repository_created_at), '%Y-%m') AS month,
  count(*) cnt
FROM
  [publicdata:samples.github_timeline]
WHERE
  TIMESTAMP(repository_created_at) >= '{{ task.year }}-01-01 00:00:00'
  AND
  TIMESTAMP(repository_created_at) <= '{{ task.year + 1 }}-01-01 00:00:00'
GROUP BY
  month
ORDER BY
  month
```

このように年ごとに集計するテンプレートに対して、外部から変数 `year` を渡すことができると、
複数年のレポートを一気に作成することができるようになる。

```python
class MyQueryWithParameters(luigi_bigquery.Query):
    source = 'templates/query_with_time_range.sql'

    # parameters
    year = luigi.IntParameter()

    def output(self):
        # create a unique name for this output using parameters
        return luigi_bigquery.ResultTarget('MyQueryWithParameters-{0}.job'.format(self.year))

class MyQueryAggregator(luigi.Task):

    def requires(self):
        # create a list of tasks with different parameters
        return [
            MyQueryWithParameters(2009),
            MyQueryWithParameters(2010),
            MyQueryWithParameters(2011),
            MyQueryWithParameters(2012)
        ]

    def output(self):
        return luigi.LocalTarget('MyQueryAggregator.txt')

    def run(self):
        with self.output().open('w') as f:
            # repeat for each ResultTarget
            for target in self.input():
                # output results into a single file
                for row in target.result:
                    f.write(str(row) + "\n")
```

パラメータを定義するにはクラスに `year = luigi.IntParameter()` のように
`パラメータ名 = luigi.<type>Parameter()` のように記述する。ちなみに文字列側のパラメータはよく使うので、
`luigi.StringParameter()` ではなく、単なる `luigi.Parameter()` と記述することに注意。

### テーブル出力機能とクエリパイプライン

クエリの結果をネットワーク越しに引っ張ってくるのは便利だが、結果が多い場合には遅い。
また、あるタスクで集計した結果を、後続のクエリで利用したい場合もある。
これを満たすために、luigi-bigquery はクエリの結果をテーブルに保存する機能を持っている。
また、出力した結果を後続のタスクで利用することもできる。

クエリ結果をテーブルに保存するタスクは、`luigi_bigquery.QueryTable` を継承し、`dataset`, `table`, `query` メソッドを定義する。
このテーブルを後続のタスクで使う場合は、`requires()` の戻り値として指定し、`query` メソッド内で `input` メソッドの戻り値を利用する。

```python
## Building Pipelines using QueryTable

class MyQueryTableStep1(luigi_bigquery.QueryTable):

    def dataset(self):
        return 'tmp'

    def table(self):
        return 'github_nested_count'

    def query(self):
        return "SELECT count(*) cnt FROM [publicdata:samples.github_nested]"

class MyQueryTableStep2(luigi_bigquery.Query):
    def requires(self):
        return MyQueryTableStep1()

    def query(self):
    　　# input()で は requires() の戻り値のタスクの output() の戻り値が取得できる
        input = self.input()    # MyQueryTableStep().output() の結果
        print(input.dataset_id) # => `tmp`
        print(input.table_id)   # => `github_nested_count`
        return "SELECT cnt FROM [{0}.{1}]".format(input.dataset_id, input.table_id)

    def output(self):
        return luigi.LocalTarget('MyQueryTableStep2.csv')

    def run(self):
        # retrieve the result and save it as a CSV file
        result = self.run_query(self.query())
        with self.output().open('w') as f:
            result.to_csv(f)
```

ややこしいので図説すると、

![Alt text](http://g.gravizo.com/g?
digraph G {
aize ="4,4";
A -> B;
}
)

この場合、`A().output()` => `B().input()` がつながっているということである。

### GCS へのエクスポート機能

数十万行のクエリの結果を手元に引っ張ってきてCSVに出力するのは遅いので、
クエリの結果を直接 GCS にエクスポートする機能を提供しています。

`luigi_bigquery.QueryToGCS` を継承した Task クラスを作成し、`bucket`, `path`, `query` メソッドを作成するだけで OK。
内部的な動作としては、クエリ結果を格納するテンポラリテーブルを作成し、そこから BigQuery の Table Export 機能を使って GCS に出力しています。

```python
class MyQueryToGCS(luigi_bigquery.QueryToGCS):
    def bucket(self):
        return 'my-bucket'

    def path(self):
        return '/path/to/file.csv'

    def query(self):
        return "SELECT count(*) cnt FROM [publicdata:samples.github_nested]"
```

## まとめ

luigi-bigquery 便利（自画自賛）
