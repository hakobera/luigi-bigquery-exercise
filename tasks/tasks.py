#!/usr/bin/env python

import luigi
import luigi_bigquery

from datetime import datetime as dt
import six

class DailyTask(luigi.Task):
    day = luigi.DateParameter()

    def requires(self):
        return DailyTop10Account(day=self.day)

    def output(self):
        return luigi.LocalTarget("output/DailyTask-{0}.txt".format(self.day.strftime('%Y%m%d')))

    def run(self):
        with self.output().open('w') as f:
            f.write("Done\n")

class BatchTask(luigi.Task):
    def requires(self):
        f = lambda x: DailyTop10Account(day=dt.strptime("2015-11-{0:02d}".format(x), "%Y-%m-%d"))
        return map(f, range(8, 14))

        # Above code is same mean as following code
        #return [
        #    DailyTop10Account(day=dt.strptime("2015-11-08", "%Y-%m-%d")),
        #    DailyTop10Account(day=dt.strptime("2015-11-09", "%Y-%m-%d")),
        #    DailyTop10Account(day=dt.strptime("2015-11-10", "%Y-%m-%d")),
        #    DailyTop10Account(day=dt.strptime("2015-11-11", "%Y-%m-%d")),
        #    DailyTop10Account(day=dt.strptime("2015-11-12", "%Y-%m-%d")),
        #    DailyTop10Account(day=dt.strptime("2015-11-13", "%Y-%m-%d")),
        #    DailyTop10Account(day=dt.strptime("2015-11-14", "%Y-%m-%d"))
        #]

    def output(self):
        return luigi.LocalTarget("output/BatchTask.txt")

    def run(self):
        with self.output().open('w') as f:
            f.write("Done\n")

class DailyTop10Account(luigi_bigquery.QueryTable):
    day = luigi.DateParameter()
    source = "queries/daily_top10_account.sql"

    def dataset(self):
        return 'tmp'

    def table(self):
        return "{0}_{1}".format('daily_top10_acount', self.day.strftime('%Y%m%d'))

class PandasExample(luigi_bigquery.Query):
    day = luigi.DateParameter()
    source = "queries/daily_top10_account.sql"

    def output(self):
        return luigi.LocalTarget("output/PandasExample-{0}.txt".format(self.day.strftime('%Y%m%d')))

    def run(self):
        query = self.load_query(self.source)
        result = self.run_query(query)
        with self.output().open('w') as f:
            f.write(result.to_dataframe().to_string())


if __name__ == '__main__':
    import sys
    if luigi.run():
        sys.exit(0)
    else:
        sys.exit(1)
