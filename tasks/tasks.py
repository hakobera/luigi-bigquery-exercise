#!/usr/bin/env python

import luigi
import luigi_bigquery
from datetime import datetime as dt

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
        return {
            'account_20151108': DailyTop10Account(day=dt.strptime("2015-11-08", "%Y-%m-%d")),
            'account_20151109': DailyTop10Account(day=dt.strptime("2015-11-09", "%Y-%m-%d")),
            'account_20151110': DailyTop10Account(day=dt.strptime("2015-11-10", "%Y-%m-%d")),
            'account_20151111': DailyTop10Account(day=dt.strptime("2015-11-11", "%Y-%m-%d")),
            'account_20151112': DailyTop10Account(day=dt.strptime("2015-11-12", "%Y-%m-%d")),
            'account_20151113': DailyTop10Account(day=dt.strptime("2015-11-13", "%Y-%m-%d")),
            'account_20151114': DailyTop10Account(day=dt.strptime("2015-11-14", "%Y-%m-%d")),
        }

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

if __name__ == '__main__':
    import sys
    if luigi.run():
        sys.exit(0)
    else:
        sys.exit(1)
