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

class DailyTop10Account(luigi_bigquery.QueryTable):
    day = luigi.DateParameter()
    source = "queries/daily_top10_account.sql"

    def dataset(self):
        return 'tmp'

    def table(self):
        return "{0}_{1}".format('daily_top10_account', self.day.strftime('%Y%m%d'))

class DailyTop10Organization(luigi_bigquery.QueryTable):
    day = luigi.DateParameter()
    source = "queries/daily_top10_organization.sql"

    def dataset(self):
        return 'tmp'

    def table(self):
        return "{0}_{1}".format('daily_top10_organization', self.day.strftime('%Y%m%d'))

class DailyTop10Task(luigi_bigquery.QueryTable):
    day = luigi.DateParameter()
    source = "queries/daily_top10.sql"

    def requires(self):
        return {
            'account': DailyTop10Account(day=self.day),
            'organization': DailyTop10Organization(day=self.day),
        }

    def dataset(self):
        return 'result'

    def table(self):
        return "daily_top10_{0}".format(self.day.strftime('%Y%m%d'))

    def input_table(self, index):
        t = self.input()[index]
        return "{0}.{1}".format(t.dataset_id, t.table_id)

class Top10AccountInAWeek(luigi_bigquery.QueryTable):
    source = "queries/top10_account_in_a_week.sql"

    def requires(self):
        f = lambda x: DailyTop10Account(day=dt.strptime("2015-11-{0:02d}".format(x), "%Y-%m-%d"))
        return [super(Top10AccountInAWeek, self).requires()] + list(map(f, range(8, 15)))
        # Above code is same mean as following code
        #return [
        #    super(Top10AccountInAWeek, self).requires(),
        #    DailyTop10Account(day=dt.strptime("2015-11-08", "%Y-%m-%d")),
        #    DailyTop10Account(day=dt.strptime("2015-11-09", "%Y-%m-%d")),
        #    DailyTop10Account(day=dt.strptime("2015-11-10", "%Y-%m-%d")),
        #    DailyTop10Account(day=dt.strptime("2015-11-11", "%Y-%m-%d")),
        #    DailyTop10Account(day=dt.strptime("2015-11-12", "%Y-%m-%d")),
        #    DailyTop10Account(day=dt.strptime("2015-11-13", "%Y-%m-%d")),
        #    DailyTop10Account(day=dt.strptime("2015-11-14", "%Y-%m-%d"))
        #]

    def dataset(self):
        return 'result'

    def table(self):
        return "daily_top10_account_in_a_week"

    def input_table(self, index):
        t = self.input()[index]
        return "{0}.{1}".format(t.dataset_id, t.table_id)

class ReportTask(luigi_bigquery.QueryToGCS):
    source = "queries/export_top10_account_in_a_week.sql"

    def requires(self):
        return Top10AccountInAWeek()

    def dataset(self):
        return 'tmp'

    def bucket(self):
        return "bq_sushi_2_01"

    def path(self):
        return "reports/top10_account_in_a_week.csv"

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
