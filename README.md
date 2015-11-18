# luigi-bigquery-exercise

Example code of bq_sushi2

## Setup

```bash
$ pip install -r requirements.txt
```

## Run with local scheduler

```
$ ./tasks/tasks.py DailyTask --day 2015-11-17 --local-scheduler
```

## Run with central scheduler

Run luigid first, and open http://localhost:8082.

```bash
$ luigid
```

Then open another console and run

```bash
$ ./tasks/tasks.py DailyTask --day 2015-11-17
```
