# Setup re:dash on GCP

## Offical Document

http://docs.redash.io/en/latest/setup.html#google-compute-engine

## Enable Compute Engine API of your project

1. `API Manager`
2. Search `Compute Engine`
3. Click `Enable API`

## Import image to GCP

```bash
$ gcloud compute images create "redash-081-b1110-bq" \
--source-uri gs://redash-images/redash.0.8.1.b1110-bq.tar.gz
```

## Create network

```bash
$ gcloud compute networks create redash --range 10.242.1.0/24
$ gcloud compute firewall-rules create redash-allow-http \
--allow tcp:80 \
--network redash \
--source-ranges 0.0.0.0/0
$ gcloud compute firewall-rules create redash-allow-https \
--allow tcp:443 \
--network redash
\--source-ranges 0.0.0.0/0
```

## Start instance

```bash
$ gcloud compute instances create redash \
--image redash-081-b1110-bq \
--scopes storage-ro,bigquery \
--network redash \
--zone us-central1-a
```

## Delete instance

```bash
$ gcloud compute instances delete redash --zone us-central1-a
```
