# Online Judge Backend (Modal)

Source code for the online judge backend. WIP

## Setup

```
pip install -r requirements.txt
```

## Scraping Problem Data

```
modal run scrape.py
```

This scrapes new USACO problems and every probgate contest from a month that
has already ended, so re-running it after a contest picks up the new problems.

## Importing Test Data From the Old Judge

The old AWS Lambda judge ([cpinitiative/online-judge](https://github.com/cpinitiative/online-judge))
reads test data from the `cpi-onlinejudge` S3 bucket. That includes problems
probgate doesn't have, like the CSES problemset. To copy it into the Modal
volume, create a Modal secret named `aws-cpi-onlinejudge` with
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_DEFAULT_REGION` for an
IAM user that can read the bucket, then run:

```
modal run import_legacy_data.py
```

The judge uses probgate data for USACO problems when it has it, and falls back
to the imported data otherwise. `/judge` accepts `1251`, `usaco-1251` or
`cses-1164`. `GET /problems-with-test-data.json` lists every problem that can
be judged.

## Deploying Judge

```
modal deploy judge.py
```
