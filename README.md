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

## Test Data Probgate Doesn't Have

`scrape.py` also runs `extra_test_data_scraper.py`, which fills in what
probgate is missing so this judge covers everything the old Lambda judge did:

- USACO problems missing from the probgate mapping, from the test data ZIPs on
  usaco.org's results pages.
- The CSES problemset. CSES only shows tests after you've submitted, so this
  submits a dummy solution to each problem the account hasn't tried yet and then
  downloads the tests from that submission. It needs a `cses` Modal secret:

  ```
  modal secret create cses CSES_USERNAME=... CSES_PASSWORD=...
  ```

The judge uses probgate data for USACO problems when it has it, and this data
otherwise. `/judge` accepts `1251`, `usaco-1251` or `cses-1164`.
`GET /problems-with-test-data.json` lists every problem that can be judged.

## Deploying Judge

```
modal deploy judge.py
```
