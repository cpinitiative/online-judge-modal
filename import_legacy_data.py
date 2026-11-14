"""
Copies the test data used by the old AWS Lambda judge
(https://github.com/cpinitiative/online-judge) into the Modal volume, so that
every problem the old judge could grade can also be graded here.

The old judge reads test data from the `cpi-onlinejudge` S3 bucket, laid out as
`{problem_id}/{n}.in` and `{problem_id}/{n}.out`, where problem_id is e.g.
`usaco-1046` or `cses-1164` (see https://github.com/cpinitiative/oj-test-data).
This includes all of the CSES problemset, which probgate doesn't have.

Files are written to `data_private/legacy/problems/{problem_id}/`, and an index
of `{problem_id: number_of_test_cases}` is written to
`data_private/legacy/index.json`.

Requires a Modal secret named `aws-cpi-onlinejudge` with AWS_ACCESS_KEY_ID,
AWS_SECRET_ACCESS_KEY and AWS_DEFAULT_REGION (us-west-1) for an IAM user that
can read the bucket.

Usage:

    modal run import_legacy_data.py

Re-running is safe: problems already present in the volume are skipped unless
`--overwrite` is passed.
"""

import json
import os
import re
from concurrent.futures import ThreadPoolExecutor

import modal

BUCKET = "cpi-onlinejudge"
LEGACY_DIR = "data_private/legacy"

app = modal.App(
    "usaco-problems-legacy-import",
    image=modal.Image.debian_slim().pip_install("boto3"),
    volumes={
        "/root/data_private": modal.Volume.from_name(
            "usaco-problems", create_if_missing=True
        )
    },
)

TEST_FILE_RE = re.compile(r"^(?P<problem>[^/]+)/(?P<n>\d+)\.(?P<ext>in|out)$")


def list_problems(s3) -> dict[str, list[str]]:
    """Returns {problem_id: [keys]} for every object in the bucket."""
    problems: dict[str, list[str]] = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            match = TEST_FILE_RE.match(key)
            if not match:
                print(f"Skipping unexpected key {key}")
                continue
            problems.setdefault(match.group("problem"), []).append(key)
    return problems


def num_test_cases(keys: list[str]) -> int | None:
    """
    Number of test cases if the problem has 1.in/1.out ... n.in/n.out with no
    gaps (which is what the old judge assumes), otherwise None.
    """
    inputs = set()
    outputs = set()
    for key in keys:
        match = TEST_FILE_RE.match(key)
        assert match
        (inputs if match.group("ext") == "in" else outputs).add(
            int(match.group("n"))
        )
    n = len(inputs)
    if n == 0 or inputs != outputs or inputs != set(range(1, n + 1)):
        return None
    return n


@app.function(
    secrets=[modal.Secret.from_name("aws-cpi-onlinejudge")],
    timeout=60 * 60 * 2,
)
def import_legacy_data(overwrite: bool = False):
    import boto3

    s3 = boto3.client("s3")
    problems = list_problems(s3)
    print(f"Found {len(problems)} problems in s3://{BUCKET}")

    index_path = os.path.join(LEGACY_DIR, "index.json")
    try:
        with open(index_path, "r") as f:
            index: dict[str, int] = json.load(f)
    except FileNotFoundError:
        index = {}

    def download(problem_id: str, keys: list[str]):
        n = num_test_cases(keys)
        if n is None:
            return problem_id, None, "test files are missing or misnumbered"

        problem_dir = os.path.join(LEGACY_DIR, "problems", problem_id)
        if not overwrite and index.get(problem_id) == n:
            return problem_id, n, "already imported"

        os.makedirs(problem_dir, exist_ok=True)
        for key in keys:
            s3.download_file(
                BUCKET, key, os.path.join(problem_dir, key.split("/", 1)[1])
            )
        return problem_id, n, "imported"

    errors = []
    with ThreadPoolExecutor(max_workers=16) as pool:
        for problem_id, n, status in pool.map(
            lambda item: download(*item), sorted(problems.items())
        ):
            print(f"{problem_id}: {status}")
            if n is None:
                errors.append(f"{problem_id}: {status}")
            else:
                index[problem_id] = n

    os.makedirs(LEGACY_DIR, exist_ok=True)
    with open(index_path, "w") as f:
        json.dump(index, f, indent=2, sort_keys=True)

    print(f"\nIndexed {len(index)} legacy problems; {len(errors)} errors")
    for error in errors:
        print(error)


@app.local_entrypoint()
def main(overwrite: bool = False):
    import_legacy_data.remote(overwrite=overwrite)
