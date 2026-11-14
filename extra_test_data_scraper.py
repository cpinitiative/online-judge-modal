"""
Scrapes test data that probgate doesn't have, so that this judge can grade
every problem the old Lambda judge (cpinitiative/online-judge) could:

- USACO problems missing from the probgate mapping: downloaded from the test
  data ZIPs linked on usaco.org's contest results pages.
- The CSES problemset: CSES only shows test data for a problem after you've
  submitted to it, so this submits a dummy wrong answer to every problem the
  account hasn't submitted to yet, then downloads the tests from that
  submission's results page. (Ported from cpinitiative/oj-test-data.)

Test data is written to data_private/extra/problems/{problem_id}/{n}.in and
{n}.out, and data_private/extra/index.json maps each problem ID to
{"num_tests", "time_limit_ms", "file_io_name"}.

Run via scrape.py (`modal run scrape.py`). CSES needs CSES_USERNAME and
CSES_PASSWORD in the `cses` Modal secret; it's skipped if they're not set.
"""

import io
import json
import os
import re
import shutil
import time
import zipfile

import requests
from bs4 import BeautifulSoup

EXTRA_DIR = "data_private/extra"
INDEX_PATH = os.path.join(EXTRA_DIR, "index.json")
USACO_URL = "https://usaco.org/"
REQUEST_DELAY = 1  # seconds; be nice to usaco.org and cses.fi

# The old judge used a 2 second time limit for every problem.
DEFAULT_TIME_LIMIT_MS = 2000


def load_index() -> dict:
    try:
        with open(INDEX_PATH, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_index(index: dict):
    os.makedirs(EXTRA_DIR, exist_ok=True)
    with open(INDEX_PATH, "w") as f:
        json.dump(index, f, indent=2, sort_keys=True)


def problem_dir(problem_id: str) -> str:
    return os.path.join(EXTRA_DIR, "problems", problem_id)


def write_problem(problem_id: str, tests: list[tuple[bytes, bytes]]):
    """Atomically replaces a problem's test data with the given tests."""
    final_dir = problem_dir(problem_id)
    tmp_dir = final_dir + ".tmp"
    shutil.rmtree(tmp_dir, ignore_errors=True)
    os.makedirs(tmp_dir)
    for i, (input_data, output_data) in enumerate(tests, start=1):
        with open(os.path.join(tmp_dir, f"{i}.in"), "wb") as f:
            f.write(input_data)
        with open(os.path.join(tmp_dir, f"{i}.out"), "wb") as f:
            f.write(output_data)
    shutil.rmtree(final_dir, ignore_errors=True)
    os.rename(tmp_dir, final_dir)


# ---------------------------------------------------------------------------
# USACO
# ---------------------------------------------------------------------------

# Newer contests use 1.in / 1.out; older ones use I.1 / O.1.
USACO_INPUT_RE = re.compile(r"^(?:(\d+)\.in|I\.(\d+))$")
USACO_OUTPUT_RE = re.compile(r"^(?:(\d+)\.out|O\.(\d+))$")


def parse_usaco_zip(content: bytes) -> list[tuple[bytes, bytes]] | None:
    inputs: dict[int, bytes] = {}
    outputs: dict[int, bytes] = {}
    with zipfile.ZipFile(io.BytesIO(content)) as z:
        for name in z.namelist():
            base = os.path.basename(name)
            if name.startswith("__MACOSX") or not base:
                continue
            if m := USACO_INPUT_RE.match(base):
                inputs[int(m.group(1) or m.group(2))] = z.read(name)
            elif m := USACO_OUTPUT_RE.match(base):
                outputs[int(m.group(1) or m.group(2))] = z.read(name)
    n = len(inputs)
    if n == 0 or set(inputs) != set(outputs) or set(inputs) != set(range(1, n + 1)):
        return None
    return [(inputs[i], outputs[i]) for i in range(1, n + 1)]


def get_usaco_results_pages(session: requests.Session) -> list[str]:
    soup = BeautifulSoup(
        session.get(USACO_URL + "index.php?page=contests").content, "html.parser"
    )
    return [
        USACO_URL + a["href"]
        for a in soup.find_all("a", href=True)
        if "results" in a["href"] and "Results" in a.text
    ]


def get_usaco_test_data_links(
    session: requests.Session, results_url: str
) -> dict[str, str]:
    """{usaco problem id: test data zip url} for one contest's results page."""
    soup = BeautifulSoup(session.get(results_url).content, "html.parser")
    links = {}
    for panel in soup.find_all("div", class_="historypanel"):
        problem_id = data_url = None
        for a in panel.find_all("a", href=True):
            if "cpid=" in a["href"]:
                problem_id = a["href"].split("cpid=")[1].split("&")[0]
            if "Test data" in a.text:
                data_url = requests.compat.urljoin(USACO_URL, a["href"])
        if problem_id and data_url:
            links[problem_id] = data_url
    return links


def scrape_usaco(index: dict, wanted_ids: set[str], file_io_names: dict):
    """Downloads test data for the USACO problems in wanted_ids."""
    missing = {id for id in wanted_ids if f"usaco-{id}" not in index}
    print(f"USACO: {len(missing)} problems need test data")
    if not missing:
        return

    session = requests.Session()
    for results_url in get_usaco_results_pages(session):
        time.sleep(REQUEST_DELAY)
        for usaco_id, data_url in get_usaco_test_data_links(
            session, results_url
        ).items():
            if usaco_id not in missing:
                continue
            problem_id = f"usaco-{usaco_id}"
            time.sleep(REQUEST_DELAY)
            try:
                response = session.get(data_url)
                response.raise_for_status()
                tests = parse_usaco_zip(response.content)
            except (requests.RequestException, zipfile.BadZipFile) as e:
                print(f"{problem_id}: failed to download {data_url}: {e}")
                continue
            if tests is None:
                print(f"{problem_id}: couldn't find numbered tests in {data_url}")
                continue
            write_problem(problem_id, tests)
            index[problem_id] = {
                "num_tests": len(tests),
                "time_limit_ms": DEFAULT_TIME_LIMIT_MS,
                "file_io_name": file_io_names.get(problem_id),
            }
            save_index(index)
            missing.discard(usaco_id)
            print(f"{problem_id}: saved {len(tests)} tests")

    for usaco_id in sorted(missing, key=int):
        print(f"usaco-{usaco_id}: no test data found on usaco.org")


# ---------------------------------------------------------------------------
# CSES
# ---------------------------------------------------------------------------

CSES_URL = "https://cses.fi"
CSES_DUMMY_CODE = "int main() { return 0; }\n"


def cses_login(session: requests.Session) -> str:
    username = os.environ.get("CSES_USERNAME")
    password = os.environ.get("CSES_PASSWORD")
    soup = BeautifulSoup(session.get(f"{CSES_URL}/login").content, "html.parser")
    csrf_token = soup.find("input", {"name": "csrf_token"})["value"]
    response = session.post(
        f"{CSES_URL}/login",
        data={"nick": username, "pass": password, "csrf_token": csrf_token},
    )
    response.raise_for_status()
    if "/logout" not in response.text:
        raise RuntimeError("CSES login failed; check CSES_USERNAME / CSES_PASSWORD")
    return csrf_token


def cses_problem_list(session: requests.Session) -> list[tuple[str, bool]]:
    """[(task id, whether this account has submitted to it)]"""
    soup = BeautifulSoup(
        session.get(f"{CSES_URL}/problemset/list/").content, "html.parser"
    )
    problems = []
    for task in soup.find_all("li", class_="task"):
        link = task.find("a", href=True)
        if not link:
            continue
        task_id = link["href"].rstrip("/").split("/")[-1]
        status = task.find("span", class_="task-score")
        # task-score has class "full" (solved) or "zero" (attempted) once
        # you've submitted
        submitted = status is not None and (
            "full" in status.get("class", []) or "zero" in status.get("class", [])
        )
        problems.append((task_id, submitted))
    return problems


def cses_submit_dummy(session: requests.Session, csrf_token: str, task_id: str):
    response = session.post(
        f"{CSES_URL}/course/send.php",
        data={
            "csrf_token": csrf_token,
            "task": task_id,
            "lang": "C++",
            "option": "C++17",
            "type": "course",
            "target": "problemset",
        },
        files={"file": ("main.cpp", CSES_DUMMY_CODE)},
    )
    response.raise_for_status()


def cses_time_limit_ms(soup: BeautifulSoup) -> int:
    text = soup.get_text(" ")
    if m := re.search(r"Time limit:\s*([\d.]+)\s*s", text):
        return int(float(m.group(1)) * 1000)
    return DEFAULT_TIME_LIMIT_MS


def cses_download_tests(
    session: requests.Session, task_id: str
) -> tuple[list[tuple[bytes, bytes]], int] | None:
    task_url = f"{CSES_URL}/problemset/task/{task_id}"
    task_soup = BeautifulSoup(session.get(task_url).content, "html.parser")
    time_limit_ms = cses_time_limit_ms(task_soup)

    result_link = next(
        (
            a["href"]
            for a in task_soup.find_all("a", href=True)
            if "/problemset/result/" in a["href"]
        ),
        None,
    )
    if result_link is None:
        return None
    result_soup = BeautifulSoup(
        session.get(requests.compat.urljoin(CSES_URL, result_link)).content,
        "html.parser",
    )

    # Each test case on the results page has "save" links for its input
    # (/file/{id}/1/...) and correct output (/file/{id}/2/...). Some tests have
    # no output file; those are treated as empty.
    tests: dict[int, dict[str, str]] = {}
    test_number = 0
    for a in result_soup.find_all("a", class_="save", href=True):
        m = re.match(r"^/file/([^/]+)/([12])/", a["href"])
        if not m:
            continue
        file_id, kind = m.groups()
        if kind == "1":
            test_number += 1
            tests[test_number] = {"in": file_id}
        elif test_number in tests and "out" not in tests[test_number]:
            # Only take the first output link after each input: that's the
            # correct output, not the submission's output.
            tests[test_number]["out"] = file_id

    def fetch(file_id: str | None, kind: str) -> bytes:
        if file_id is None:
            return b""
        response = session.get(f"{CSES_URL}/file/{file_id}/{kind}/2/")
        response.raise_for_status()
        return response.content

    result = []
    for n in sorted(tests):
        result.append((fetch(tests[n]["in"], "1"), fetch(tests[n].get("out"), "2")))
        time.sleep(0.2)
    return result, time_limit_ms


def scrape_cses(index: dict):
    if not os.environ.get("CSES_USERNAME") or not os.environ.get("CSES_PASSWORD"):
        print("CSES: skipping; CSES_USERNAME / CSES_PASSWORD aren't set")
        return

    session = requests.Session()
    csrf_token = cses_login(session)
    problems = cses_problem_list(session)
    todo = [(id, sub) for id, sub in problems if f"cses-{id}" not in index]
    print(f"CSES: {len(problems)} problems, {len(todo)} need test data")

    for task_id, submitted in todo:
        if not submitted:
            print(f"cses-{task_id}: submitting a dummy solution")
            cses_submit_dummy(session, csrf_token, task_id)
            time.sleep(3)

    for task_id, _ in todo:
        problem_id = f"cses-{task_id}"
        time.sleep(REQUEST_DELAY)
        try:
            downloaded = cses_download_tests(session, task_id)
        except requests.RequestException as e:
            print(f"{problem_id}: failed: {e}")
            continue
        if not downloaded or not downloaded[0]:
            print(f"{problem_id}: no submission results with tests found")
            continue
        tests, time_limit_ms = downloaded
        write_problem(problem_id, tests)
        index[problem_id] = {
            "num_tests": len(tests),
            "time_limit_ms": time_limit_ms,
            "file_io_name": None,
        }
        save_index(index)
        print(f"{problem_id}: saved {len(tests)} tests")


def main():
    with open("data_private/usaco/problems.json", "r") as f:
        usaco_problems = json.load(f)
    with open("data_private/probgate/usaco_to_probgate_mapping.json", "r") as f:
        probgate_mapping = json.load(f)
    with open("legacy_file_io_names.json", "r") as f:
        file_io_names = json.load(f)

    index = load_index()
    scrape_usaco(
        index,
        wanted_ids=set(usaco_problems) - set(probgate_mapping),
        file_io_names=file_io_names,
    )
    scrape_cses(index)
    save_index(index)
    print(f"\n{len(index)} problems with extra test data")


if __name__ == "__main__":
    main()
