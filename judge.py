import json
import re
import traceback
from dataclasses import dataclass
from fastapi.responses import PlainTextResponse, StreamingResponse
import modal
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import requests
from fastapi.middleware.cors import CORSMiddleware

web_app = FastAPI()

web_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app = modal.App(
    "usaco-judge",
    image=modal.Image.debian_slim().pip_install(
        "fastapi[standard]", "pydantic", "requests"
    ),
    volumes={"/root/data_private": modal.Volume.from_name("usaco-problems")},
)

COMPILE_URL = (
    "https://v3nuswv3poqzw6giv37wmrt6su0krxvt.lambda-url.us-east-1.on.aws/compile"
)
EXECUTE_URL = (
    "https://v3nuswv3poqzw6giv37wmrt6su0krxvt.lambda-url.us-east-1.on.aws/execute"
)
LARGE_INPUT_URL = (
    "https://v3nuswv3poqzw6giv37wmrt6su0krxvt.lambda-url.us-east-1.on.aws/large-input"
)


def get_usaco_problems():
    with open("data_private/usaco/problems.json", "r") as f:
        return json.load(f)


def get_usaco_to_probgate_mapping():
    with open("data_private/probgate/usaco_to_probgate_mapping.json", "r") as f:
        return json.load(f)


def get_probgate_problem(problem_id: str):
    with open(f"data_private/probgate/problems/{problem_id}/config.json", "r") as f:
        return json.load(f)


def get_extra_index():
    """
    {problem_id: {"num_tests", "time_limit_ms", "file_io_name"}} for test data
    probgate doesn't have (see extra_test_data_scraper.py).
    """
    try:
        with open("data_private/extra/index.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


@dataclass
class ResolvedProblem:
    timeout_ms: int
    file_io_name: str | None
    # (input path, output path) for each test case
    tests: list[tuple[str, str]]


class ProblemNotFound(Exception):
    pass


def resolve_problem(problem_id: str) -> ResolvedProblem:
    """
    Accepts a USACO problem ID ("1251" or "usaco-1251") or a USACO Guide
    problem ID for another judge we have test data for (e.g. "cses-1164").

    USACO problems use probgate test data when we have it. Otherwise, fall back
    to the test data from extra_test_data_scraper.py.
    """
    usaco_match = re.fullmatch(r"(?:usaco-)?(\d+)", problem_id)
    if usaco_match:
        usaco_id = usaco_match.group(1)
        usaco_to_probgate_mapping = get_usaco_to_probgate_mapping()
        if usaco_id in usaco_to_probgate_mapping:
            probgate_problem_id = usaco_to_probgate_mapping[usaco_id]
            probgate_problem = get_probgate_problem(probgate_problem_id)
            problem_dir = f"data_private/probgate/problems/{probgate_problem_id}"
            return ResolvedProblem(
                timeout_ms=probgate_problem["time_limit_ms"],
                file_io_name=probgate_problem["shortname"],
                tests=[
                    (
                        f"{problem_dir}/{test_case['input']}",
                        f"{problem_dir}/{test_case['output']}",
                    )
                    for test_case in probgate_problem["tests"]
                ],
            )
        extra_id = f"usaco-{usaco_id}"
    else:
        extra_id = problem_id

    extra = get_extra_index().get(extra_id)
    if extra:
        problem_dir = f"data_private/extra/problems/{extra_id}"
        return ResolvedProblem(
            timeout_ms=extra["time_limit_ms"],
            file_io_name=extra["file_io_name"],
            tests=[
                (f"{problem_dir}/{i}.in", f"{problem_dir}/{i}.out")
                for i in range(1, extra["num_tests"] + 1)
            ],
        )

    if usaco_match and usaco_match.group(1) not in get_usaco_problems():
        raise ProblemNotFound("Problem not found")
    raise ProblemNotFound("We don't have test data for this problem yet.")


@dataclass
class JudgeOneParams:
    executable: dict
    timeout_ms: int
    file_io_name: str | None
    input_file_path: str
    output_file_path: str
    result_attrs: dict


@app.function(region="us-east", cloud="aws")
def judge_one(
    params: JudgeOneParams,
):
    with open(params.input_file_path, "r") as f:
        input_data = f.read()
    with open(params.output_file_path, "r") as f:
        output_data = f.read()

    if len(input_data) > 2_000_000:
        # There's a 6 MB limit for AWS lambda functions
        response = requests.post(
            LARGE_INPUT_URL,
        )
        result = response.json()
        requests.put(
            result["presigned_url"],
            data=input_data,
        )
        stdin, stdin_id = None, result["input_id"]
    else:
        stdin, stdin_id = input_data, None

    response = requests.post(
        EXECUTE_URL,
        json={
            "executable": params.executable,
            "options": {
                "stdin": stdin,
                "stdin_id": stdin_id,
                "timeout_ms": params.timeout_ms,
                "file_io_name": params.file_io_name,
            },
        },
        headers={"Content-Type": "application/json"},
    )
    try:
        result = response.json()

        if result["full_output_url"] is not None:
            response = requests.get(result["full_output_url"])
            result = response.json()
    except requests.JSONDecodeError:
        result = {"internal_error": response.text}

    if "internal_error" not in result:
        # Naive grader: Check for identical output
        if result["verdict"] == "accepted":
            output = result["file_output"] or result["stdout"]
            if output.strip() != output_data.strip():
                result["verdict"] = "wrong_answer"

        result["stdout"] = result["stdout"][:10000]
        result["stderr"] = result["stderr"][:10000]
        if result["file_output"] is not None:
            result["file_output"] = result["file_output"][:10000]

    result = {
        **result,
        **params.result_attrs,
    }

    return f"event: execute\ndata: {json.dumps(result)}\n\n"


def compile(source_code: str, compiler_options: str, language: str):
    response = requests.post(
        COMPILE_URL,
        json={
            "source_code": source_code,
            "compiler_options": compiler_options,
            "language": language,
        },
        headers={"Content-Type": "application/json"},
    )
    try:
        return response.json()
    except requests.JSONDecodeError:
        raise Exception(response.text)


class JudgeRequest(BaseModel):
    problem_id: str
    source_code: str
    compiler_options: str
    language: str


@web_app.post("/judge")
def judge(request: JudgeRequest):
    try:
        problem = resolve_problem(request.problem_id)
    except ProblemNotFound as e:
        raise HTTPException(status_code=404, detail=str(e))

    def _judge():
        try:
            compile_result = compile(
                request.source_code, request.compiler_options, request.language
            )

            if "compile_output" in compile_result:
                yield f"event: compile\ndata: {json.dumps(compile_result['compile_output'])}\n\n"
            else:
                yield f"event: compile\ndata: {json.dumps(compile_result)}\n\n"

            if (
                "executable" not in compile_result
                or compile_result["executable"] is None
            ):
                return

            yield from judge_one.map(
                (
                    JudgeOneParams(
                        executable=compile_result["executable"],
                        timeout_ms=problem.timeout_ms,
                        file_io_name=problem.file_io_name,
                        input_file_path=input_file_path,
                        output_file_path=output_file_path,
                        result_attrs={
                            "test_case": i,
                            "total_test_cases": len(problem.tests),
                        },
                    )
                    for i, (input_file_path, output_file_path) in enumerate(
                        problem.tests
                    )
                ),
                order_outputs=False,
            )
        except Exception as e:
            # Kinda dumb but we can't have newlines, so we use repr()
            yield f"event: error\ndata: {repr(e)}. {repr(traceback.format_exc())}\n\n"

    return StreamingResponse(
        _judge(),
        media_type="text/event-stream",
    )


@web_app.get("/")
async def root():
    return PlainTextResponse("Judge OK")


@web_app.get("/problems-with-test-data.json")
async def get_problems_with_test_data_route():
    """
    IDs of every problem that can be judged, in the same format /judge accepts
    (e.g. "usaco-1251", "cses-1164").
    """
    problem_ids = {f"usaco-{id}" for id in get_usaco_to_probgate_mapping()}
    problem_ids.update(get_extra_index().keys())
    return sorted(problem_ids)


@web_app.get("/usaco-problems.json")
async def get_usaco_problems_route():
    problems = get_usaco_problems()
    return problems


@app.function(region="us-east")
@modal.asgi_app()
def fastapi_app():
    return web_app
