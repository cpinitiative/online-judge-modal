import json
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


@dataclass
class JudgeOneParams:
    executable: dict
    timeout_ms: int
    file_io_name: str
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
    problems = get_usaco_problems()
    problem_id = request.problem_id

    if problem_id not in problems:
        raise HTTPException(status_code=404, detail="Problem not found")

    usaco_to_probgate_mapping = get_usaco_to_probgate_mapping()
    if problem_id not in usaco_to_probgate_mapping:
        raise HTTPException(
            status_code=404, detail="We don't have test data for this problem yet."
        )

    probgate_problem_id = usaco_to_probgate_mapping[problem_id]
    probgate_problem = get_probgate_problem(probgate_problem_id)

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
                        timeout_ms=probgate_problem["time_limit_ms"],
                        file_io_name=probgate_problem["shortname"],
                        input_file_path=f"data_private/probgate/problems/{probgate_problem_id}/{test_case['input']}",
                        output_file_path=f"data_private/probgate/problems/{probgate_problem_id}/{test_case['output']}",
                        result_attrs={
                            "test_case": i,
                            "total_test_cases": len(probgate_problem["tests"]),
                        },
                    )
                    for i, test_case in enumerate(probgate_problem["tests"])
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


@web_app.get("/usaco-problems.json")
async def get_usaco_problems_route():
    problems = get_usaco_problems()
    return problems


@app.function(region="us-east")
@modal.asgi_app()
def fastapi_app():
    return web_app
