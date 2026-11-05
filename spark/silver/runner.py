"""Internal, bounded submit API. No shell input, host socket, or published port.

One job at a time prevents concurrent publication of the same dataset. Request
IDs are deterministic; Airflow retries attach to the existing process/results.
"""

import hashlib
import hmac
import json
import os
import subprocess
import signal
import re
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from .registry import select_datasets

ROOT = Path(os.environ.get("SILVER_JOB_ROOT", "/tmp/silver-jobs"))
LOCK = threading.Lock()
ACTIVE = {}
TOKEN_ENV = "SILVER_RUNNER_TOKEN"


def validate_token(token):
    if not isinstance(token, str) or len(token) < 32:
        raise RuntimeError(f"{TOKEN_ENV} must contain at least 32 characters")
    return token


def authorized(header, token):
    if not isinstance(header, str) or not header.startswith("Bearer "):
        return False
    return hmac.compare_digest(header[7:], token)


def gold_input_matches(log_path, expected_batch_id):
    records = []
    for line in Path(log_path).read_text().splitlines():
        if not line.startswith("{") or '"silver_batch_id"' not in line:
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            return False
    return (len(records) == 1 and
            records[0].get("silver_batch_id") == expected_batch_id)


def execute(job, request):
    directory = ROOT / job
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "request.json").write_text(json.dumps(request))
    cmd = [
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[2]",
    ]
    environment = os.environ.copy()
    if request["pipeline"] == "gold":
        stages = {"inspect_input", "foundation", "dimensions", "facts", "publish"}
        stage = request["stage"]
        if stage not in stages:
            raise ValueError("Invalid Gold stage")
        manifest_id = request["manifest_id"]
        if stage == "publish":
            cmd += ["/opt/silver/jobs/gold/publish_candidate.py",
                    "--manifest-id", manifest_id]
        else:
            actual = "inspect" if stage == "inspect_input" else stage
            cmd += ["/opt/silver/jobs/gold/candidate.py", "--stage", actual,
                    "--manifest-id", manifest_id]
    elif request["pipeline"] == "silver_release":
        stage = request["stage"]
        scripts = {
            "foundation": "foundation.py", "backfill": "backfill.py",
            "repair_metadata": "repair_metadata.py",
            "reconcile_weather_units": "reconcile_weather_units.py", "refs": "refs.py",
            "policy": "policy.py", "canonical": "canonical.py",
            "validate": "validate.py", "gold_smoke": "gold_contract_smoke.py",
            "prepare": "prepare_publish.py", "activate": "activate.py",
        }
        cmd += [f"/opt/silver/jobs/silver/{scripts[stage]}"]
        environment["BRONZE_RELEASE_ID"] = request["bronze_release_id"]
        if stage == "backfill":
            cmd += ["--run-id", request["run_id"],
                    "--result-file", str(directory / "results.json")]
        elif stage == "validate":
            cmd += ["--run-id", request["dq_run_id"]]
        elif stage == "prepare":
            cmd += ["--batch-id", request["batch_id"],
                    "--dq-run-id", request["dq_run_id"]]
        elif stage == "activate":
            cmd += ["--batch-id", request["batch_id"]]
    else:
        cmd += ["/opt/silver/jobs/silver/backfill.py", "--run-id", request["run_id"]]
        cmd += ["--result-file", str(directory / "results.json")]
        for dataset in request["datasets"]:
            cmd += ["--dataset-id", dataset]
    code = 1
    try:
        with (directory / "spark.log").open("w") as log:
            process = subprocess.Popen(
                cmd, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
                env=environment,
            )
            try:
                code = process.wait(timeout=7200)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                code = 124
        if (code == 0 and request["pipeline"] == "gold" and
                request["stage"] == "inspect_input" and
                request.get("expected_silver_batch_id")):
            if not gold_input_matches(
                    directory / "spark.log", request["expected_silver_batch_id"]):
                with (directory / "spark.log").open("a") as log:
                    log.write("Gold inspected a different Silver batch\n")
                code = 1
    finally:
        temporary = directory / "exit.tmp"
        temporary.write_text(json.dumps({"exit_code": code}))
        temporary.replace(directory / "exit.json")
        with LOCK:
            ACTIVE.pop(job, None)


class Handler(BaseHTTPRequestHandler):
    def reply(self, code, payload):
        data = json.dumps(payload).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        token = validate_token(os.environ.get(TOKEN_ENV))
        if not authorized(self.headers.get("Authorization"), token):
            return self.reply(401, {"error": "unauthorized"})
        if self.path != "/jobs":
            return self.reply(404, {"error": "not_found"})
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 16384:
                raise ValueError()
            request = json.loads(self.rfile.read(length))
            if not isinstance(request, dict):
                raise ValueError()
            if (
                not isinstance(request.get("run_id"), str)
                or not 1 <= len(request["run_id"]) <= 500
            ):
                raise ValueError()
            pipeline = request.get("pipeline", "silver")
            if pipeline not in {"silver", "silver_release", "gold"}:
                raise ValueError()
            if pipeline == "gold":
                stage = request.get("stage")
                if stage not in {"inspect_input", "foundation", "dimensions", "facts", "publish"}:
                    raise ValueError()
                manifest_id = request.get("manifest_id")
                expected = request.get("expected_silver_batch_id")
                if not isinstance(manifest_id, str):
                    raise ValueError()
                from gold.candidate import validate_id
                validate_id(manifest_id)
                if expected is not None and (
                        not isinstance(expected, str) or
                        not re.fullmatch(r"silver-[a-z0-9][a-z0-9_-]{2,95}", expected)):
                    raise ValueError()
                request = {"run_id": request["run_id"], "stage": stage,
                           "manifest_id": manifest_id, "pipeline": "gold",
                           "expected_silver_batch_id": expected}
            elif pipeline == "silver_release":
                stage = request.get("stage")
                if stage not in {"foundation", "backfill", "repair_metadata",
                                 "reconcile_weather_units",
                                 "refs", "policy", "canonical", "validate",
                                 "gold_smoke", "prepare", "activate"}:
                    raise ValueError()
                release_id = request.get("bronze_release_id")
                batch_id = request.get("batch_id")
                dq_run_id = request.get("dq_run_id")
                if (not isinstance(release_id, str) or
                        not re.fullmatch(r"bronze-[a-z0-9][a-z0-9_-]{2,95}", release_id) or
                        not isinstance(batch_id, str) or
                        not re.fullmatch(r"silver-[a-z0-9][a-z0-9_-]{2,95}", batch_id) or
                        not isinstance(dq_run_id, str) or
                        not re.fullmatch(r"silver-dq-[a-z0-9][a-z0-9_-]{2,95}", dq_run_id)):
                    raise ValueError()
                request = {"pipeline": "silver_release", "stage": stage,
                           "run_id": request["run_id"],
                           "bronze_release_id": release_id,
                           "batch_id": batch_id, "dq_run_id": dq_run_id}
            else:
                request = {
                    "run_id": request["run_id"],
                    "datasets": select_datasets(request.get("dataset_id")),
                    "pipeline": pipeline,
                }
        except (ValueError, TypeError, KeyError):
            return self.reply(400, {"error": "invalid_request"})
        job = hashlib.sha256(json.dumps(request, sort_keys=True).encode()).hexdigest()
        with LOCK:
            if job in ACTIVE or (ROOT / job / "exit.json").exists():
                return self.reply(200, {"job_id": job})
            if ACTIVE:
                return self.reply(409, {"error": "runner_busy"})
            ACTIVE[job] = True
            threading.Thread(target=execute, args=(job, request), daemon=True).start()
        self.reply(202, {"job_id": job})

    def do_GET(self):
        if self.path == "/health":
            return self.reply(200, {"status": "ok"})
        token = validate_token(os.environ.get(TOKEN_ENV))
        if not authorized(self.headers.get("Authorization"), token):
            return self.reply(401, {"error": "unauthorized"})
        job = self.path.removeprefix("/jobs/")
        if len(job) != 64 or any(c not in "0123456789abcdef" for c in job):
            return self.reply(404, {"error": "not_found"})
        directory = ROOT / job
        if (directory / "exit.json").exists():
            status = json.loads((directory / "exit.json").read_text())
            results = directory / "results.json"
            return self.reply(
                200,
                {
                    "status": "SUCCESS" if status["exit_code"] == 0 else "FAILED",
                    **status,
                    "results": (
                        json.loads(results.read_text()) if results.exists() else []
                    ),
                    "log_path": str(directory / "spark.log"),
                },
            )
        if job in ACTIVE:
            return self.reply(200, {"status": "RUNNING"})
        return self.reply(404, {"error": "unknown_or_interrupted_job"})


if __name__ == "__main__":
    validate_token(os.environ.get(TOKEN_ENV))
    ROOT.mkdir(parents=True, exist_ok=True)
    ThreadingHTTPServer(("0.0.0.0", 8090), Handler).serve_forever()
