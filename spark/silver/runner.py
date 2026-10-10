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


def execute(job, request):
    directory = ROOT / job
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "request.json").write_text(json.dumps(request))
    cmd = [
        "/opt/spark/bin/spark-submit",
        "--master",
        "local[2]",
        "/opt/silver/jobs/silver_pipeline.py",
        "--run-id",
        request["run_id"],
        "--result-file",
        str(directory / "results.json"),
    ]
    for dataset in request["datasets"]:
        cmd += ["--dataset-id", dataset]
    code = 1
    try:
        with (directory / "spark.log").open("w") as log:
            process = subprocess.Popen(
                cmd, stdout=log, stderr=subprocess.STDOUT, start_new_session=True
            )
            try:
                code = process.wait(timeout=7200)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
                code = 124
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
            request = {
                "run_id": request["run_id"],
                "datasets": select_datasets(request.get("dataset_id")),
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
