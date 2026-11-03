"""Checks on the workflows, the rolling-issue script and the Dependabot config.

None of it can run until the repository is on GitHub, so these tests read the files as
text (the standard library only, no YAML parser) and run the shell that they hold
against stand-ins for `uv` and `gh`. They look for the mistakes that matter in a public
repository that publishes with its own token: a release made outside the kit, an action
that is not pinned, a job that cannot time out, more permission than a job needs, a
stream that can run twice at once, and a re-dispatch that starves the daily run.
"""

import json
import os
import re
import shutil
import subprocess
import sys
import textwrap
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_DIR = ROOT / ".github" / "workflows"
SCRIPT = ROOT / "scripts" / "rolling-issue.sh"
KEEPALIVE = ROOT / "scripts" / "keepalive.sh"
DEPENDABOT = ROOT / ".github" / "dependabot.yml"
EXPECTED = ("backfill.yml", "smoke.yml", "test.yml", "tick.yml")

# Every job that writes: the token's `contents: write` is for these alone.
PUBLISH_JOBS = {
    ("tick.yml", "pypi-registry"),
    ("tick.yml", "osv-pypi"),
    ("tick.yml", "catalog"),
    ("backfill.yml", "sweep"),
    ("smoke.yml", "smoke"),
}
REPORT_JOB = ("backfill.yml", "report")
# What each job with permissions of its own is granted. A job that is not here has the
# top level's, which is nothing or `contents: read`.
PERMISSIONS = {
    ("tick.yml", "pypi-registry"): {"contents": "write", "issues": "write"},
    ("tick.yml", "osv-pypi"): {"contents": "write", "issues": "write"},
    ("tick.yml", "catalog"): {"contents": "write", "issues": "write"},
    ("backfill.yml", "sweep"): {"contents": "write", "issues": "write"},
    ("backfill.yml", "redispatch"): {"actions": "write"},
    REPORT_JOB: {"actions": "write", "contents": "read", "issues": "write"},
    ("smoke.yml", "smoke"): {"contents": "write", "issues": "write"},
}
# The jobs that dispatch a sweep: the next one, and one replaced while it waited.
DISPATCH_JOBS = {("backfill.yml", "redispatch"), REPORT_JOB}
REGISTRY_GROUP = {"group": "stream-pypi-registry", "cancel-in-progress": "false"}
OSV_GROUP = {"group": "stream-osv-pypi", "cancel-in-progress": "false"}
CATALOG_GROUP = {"group": "catalog", "cancel-in-progress": "false"}
# The streams that tick.yml runs, each in a job named for it, and the command that the
# run step of the job starts. `--bootstrap`, and `--override-reason` with its reason,
# come after it when they are asked for.
TICK_COMMANDS = {
    "pypi-registry": [
        *["uv", "run", "--package", "pypi-registry", "python", "-m", "pypi_registry"],
        *["run", "--max-seconds", "15000"],
    ],
    "osv-pypi": [
        *["uv", "run", "--package", "osv-pypi", "python", "-m", "osv_pypi"],
        *["run", "--max-seconds", "1200"],
    ],
    # The catalog has no budget of its own: its limits are its job's and its step's.
    "catalog": ["uv", "run", "--package", "catalog", "python", "-m", "catalog", "run"],
}
SWEEP_COMMAND = [
    *["uv", "run", "--package", "pypi-registry", "python", "-m", "pypi_registry"],
    *["sweep", "--max-seconds", "15000"],
]
RUN_URL = "https://github.com/py-observatory/data/actions/runs/12345"


# --- reading the workflows as text ---


def workflows() -> dict[str, str]:
    """The text of each workflow by file name. The workflows that this repository is
    meant to have must all be there, so that no check below passes by finding none."""
    found = {
        path.name: path.read_text(encoding="utf-8")
        for path in sorted(WORKFLOW_DIR.glob("*.yml"))
    }
    missing = sorted(set(EXPECTED) - set(found))
    assert not missing, f"missing workflows: {', '.join(missing)}"
    return found


def split_jobs(text: str) -> tuple[str, dict[str, str]]:
    """The text of a workflow outside its `jobs:` section, and the text under each
    job's name, in the order of the file."""
    outside: list[str] = []
    jobs: dict[str, list[str]] = {}
    in_jobs = False
    current: str | None = None
    for line in text.splitlines():
        if re.match(r"[A-Za-z_-]+:", line):  # a key at the top level
            in_jobs = line.startswith("jobs:")
            current = None
            if not in_jobs:
                outside.append(line)
        elif in_jobs:
            header = re.fullmatch(r"  ([A-Za-z0-9_-]+):\s*(?:#.*)?", line)
            if header:
                current = header[1]
                jobs[current] = []
            elif current is not None:
                jobs[current].append(line)
        else:
            outside.append(line)
    return "\n".join(outside), {name: "\n".join(body) for name, body in jobs.items()}


def all_jobs() -> list[tuple[str, str, str]]:
    """(workflow file, job name, job text) for every job of every workflow."""
    return [
        (file, name, body)
        for file, text in workflows().items()
        for name, body in split_jobs(text)[1].items()
    ]


def job_of(file: str, name: str) -> str:
    return split_jobs(workflows()[file])[1][name]


def entry(section: str, key: str, indent: int) -> tuple[str, str] | None:
    """What a key at `indent` spaces holds, as (the value on its own line without a
    comment, the lines nested under it), or None when the section has no such key."""
    lines = section.splitlines()
    for i, line in enumerate(lines):
        found = re.fullmatch(rf" {{{indent}}}{re.escape(key)}:\s*(.*)", line)
        if not found:
            continue
        body = []
        for follow in lines[i + 1 :]:
            depth = len(follow) - len(follow.lstrip())
            if follow.strip() and depth <= indent and not follow.lstrip()[0] == "#":
                break
            body.append(follow)
        return re.sub(r"\s+#.*$", "", found[1]).strip(), "\n".join(body)
    return None


def scalar(section: str, key: str, indent: int) -> str | None:
    found = entry(section, key, indent)
    return None if found is None else found[0]


def mapping(section: str, key: str, indent: int) -> dict[str, str] | str | None:
    """A key whose value is a map of scalars, in flow style (`{a: b}`) or block style.
    A scalar value (`read-all`) comes back as a string, a missing key as None."""
    found = entry(section, key, indent)
    if found is None:
        return None
    inline, body = found
    if inline.startswith("{"):
        pairs = [pair.split(":", 1) for pair in inline.strip("{} ").split(",") if pair]
        return {name.strip(): value.strip() for name, value in pairs}
    if inline:
        return inline
    pattern = rf" {{{indent + 2}}}([\w-]+):\s*(\S.*?)\s*(?:#.*)?"
    rows = [re.fullmatch(pattern, line) for line in body.splitlines()]
    return {row[1]: row[2] for row in rows if row}


def sequence(section: str, key: str, indent: int) -> list[str] | None:
    """A key whose value is a list of scalars, in flow style (`[a, b]`) or block style.
    A scalar value comes back as a list of one, a missing key as None."""
    found = entry(section, key, indent)
    if found is None:
        return None
    inline, body = found
    if inline.startswith("["):
        return [item.strip() for item in inline.strip("[] ").split(",") if item.strip()]
    if inline:
        return [inline]
    return re.findall(r"(?m)^\s+- (\S+)", body)


def steps(job: str) -> list[str]:
    """The text of each step of a job (they come last in a job's text), without the
    comments, so that a comment above a step is not read as part of the one before."""
    lines = [line for line in job.splitlines() if not line.lstrip().startswith("#")]
    code = "\n".join(lines)
    return ["- " + part for part in re.split(r"(?m)^      - ", code)[1:]]


def run_scripts(text: str) -> list[str]:
    """The script of every `run:` in `text`, dedented."""
    scripts = []
    lines = text.splitlines()
    for i, line in enumerate(lines):
        found = re.fullmatch(r"(\s*)(-\s+)?run:\s*(.*?)\s*", line)
        if not found:
            continue
        value = found[3]
        if not re.fullmatch(r"[|>][+-]?", value):
            scripts.append(value)
            continue
        column = len(found[1]) + len(found[2] or "")
        body = []
        for follow in lines[i + 1 :]:
            if follow.strip() and len(follow) - len(follow.lstrip()) <= column:
                break
            body.append(follow)
        scripts.append(textwrap.dedent("\n".join(body)))
    return scripts


def one_script(file: str, name: str, mentions: str) -> str:
    """The one `run:` script of a job that contains `mentions`."""
    [script] = [s for s in run_scripts(job_of(file, name)) if mentions in s]
    return script


def tick_run(stream: str) -> str:
    """What the run step of a stream's job in tick.yml mentions, to find it by: the
    stream's module and `run`."""
    return f"{stream.replace('-', '_')} run"


def tick_script(stream: str) -> str:
    """The script of the step that runs `stream` in its job of tick.yml."""
    return one_script("tick.yml", stream, tick_run(stream))


def tick_step(stream: str) -> str:
    """The text of the step that runs `stream` in its job of tick.yml."""
    [step] = [s for s in steps(job_of("tick.yml", stream)) if tick_run(stream) in s]
    return step


def step_limit(file: str, job: str, mentions: str) -> int:
    """The `timeout-minutes` of the one step of a job whose text contains `mentions`."""
    [step] = [s for s in steps(job_of(file, job)) if mentions in s]
    found = re.search(r"(?m)^(?: {8}|- )timeout-minutes:\s*(\d+)\s*(?:#.*)?$", step)
    assert found, f"{file}: the step with {mentions!r} has no timeout-minutes"
    return int(found[1])


def step_if(step: str) -> str | None:
    """The `if:` of a step, or None when it has none."""
    found = re.search(r"(?m)^(?: {8}|- )if:\s*(.*?)\s*$", step)
    return found[1] if found else None


def evaluate(condition: str, values: dict[str, object]) -> bool:
    """The `if:` of a job or a step, as Python, for `values`: the value of each status
    function (`always()`, `success()`, `failure()`) and each context value (such as
    `needs.sweep.result`) by its name. A context value that is not given is null, as an
    output that was never set is. A condition with no status function is taken with
    `success() &&` before it, as GitHub takes it."""
    assert re.fullmatch(r"[\w\s().&|=!']+", condition), "only what GitHub evaluates"
    if not re.search(r"\b(?:always|success|failure|cancelled)\(\)", condition):
        condition = f"success() && ({condition})"
    python = condition.replace("&&", " and ").replace("||", " or ")
    python = re.sub(r"\b\w+\(\)", lambda call: repr(values[call[0]]), python)
    python = re.sub(
        r"\b(?:needs|steps)\.[\w.]+", lambda name: repr(values.get(name[0])), python
    )
    return eval(python, {"__builtins__": {}})


# --- stand-ins for uv and gh, and a way to run shell against them ---

STUB = """#!{python}
import json, os, subprocess, sys

name, args = os.path.basename(sys.argv[0]), sys.argv[1:]
log_path = os.environ["STUB_LOG"]
calls = []
if os.path.exists(log_path):
    with open(log_path, encoding="utf-8") as log:
        calls = [json.loads(line) for line in log]
with open(log_path, "a", encoding="utf-8") as log:
    log.write(json.dumps([name, *args]) + "\\n")


def earlier(*command):
    # How many calls of `command` came before this one.
    return sum(1 for call in calls if call[: len(command)] == list(command))


def fail_first(knob, *command):
    # The first calls of `command`, as many as `knob` says, fail as a flaky API does.
    if earlier(*command) < int(os.environ.get(knob, "0")):
        print("gh: HTTP 502", file=sys.stderr)
        sys.exit(1)


def answer(listing):
    if "--jq" in args:  # as gh does with its own jq
        expression = args[args.index("--jq") + 1]
        listing = subprocess.run(
            ["jq", "-r", expression], input=listing, capture_output=True,
            text=True, check=True,
        ).stdout
    sys.stdout.write(listing)
    sys.exit(0)


if name == "gh" and args[:2] == ["issue", "list"]:
    if os.environ.get("STUB_LIST_EXIT", "0") != "0":
        print("gh: HTTP 502", file=sys.stderr)
        sys.exit(int(os.environ["STUB_LIST_EXIT"]))
    answer(os.environ.get("STUB_ISSUES", "[]"))
if name == "gh" and args[:2] == ["run", "list"]:
    fail_first("STUB_RUN_LIST_FAILS", "gh", "run", "list")
    runs = json.loads(os.environ.get("STUB_RUNS", "[]"))
    # A dispatch that reached GitHub is a run that waits: one that succeeded, and one
    # that failed only on the way back, when STUB_FAILED_DISPATCH_LANDS is set.
    failing = int(os.environ.get("STUB_DISPATCH_FAILS", "0"))
    lands = os.environ.get("STUB_FAILED_DISPATCH_LANDS") == "1"
    for index in range(earlier("gh", "workflow", "run")):
        if index >= failing or lands:
            runs.append(dict(databaseId=900 + index, status="queued"))
    answer(json.dumps(runs))
if name == "gh" and args[:2] == ["workflow", "run"]:
    fail_first("STUB_DISPATCH_FAILS", "gh", "workflow", "run")
if name == "gh" and args[:1] == ["api"]:
    # The API's replies by endpoint, as JSON text, and the one endpoint that fails.
    endpoint = next(arg for arg in args if arg.startswith("repos/"))
    if os.environ.get("STUB_API_FAILS") == endpoint:
        print("gh: HTTP 502", file=sys.stderr)
        sys.exit(1)
    answer(json.loads(os.environ.get("STUB_API_REPLIES", "{{}}")).get(endpoint, ""))
sys.exit(int(os.environ.get("STUB_EXIT", "0")))
"""
needs_jq = pytest.mark.skipif(
    shutil.which("jq") is None, reason="the stand-in for gh applies --jq with jq"
)


def run_shell(
    tmp_path: Path, argv: list[str], stubs: tuple[str, ...], **env: str
) -> tuple[subprocess.CompletedProcess[str], list[list[str]]]:
    """Run `argv` with a stand-in for each command of `stubs` first on the PATH, and
    return the process and the calls that the stand-ins logged, as argument lists
    that start with the command's name."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    for name in stubs:
        stub = bin_dir / name
        stub.write_text(STUB.format(python=sys.executable), encoding="utf-8")
        stub.chmod(0o755)
    log = tmp_path / "stub.log"
    environment = {
        "PATH": f"{bin_dir}{os.pathsep}{os.environ['PATH']}",
        "STUB_LOG": str(log),
        "GITHUB_SERVER_URL": "https://github.com",
        "GITHUB_REPOSITORY": "py-observatory/data",
        "GITHUB_RUN_ID": "12345",
        **env,
    }
    done = subprocess.run(
        argv, env=environment, capture_output=True, text=True, check=False
    )
    lines = log.read_text(encoding="utf-8").splitlines() if log.exists() else []
    return done, [json.loads(line) for line in lines]


def run_step(
    tmp_path: Path, script: str, *, stubs: tuple[str, ...] = ("uv",), **env: str
) -> tuple[subprocess.CompletedProcess[str], list[list[str]]]:
    """Run a step's script as the runner does (`bash -e`), with a stand-in for each
    command of `stubs`."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    file = tmp_path / "step.sh"
    file.write_text(script, encoding="utf-8")
    return run_shell(tmp_path, ["bash", "-e", str(file)], stubs, **env)


# --- the workflows as a whole ---


def test_no_gh_release_outside_kit():
    """Releases are made by the kit, which paces its writes, never replaces an asset
    and swaps through `swap-next-` names. A `gh release` call, an action that wraps
    it or a raw API call would bypass all of that."""
    gap = r"(?:\s|\\\n)+"  # blanks, and a backslash that continues the line
    through_gh = [
        rf"\bgh{gap}(?:-\S+{gap})*release\b",
        rf"\bgh{gap}api\b[^\n]*releases?\b",
        r"gh-release",
    ]
    # A curl in a workflow or a script. The tests of a stream name these hosts when
    # they look at what the kit sent to a fake.
    through_the_api = [r"uploads\.github\.com|api\.github\.com/repos/[^\s]*/releases"]
    searched = {
        ".github": through_gh + through_the_api,
        "scripts": through_gh + through_the_api,
        "streams": through_gh,
        "catalog": through_gh,
    }
    skipped = {"__pycache__", ".ruff_cache", ".pytest_cache", ".venv"}
    found = []
    for folder, bypasses in searched.items():
        for path in sorted((ROOT / folder).rglob("*")):
            if not path.is_file() or skipped & set(path.parts):
                continue
            text = path.read_text(encoding="utf-8", errors="replace")
            for pattern in bypasses:
                for hit in re.finditer(pattern, text):
                    number = text.count("\n", 0, hit.start()) + 1
                    found.append(f"{path.relative_to(ROOT)}:{number}: {hit[0]!r}")
    assert not found, "\n".join(found)


def test_every_job_has_timeout():
    jobs = all_jobs()
    assert len(jobs) >= 6
    for file, name, body in jobs:
        value = scalar(body, "timeout-minutes", 4)
        assert value is not None, f"{file}: job {name} has no timeout-minutes"
        assert value.isdigit() and 1 <= int(value) <= 360, f"{file}: {name}: {value}"


def test_job_timeouts_leave_room_after_the_run_budget():
    """A hung run is ended by the limit of its step, which is a few minutes shorter than
    its job's so that the step fails and the failure issue is opened (a job that times
    out may count as cancelled). So the limits that apply are the steps'.

    The registry's tick and sweep both stop starting projects after 15,000 s (250
    minutes), and then only the project in hand, the apply and the publish remain. A
    sweep always runs to its deadline, and so does a tick that has much to read. The
    publish compresses a state of about 2 GB late in the backfill, at 4 MB/s on a Mac
    and slower on a runner. The tick's step of 327 minutes leaves 77 for that, and the
    sweep's of 297 leaves 47.

    The osv-pypi tick stops fetching records after 1,200 s (20 minutes). Its state and
    its open delta are each under the 81 MB that OSV's records add up to, which takes
    a few minutes at most to compress on a runner. Its step of 27 minutes leaves 7 for
    that and for the record in hand, so it needs none of the registry's room.

    The catalog has no budget to pass: it reads what the streams published, builds one
    database and publishes it. Its step of 57 minutes ends it three minutes before its
    job's 60, which is time enough to open the issue.
    """
    assert scalar(job_of("tick.yml", "pypi-registry"), "timeout-minutes", 4) == "330"
    assert scalar(job_of("tick.yml", "osv-pypi"), "timeout-minutes", 4) == "30"
    assert scalar(job_of("tick.yml", "catalog"), "timeout-minutes", 4) == "60"
    assert scalar(job_of("backfill.yml", "sweep"), "timeout-minutes", 4) == "300"
    assert scalar(job_of("smoke.yml", "smoke"), "timeout-minutes", 4) == "15"

    assert step_limit("tick.yml", "pypi-registry", "pypi_registry run") == 327
    assert step_limit("tick.yml", "osv-pypi", "osv_pypi run") == 27
    assert step_limit("tick.yml", "catalog", "catalog run") == 57
    assert "--max-seconds" not in tick_script("catalog")
    assert step_limit("backfill.yml", "sweep", "pypi_registry sweep") == 297
    assert step_limit("smoke.yml", "smoke", "obskit.smoke") == 12

    # The least room, in minutes, that each run leaves after its deadline.
    for file, job, mentions, least in [
        ("tick.yml", "pypi-registry", "pypi_registry run", 45),
        ("tick.yml", "osv-pypi", "osv_pypi run", 5),
        ("backfill.yml", "sweep", "pypi_registry sweep", 45),
    ]:
        budget = re.findall(r"--max-seconds (\d+)", one_script(file, job, mentions))
        assert len(budget) == 1, f"{file}: {job}: --max-seconds must be passed once"
        room = step_limit(file, job, mentions) - int(budget[0]) / 60
        assert room >= least, f"{file}: {job}: {room:.0f} minutes are left"


def test_actions_pinned_by_sha():
    pattern = re.compile(r"\s*(?:-\s+)?uses:\s*(\S+)(?:\s+#\s*(\S+))?\s*")
    for file, text in workflows().items():
        pinned = 0
        for number, line in enumerate(text.splitlines(), 1):
            if "uses:" not in line or line.lstrip().startswith("#"):
                continue
            found = pattern.fullmatch(line)
            where = f"{file}:{number}"
            assert found, f"{where}: cannot read {line.strip()!r}"
            ref, tag = found.groups()
            assert re.fullmatch(r"[\w.-]+/[\w./-]+@[0-9a-f]{40}", ref), (
                f"{where}: {ref} is not pinned by a full commit SHA"
            )
            # Dependabot reads the version from the comment, to update the pin.
            assert tag and re.fullmatch(r"v[0-9]+(?:\.[0-9]+)*", tag), (
                f"{where}: {ref} needs its tag in a trailing comment"
            )
            pinned += 1
        assert pinned, f"{file} uses no action, so the check is not looking at it"


def test_each_action_has_one_pin():
    pins = defaultdict(set)
    for text in workflows().values():
        for ref in re.findall(r"(?m)^\s*(?:-\s+)?uses:\s*(\S+)", text):
            action, _, sha = ref.partition("@")
            pins[action].add(sha)
    assert {"actions/checkout", "astral-sh/setup-uv"} <= set(pins)
    assert all(len(shas) == 1 for shas in pins.values()), dict(pins)


def test_top_level_permissions_are_minimal():
    """The token starts with nothing, or read-only contents, and a job that writes
    says so for itself."""
    for file, text in workflows().items():
        outside, _ = split_jobs(text)
        granted = mapping(outside, "permissions", 0)
        assert granted in ({}, {"contents": "read"}), f"{file}: {granted!r}"
    for file in ("tick.yml", "backfill.yml", "smoke.yml"):
        assert mapping(split_jobs(workflows()[file])[0], "permissions", 0) == {}


def test_only_the_jobs_that_need_a_permission_have_it():
    seen = set()
    for file, name, body in all_jobs():
        granted = mapping(body, "permissions", 4)
        if (file, name) not in PERMISSIONS:  # the job has what the top level gives it
            assert granted is None, f"{file}: {name} has permissions of its own"
            continue
        assert granted == PERMISSIONS[file, name], f"{file}: {name}: {granted!r}"
        seen.add((file, name))
    assert seen == set(PERMISSIONS)
    # The table is what the checks above hold the files to, so hold it to the point of
    # it: `contents: write` for the jobs that publish and `actions: write` for the
    # jobs that dispatch, and for no other.
    writing = {
        job for job, got in PERMISSIONS.items() if got.get("contents") == "write"
    }
    assert writing == PUBLISH_JOBS
    dispatching = {job for job, got in PERMISSIONS.items() if "actions" in got}
    assert dispatching == DISPATCH_JOBS
    dispatches = {
        (file, name) for file, name, body in all_jobs() if "gh workflow" in body
    }
    assert dispatches == DISPATCH_JOBS


def test_registry_jobs_share_concurrency_group():
    """A tick and a sweep both publish pypi-registry-current, so they must never run
    at once, and none may be cancelled part way through a publish."""
    registry = {
        (file, name): body
        for file, name, body in all_jobs()
        if "python -m pypi_registry" in body
    }
    assert set(registry) == {("tick.yml", "pypi-registry"), ("backfill.yml", "sweep")}
    for (file, name), body in registry.items():
        assert mapping(body, "concurrency", 4) == REGISTRY_GROUP, f"{file}: {name}"


def test_the_osv_job_has_its_own_concurrency_group():
    """Only the tick publishes osv-pypi-current, so it must never run twice at once,
    and a run that has begun is never cancelled by a newer one. The group is not the
    registry's: a run of one stream never waits for the other's, nor for a sweep."""
    osv = {
        (file, name): body
        for file, name, body in all_jobs()
        if "python -m osv_pypi" in body
    }
    assert set(osv) == {("tick.yml", "osv-pypi")}
    assert mapping(osv["tick.yml", "osv-pypi"], "concurrency", 4) == OSV_GROUP
    assert OSV_GROUP["group"] != REGISTRY_GROUP["group"]


def test_the_catalog_job_has_its_own_concurrency_group():
    """Only the tick publishes catalog-current, so it must never run twice at once, and
    a run that has begun is never cancelled by a newer one."""
    catalog = {
        (file, name): body
        for file, name, body in all_jobs()
        if "python -m catalog" in body
    }
    assert set(catalog) == {("tick.yml", "catalog")}
    assert mapping(catalog["tick.yml", "catalog"], "concurrency", 4) == CATALOG_GROUP
    assert CATALOG_GROUP["group"] not in {REGISTRY_GROUP["group"], OSV_GROUP["group"]}


def test_the_osv_job_runs_beside_the_registry_job():
    """osv-pypi is the tick's second stream. It is set up as the registry is, with the
    same pinned checkout, which keeps no token, and the same pinned uv. It syncs its own
    package only, publishes with the workflow's token and reports to its own rolling
    issue. Nothing makes it wait for the registry's job or skip it: a registry run that
    takes hours or fails must not hold the other stream back."""
    jobs = split_jobs(workflows()["tick.yml"])[1]
    assert list(jobs) == ["pypi-registry", "osv-pypi", "catalog"]
    registry, osv = jobs["pypi-registry"], jobs["osv-pypi"]
    for job in (registry, osv):
        assert entry(job, "needs", 4) is None
        assert entry(job, "if", 4) is None
    assert (
        scalar(osv, "runs-on", 4) == scalar(registry, "runs-on", 4) == "ubuntu-latest"
    )

    texts = steps(osv)
    in_order = [
        "actions/checkout",
        "astral-sh/setup-uv",
        "uv sync --locked --package osv-pypi",
        "osv_pypi run",
        "rolling-issue.sh osv-pypi failure",
        "rolling-issue.sh osv-pypi success",
    ]
    assert len(texts) == len(in_order)
    for text, mentions in zip(texts, in_order, strict=True):
        assert mentions in text
    assert texts[:2] == steps(registry)[:2], (
        "the registry's checkout and uv, as they are"
    )
    token = re.search(r"(?m)^\s+GITHUB_TOKEN:\s*(.*?)\s*$", texts[3])
    assert token and token[1] == "${{ github.token }}", "the run publishes with it"


def test_smoke_job_has_its_own_concurrency_group():
    smoke = job_of("smoke.yml", "smoke")
    assert mapping(smoke, "concurrency", 4) == {
        "group": "stream-kit-smoke",
        "cancel-in-progress": "false",
    }


def test_the_catalog_job_runs_after_osv_whatever_its_outcome():
    """The catalog waits for osv-pypi, which takes half an hour at most, and runs
    whether or not it succeeded: a stream that failed is read from its last publish.
    It does not wait for the registry's job, which can queue for hours behind the sweep,
    and so reads the registry's last publish. The step after the build keeps the
    scheduled runs enabled, and runs whether or not the build failed. It comes before
    the steps of the rolling issue, so that a keepalive that fails opens the issue
    too, and is not left to fail after the issue was closed."""
    catalog = job_of("tick.yml", "catalog")
    registry = job_of("tick.yml", "pypi-registry")
    assert scalar(catalog, "needs", 4) == "osv-pypi"
    assert scalar(catalog, "if", 4) == "${{ !cancelled() }}"
    assert scalar(catalog, "runs-on", 4) == "ubuntu-latest"
    assert mapping(catalog, "permissions", 4) == PERMISSIONS["tick.yml", "catalog"]
    assert mapping(catalog, "concurrency", 4) == CATALOG_GROUP

    texts = steps(catalog)
    in_order = [
        "actions/checkout",
        "astral-sh/setup-uv",
        "uv sync --locked --package catalog",
        "catalog run",
        "scripts/keepalive.sh",
        "rolling-issue.sh catalog failure",
        "rolling-issue.sh catalog success",
    ]
    assert len(texts) == len(in_order)
    for text, mentions in zip(texts, in_order, strict=True):
        assert mentions in text
    assert texts[:2] == steps(registry)[:2], (
        "the registry's checkout and uv, as they are"
    )
    assert [step_if(text) for text in texts[3:]] == [
        None,
        "${{ !cancelled() }}",
        "failure()",
        "success()",
    ]
    for text in (texts[3], texts[4]):
        token = re.search(r"(?m)^\s+(?:GITHUB|GH)_TOKEN:\s*(.*?)\s*$", text)
        assert token and token[1] == "${{ github.token }}", text


def test_only_the_stream_jobs_join_a_concurrency_group():
    """A job that queues in the group replaces the one that is waiting there, so the
    jobs that only dispatch or report stay out of it."""
    in_a_group = {
        (file, name) for file, name, body in all_jobs() if entry(body, "concurrency", 4)
    }
    assert in_a_group == PUBLISH_JOBS


def test_no_workflow_has_a_group_of_its_own_for_all_its_jobs():
    """A group at the top of a workflow holds every job of it, including any job
    that is meant to be outside."""
    for file, text in workflows().items():
        assert entry(split_jobs(text)[0], "concurrency", 0) is None, file


def test_backfill_redispatches_from_a_job_outside_the_group():
    """A group holds one running job and one pending one, and a job that queues
    cancels the pending one. A sweep that dispatched the next sweep from inside the
    group would cancel the tick that waits behind it, for as long as the sweeps go
    on. So the dispatch is a job of its own, after the sweep job has left the group."""
    jobs = split_jobs(workflows()["backfill.yml"])[1]
    assert list(jobs) == ["sweep", "redispatch", "report"]
    sweep, redispatch = jobs["sweep"], jobs["redispatch"]

    assert "gh workflow run" not in sweep
    assert entry(redispatch, "concurrency", 4) is None
    assert scalar(redispatch, "needs", 4) == "sweep"
    assert scalar(redispatch, "if", 4) == "needs.sweep.outputs.continue == 'true'"
    assert scalar(sweep, "outputs", 4) == ""
    assert scalar(sweep, "continue", 6) == "${{ steps.sweep.outputs.continue }}"
    script = one_script("backfill.yml", "redispatch", "gh workflow run")
    assert script.count("gh workflow run") == 1
    assert "gh workflow run backfill.yml -f task=sweep" in script
    # Every try looks for another backfill run first, and knows this one by its id.
    assert script.count("gh run list") == 1
    assert script.index("gh run list") < script.index("gh workflow run")
    assert '"$GITHUB_RUN_ID"' in script
    token = re.search(r"(?m)^\s+GH_TOKEN:\s*(.*?)\s*$", redispatch)
    assert token and token[1] == "${{ github.token }}"
    assert re.search(r"(?m)^\s+GH_REPO:", redispatch), "no checkout, so no repository"
    assert not re.search(r"uses:\s*actions/checkout", redispatch)


def test_a_sweep_says_in_its_first_step_that_it_started(tmp_path):
    """A sweep that a newer run replaced while it waited for the group runs no step, so
    it never says that it started. That is how the report job tells it from a sweep
    that was cancelled while it ran."""
    sweep = job_of("backfill.yml", "sweep")
    first = steps(sweep)[0]
    assert re.search(r"(?m)^(?: {8}|- )id:\s*started\s*$", first)
    assert scalar(sweep, "started", 6) == "${{ steps.started.outputs.started }}"
    [script] = run_scripts(first)
    output = tmp_path / "output"
    output.write_text("")

    done, calls = run_step(tmp_path / "run", script, GITHUB_OUTPUT=str(output))

    assert done.returncode == 0, done.stderr
    assert output.read_text() == "started=true\n"
    assert calls == []


def test_nothing_dispatches_from_inside_a_concurrency_group():
    for file, name, body in all_jobs():
        if "gh workflow run" in body:
            assert entry(body, "concurrency", 4) is None, f"{file}: {name}"


DISPATCH = ["gh", "workflow", "run", "backfill.yml", "-f", "task=sweep"]
LISTING = [
    *["gh", "run", "list", "--repo", "py-observatory/data", "--workflow"],
    *["backfill.yml", "--limit", "20", "--json", "databaseId,status"],
    *["--jq", '.[] | select(.status != "completed") | .databaseId'],
]
WAIT = ["sleep", "20"]
# The backfill runs that GitHub lists when there is no other chain: this one (its id is
# GITHUB_RUN_ID in run_shell), and two that have completed.
ALONE = [
    {"databaseId": 12345, "status": "in_progress"},
    {"databaseId": 12001, "status": "completed"},
    {"databaseId": 12002, "status": "completed"},
]


def dispatch(
    tmp_path: Path, job: tuple[str, str], runs: list[dict] = ALONE, **env: str
) -> tuple[subprocess.CompletedProcess[str], list[list[str]]]:
    """The dispatch script of `job`, run with stand-ins for gh and sleep, where GitHub
    lists `runs` as the backfill's runs, and the dispatches that reached it."""
    script = one_script(*job, "gh workflow run")
    return run_step(
        tmp_path, script, stubs=("gh", "sleep"), STUB_RUNS=json.dumps(runs), **env
    )


@needs_jq
@pytest.mark.parametrize("job", sorted(DISPATCH_JOBS), ids=lambda job: job[1])
def test_a_dispatch_is_tried_up_to_three_times(tmp_path, job):
    """A dispatch that fails once must not end a chain of some 78 sweeps. Each try
    looks for another backfill run first, and finds none but this one."""
    # How many dispatches fail first, then the exit code, the tries and the waits.
    cases = {0: (0, 1, 0), 1: (0, 2, 1), 2: (0, 3, 2), 3: (1, 3, 2), 9: (1, 3, 2)}
    for failing, (status, tries, waits) in cases.items():
        done, calls = dispatch(
            tmp_path / str(failing), job, STUB_DISPATCH_FAILS=str(failing)
        )
        assert done.returncode == status, (failing, done.stderr)
        gh = [call for call in calls if call[0] == "gh"]
        assert gh == [LISTING, DISPATCH] * tries, failing
        sleeps = [call for call in calls if call[0] == "sleep"]
        assert len(sleeps) == waits, (failing, "none after the last try")
        assert all(len(call) == 2 and 0 < int(call[1]) <= 60 for call in sleeps), sleeps


@needs_jq
@pytest.mark.parametrize("job", sorted(DISPATCH_JOBS), ids=lambda job: job[1])
def test_a_dispatch_leaves_the_chain_to_another_run(tmp_path, job):
    """Another backfill run that has not completed carries the chain on, so nothing is
    dispatched, which would start a second chain, and the step succeeds."""
    for status in ("queued", "pending", "waiting", "in_progress"):
        other = {"databaseId": 12346, "status": status}

        done, calls = dispatch(tmp_path / status, job, [*ALONE, other])

        assert done.returncode == 0, (status, done.stderr)
        assert calls == [LISTING], status
        assert "12346" in done.stdout, status


@needs_jq
@pytest.mark.parametrize("job", sorted(DISPATCH_JOBS), ids=lambda job: job[1])
def test_a_dispatch_that_github_took_is_never_made_twice(tmp_path, job):
    """gh says that the dispatch failed, but GitHub took it. The next try finds the run
    that it made, and leaves the chain to it."""
    done, calls = dispatch(
        tmp_path, job, STUB_DISPATCH_FAILS="1", STUB_FAILED_DISPATCH_LANDS="1"
    )

    assert done.returncode == 0, done.stderr
    assert calls == [LISTING, DISPATCH, WAIT, LISTING]


@needs_jq
@pytest.mark.parametrize("job", sorted(DISPATCH_JOBS), ids=lambda job: job[1])
def test_a_failed_listing_is_a_failed_try(tmp_path, job):
    """A listing that fails says nothing about other runs, so it is never taken for "no
    other run": it is a try that failed."""
    done, calls = dispatch(tmp_path / "once", job, STUB_RUN_LIST_FAILS="1")

    assert done.returncode == 0, done.stderr
    assert calls == [LISTING, WAIT, LISTING, DISPATCH]

    done, calls = dispatch(tmp_path / "always", job, STUB_RUN_LIST_FAILS="9")

    assert done.returncode == 1
    assert calls == [LISTING, WAIT, LISTING, WAIT, LISTING], "and no dispatch"


@pytest.mark.parametrize("job", sorted(DISPATCH_JOBS), ids=lambda job: job[1])
def test_a_dispatch_without_its_run_id_fails(tmp_path, job):
    """Without its own id, the step would take its own run for another one and end the
    chain without a word."""
    done, calls = dispatch(tmp_path, job, GITHUB_RUN_ID="")

    assert done.returncode != 0
    assert calls == []


def test_a_hung_dispatch_fails_its_step_before_its_job_times_out():
    """The report job runs for a redispatch that failed, and a job that times out may
    count as cancelled."""
    job = int(scalar(job_of("backfill.yml", "redispatch"), "timeout-minutes", 4) or 0)
    assert step_limit("backfill.yml", "redispatch", "gh workflow run") < job


def test_report_steps_give_up_before_the_report_job_does():
    """A dispatch that hangs fails its step, and the step that opens the issue runs
    after it, so every step of the report job has a limit, and together they leave
    room within the job's."""
    job = int(scalar(job_of(*REPORT_JOB), "timeout-minutes", 4) or 0)
    report = steps(job_of(*REPORT_JOB))
    mentions = ["actions/checkout", "gh workflow run", "rolling-issue.sh"]
    assert all(any(text in step for step in report) for text in mentions)
    assert len(report) == len(mentions)
    limits = [step_limit(*REPORT_JOB, text) for text in mentions]
    assert all(0 < limit < job for limit in limits), limits
    assert sum(limits) < job, limits


def test_a_failed_close_of_the_issue_cannot_skip_the_redispatch():
    """The next sweep is dispatched only when the sweep job succeeds. After a sweep that
    exited with 3, the step that closes the backfill's issue runs, and if it failed once
    (an API error) the job would fail. The dispatch would be skipped, and so would the
    report, because the sweep job failed and was not cancelled: a red run, no issue and
    no next sweep. So no step after the sweep step may fail the job, except the one
    that runs when the sweep step has failed, and the job with it."""
    after_the_sweep = steps(job_of("backfill.yml", "sweep"))
    [index] = [i for i, step in enumerate(after_the_sweep) if "id: sweep" in step]
    sweep, after_the_sweep = after_the_sweep[index], after_the_sweep[index + 1 :]
    assert "continue-on-error" not in sweep, "the sweep itself must be able to fail"
    assert len(after_the_sweep) == 2, "the report of a failure, and the close"
    for step in after_the_sweep:
        if re.search(r"(?m)^\s*(?:- )?if:\s*failure\(\)\s*$", step):
            continue
        assert re.search(r"(?m)^\s+continue-on-error:\s*true\s*$", step), step


def test_report_covers_the_ways_the_chain_stops_silently():
    """A sweep that fails reports through its own step. These are the other ways in which
    the chain ends without a sweep saying so: the next sweep cannot be dispatched, a
    sweep is cancelled while it waits for the group (a run that queues replaces the one
    that waits) and a sweep is cancelled while it runs, as one that hits the timeout of
    its job may be. A sweep replaced while it waited is dispatched again, as the
    redispatch job dispatches the next one, and the issue opens if that fails."""
    report = job_of(*REPORT_JOB)
    assert sequence(report, "needs", 4) == ["sweep", "redispatch"]
    assert scalar(report, "if", 4) == (
        "always() && (needs.sweep.result == 'cancelled' "
        "|| needs.redispatch.result == 'failure')"
    )
    assert mapping(report, "permissions", 4) == PERMISSIONS[REPORT_JOB]
    assert scalar(report, "timeout-minutes", 4) == "5"
    assert entry(report, "concurrency", 4) is None, "a job that queues replaces a tick"
    checkout, dispatching, opening = steps(report)
    assert "actions/checkout" in checkout
    assert re.search(r"(?m)^\s+persist-credentials:\s*false\s*$", checkout)
    assert re.search(r"(?m)^(?: {8}|- )id:\s*dispatch\s*$", dispatching)
    tries = [
        one_script(*job, "gh workflow run").rstrip() for job in sorted(DISPATCH_JOBS)
    ]
    assert tries[0] == tries[1], "the same tries as the redispatch job"
    assert re.search(r"(?m)^\s+GH_REPO:\s*\$\{\{ github\.repository \}\}$", dispatching)
    assert one_script(*REPORT_JOB, "rolling-issue.sh") == (
        "scripts/rolling-issue.sh pypi-registry-backfill failure"
    )
    assert "rolling-issue.sh" in opening
    for step in (dispatching, opening):
        token = re.search(r"(?m)^\s+GH_TOKEN:\s*(.*?)\s*$", step)
        assert token and token[1] == "${{ github.token }}", step


def report_does(
    sweep: str, started: str | None, redispatch: str, *, dispatch_works: bool = True
) -> tuple[bool, bool]:
    """What the report job does, as (dispatches a sweep, opens the issue), for the
    results of the sweep and redispatch jobs, the sweep's `started` output (None when
    it was never set) and whether a dispatch works. A job that does not run does
    neither. Its checkout succeeds."""
    report = job_of(*REPORT_JOB)
    _, dispatching, opening = steps(report)
    needs = {
        "needs.sweep.result": sweep,
        "needs.sweep.outputs.started": started,
        "needs.redispatch.result": redispatch,
    }
    if not evaluate(scalar(report, "if", 4) or "", {**needs, "always()": True}):
        return False, False
    before = {"success()": True, "failure()": False}
    dispatches = evaluate(step_if(dispatching) or "", {**needs, **before})
    outcome = ("success" if dispatch_works else "failure") if dispatches else "skipped"
    failed = outcome == "failure"
    after = {"success()": not failed, "failure()": failed}
    opens = evaluate(
        step_if(opening) or "", {**needs, **after, "steps.dispatch.outcome": outcome}
    )
    return dispatches, opens


def test_report_runs_for_a_chain_that_stopped_without_a_report_of_its_own():
    # (the result of the sweep job, its `started`, the result of the redispatch job):
    # (does the report dispatch a sweep, does it open the issue)
    nothing = (False, False)
    assert report_does("success", "true", "success") == nothing, "next one dispatched"
    assert report_does("success", "true", "skipped") == nothing, "done, and all is well"
    assert report_does("failure", "true", "skipped") == nothing, "it reports for itself"
    assert report_does("success", "true", "failure") == (False, True), "not dispatched"
    # Cancelled while it ran: by its job's timeout, or by hand.
    assert report_does("cancelled", "true", "skipped") == (False, True)
    # Replaced while it waited, so it never started: it is dispatched again, and the
    # issue opens only if every try fails. An output that was never set is null, or
    # an empty string.
    for unset in (None, ""):
        assert report_does("cancelled", unset, "skipped") == (True, False)
        failing = report_does("cancelled", unset, "skipped", dispatch_works=False)
        assert failing == (True, True)


def test_sweep_exit_codes(tmp_path):
    """Exit 3 means "run again": the job still succeeds, and says so in its output.
    Any other failure fails the job, with the code that it gave."""
    script = one_script("backfill.yml", "sweep", "pypi_registry sweep")
    expectations = {"0": (0, False), "3": (0, True), "1": (1, False), "2": (2, False)}
    for code, (status, again) in expectations.items():
        output = tmp_path / f"output-{code}"
        output.write_text("")
        done, calls = run_step(
            tmp_path / code, script, STUB_EXIT=code, GITHUB_OUTPUT=str(output)
        )
        assert done.returncode == status, (code, done.stderr)
        assert output.read_text() == ("continue=true\n" if again else ""), code
        assert calls == [SWEEP_COMMAND], code


def test_sweep_passes_its_time_budget(tmp_path):
    """15,000 s is 250 minutes, which leaves the rest of the step to publish."""
    script = one_script("backfill.yml", "sweep", "pypi_registry sweep")
    output = tmp_path / "output"
    output.write_text("")
    _, calls = run_step(tmp_path, script, GITHUB_OUTPUT=str(output))
    [call] = calls
    assert call[call.index("sweep") :] == ["sweep", "--max-seconds", "15000"]


@pytest.mark.parametrize("stream", sorted(TICK_COMMANDS))
def test_tick_passes_its_time_budget(tmp_path, stream):
    """The registry's 15,000 s is 250 minutes, which leaves the rest of its step to
    publish. osv-pypi's 1,200 s is 20 of the 27 minutes of its step."""
    _, calls = run_step(tmp_path, tick_script(stream))
    assert calls == [TICK_COMMANDS[stream]]


def tick_inputs(
    bootstrap: str = "none", override_for: str = "none", reason: str = ""
) -> dict[str, str]:
    """What the inputs of a dispatch of tick.yml come to in the environment of a step.
    The defaults are those of the dispatch: `none` for the two choices."""
    return {
        "BOOTSTRAP": bootstrap,
        "OVERRIDE_FOR": override_for,
        "OVERRIDE_REASON": reason,
    }


@pytest.mark.parametrize("stream", sorted(TICK_COMMANDS))
def test_tick_passes_bootstrap_and_a_reason_only_for_its_own_stream(tmp_path, stream):
    """`bootstrap` and `override_for` each name the one stream that they are for, so a
    dispatch meant for one stream can never bootstrap the other, or publish it past its
    gates. A job passes `--bootstrap`, and the reason, only when its own stream is the
    one that is named, and a reason that names no stream overrides nothing.

    A scheduled run has every input as an empty string, and a dispatch that leaves them
    alone has `none` for the two choices. Neither may bootstrap or override a stream.
    """
    script = tick_script(stream)
    other = next(name for name in TICK_COMMANDS if name != stream)
    why = "PyPI removed 2,000 names"
    odd = 'it\'s "odd" $HOME; ls'
    cases = [
        # (what the inputs hold, the arguments after the command)
        ({}, []),  # no variable is set at all
        (tick_inputs("", "", ""), []),  # a scheduled run
        (tick_inputs(), []),  # a dispatch that leaves the inputs alone
        (tick_inputs(bootstrap=stream), ["--bootstrap"]),
        (tick_inputs(bootstrap=other), []),
        (tick_inputs(bootstrap="true"), []),  # what the boolean input once said
        (tick_inputs(override_for=stream, reason=why), ["--override-reason", why]),
        (tick_inputs(override_for=other, reason=why), []),
        (tick_inputs(reason=why), []),  # a reason that names no stream
        (tick_inputs("", "", why), []),  # and one that a scheduled run could not have
        (tick_inputs(override_for=stream), []),  # a stream, and no reason
        (
            tick_inputs(bootstrap=stream, override_for=other, reason=why),
            ["--bootstrap"],
        ),
        (
            tick_inputs(bootstrap=other, override_for=stream, reason=why),
            ["--override-reason", why],
        ),
        (
            tick_inputs(bootstrap=stream, override_for=stream, reason=odd),
            ["--bootstrap", "--override-reason", odd],
        ),
    ]
    for number, (inputs, extra) in enumerate(cases):
        done, calls = run_step(tmp_path / str(number), script, **inputs)
        assert done.returncode == 0, done.stderr
        assert calls == [[*TICK_COMMANDS[stream], *extra]], inputs


@pytest.mark.parametrize("stream", sorted(TICK_COMMANDS))
@pytest.mark.parametrize("code", [1, 2])
def test_a_failing_run_fails_the_tick_step(tmp_path, stream, code):
    """Any code that the run exits with fails the step, so that the step that opens the
    issue runs. osv-pypi exits with 2 after it has published, when some records did not
    parse, and that opens the issue too."""
    done, _ = run_step(tmp_path, tick_script(stream), STUB_EXIT=str(code))
    assert done.returncode == code


def test_smoke_passes_bootstrap_only_when_asked(tmp_path):
    script = one_script("smoke.yml", "smoke", "obskit.smoke")
    for number, (given, extra) in enumerate(
        [
            ({}, []),
            ({"BOOTSTRAP": "false"}, []),
            ({"BOOTSTRAP": "true"}, ["--bootstrap"]),
        ]
    ):
        done, calls = run_step(tmp_path / str(number), script, **given)
        assert done.returncode == 0, done.stderr
        assert calls == [["uv", "run", "python", "-m", "obskit.smoke", *extra]]


def test_inputs_reach_scripts_through_the_environment():
    """`${{ }}` inside a script is pasted into the shell before it runs, so an input
    holding `"; curl ...` would run. The expression goes in `env:` and the script reads
    the variable. The tests that run the scripts set those variables themselves, so
    this is where the step of each stream is held to getting each of the tick's three
    inputs under the name that its script reads."""
    scripts = [s for _, _, body in all_jobs() for s in run_scripts(body)]
    assert len(scripts) >= 10
    assert [s for s in scripts if "${{" in s] == []

    wanted = {
        "BOOTSTRAP": "${{ inputs.bootstrap }}",
        "OVERRIDE_FOR": "${{ inputs.override_for }}",
        "OVERRIDE_REASON": "${{ inputs.override_reason }}",
    }
    for stream in TICK_COMMANDS:
        env = dict(
            re.findall(r"(?m)^\s+(\w+):\s*(\$\{\{.*\}\})\s*$", tick_step(stream))
        )
        assert {name: env.get(name) for name in wanted} == wanted, stream


def test_triggers():
    tick = split_jobs(workflows()["tick.yml"])[0]
    assert re.search(r"(?m)^    - cron: [\"']23 \*/6 \* \* \*[\"']\s*$", tick)
    inputs = entry(entry(tick, "workflow_dispatch", 2)[1], "inputs", 4)[1]
    assert sorted(re.findall(r"(?m)^ {6}(\w+):", inputs)) == [
        *["bootstrap", "override_for", "override_reason"]
    ]
    # Two choices, each of which names the one stream that it is for: `none` leaves both
    # streams as they are, and a choice has no other value.
    jobs = list(split_jobs(workflows()["tick.yml"])[1])
    for name in ("bootstrap", "override_for"):
        choice = entry(inputs, name, 6)[1]
        assert scalar(choice, "type", 8) == "choice", name
        assert sequence(choice, "options", 8) == [
            *["none", "pypi-registry", "osv-pypi", "catalog"]
        ]
        assert scalar(choice, "default", 8) == "none", name
        # A job of the tick that no option names could never be bootstrapped, and an
        # option that no job has would bootstrap nothing.
        assert sequence(choice, "options", 8)[1:] == jobs, name
    reason = entry(inputs, "override_reason", 6)[1]
    assert scalar(reason, "type", 8) == "string"
    assert scalar(reason, "default", 8) in ('""', "''")

    smoke = split_jobs(workflows()["smoke.yml"])[0]
    assert re.search(r"(?m)^    - cron: [\"']41 4 \* \* 1[\"']\s*$", smoke)
    inputs = entry(entry(smoke, "workflow_dispatch", 2)[1], "inputs", 4)[1]
    assert scalar(entry(inputs, "bootstrap", 6)[1], "type", 8) == "boolean"
    assert scalar(entry(inputs, "bootstrap", 6)[1], "default", 8) == "false"

    backfill = split_jobs(workflows()["backfill.yml"])[0]
    assert entry(backfill, "schedule", 2) is None  # a sweep is only ever dispatched
    task = entry(
        entry(entry(backfill, "workflow_dispatch", 2)[1], "inputs", 4)[1], "task", 6
    )
    assert scalar(task[1], "type", 8) == "choice"
    assert re.findall(r"(?m)^\s+- (\S+)", entry(task[1], "options", 8)[1]) == ["sweep"]


def test_every_stream_job_reports_to_the_rolling_issue():
    """A run that fails opens (or adds to) the issue of its stream, and one that
    succeeds closes it. Each stream of the tick has an issue of its own, and so has the
    backfill: a good osv-pypi run says nothing about the registry's, a good tick says
    nothing about a sweep chain that stopped, and a good sweep says nothing about
    whether the daily run is well. A sweep that exits with 0 or 3 is a success."""
    expected = {
        ("tick.yml", "pypi-registry"): ("pypi-registry", ["failure", "success"]),
        ("tick.yml", "osv-pypi"): ("osv-pypi", ["failure", "success"]),
        ("tick.yml", "catalog"): ("catalog", ["failure", "success"]),
        ("backfill.yml", "sweep"): ("pypi-registry-backfill", ["failure", "success"]),
        ("smoke.yml", "smoke"): ("kit-smoke", ["failure", "success"]),
    }
    for (file, name), (stream, outcomes) in expected.items():
        reported = []
        for step in steps(job_of(file, name)):
            found = re.search(
                r"(?m)^\s*(?:- )?run:\s*scripts/rolling-issue\.sh (\S+) (\S+)\s*$", step
            )
            if not found:
                continue
            assert found[1] == stream, f"{file}: {name}: {found[1]}"
            condition = re.search(r"(?m)^\s*(?:- )?if:\s*(\S+)\s*$", step)
            assert condition and condition[1] == f"{found[2]}()", (file, name, step)
            token = re.search(r"(?m)^\s+GH_TOKEN:\s*(.*?)\s*$", step)
            assert token and token[1] == "${{ github.token }}", (file, name, step)
            reported.append(found[2])
        assert reported == outcomes, f"{file}: {name}"


def test_checkouts_do_not_keep_the_token():
    """Nothing here pushes, so no checkout leaves the job's token in .git/config, not
    even the test job's read-only one, which a pull request's code could read."""
    checked_out = set()
    for file, name, body in all_jobs():
        for step in steps(body):
            if "actions/checkout" in step:
                assert re.search(r"(?m)^\s+persist-credentials:\s*false\s*$", step), (
                    f"{file}: {name}"
                )
                checked_out.add((file, name))
    # So that the loop above looks at something: every job that runs the repository's
    # code or scripts checks it out.
    assert checked_out == PUBLISH_JOBS | {REPORT_JOB, ("test.yml", "test")}


def test_the_build_is_pinned():
    """A month close that fails after adding the month's part is run again, and the
    retry must build the same bytes, which takes the same zstd and SQLite. Those come
    with the Python that uv installs, so `.python-version` names a patch release, and
    every setup-uv step pins uv to one version, the same everywhere."""
    python = (ROOT / ".python-version").read_text(encoding="utf-8")
    assert re.fullmatch(r"3\.[0-9]+\.[0-9]+\n", python), python
    pins = {}
    for file, name, body in all_jobs():
        for step in steps(body):
            if "astral-sh/setup-uv" in step:
                pinned = mapping(step, "with", 8)
                assert isinstance(pinned, dict), f"{file}: {name}: no `with`"
                assert "version" in pinned, f"{file}: {name}: uv is not pinned"
                pins[file, name] = pinned["version"]
    assert {file for file, _ in pins} == set(EXPECTED), "each workflow sets up uv"
    [version] = set(pins.values())
    assert re.fullmatch(r'"[0-9]+\.[0-9]+\.[0-9]+"', version), version


def test_workflows_run_the_locked_environment():
    for file, name, body in all_jobs():
        if (file, name) in PUBLISH_JOBS:
            assert "uv sync --locked" in "\n".join(run_scripts(body)), f"{file}: {name}"


# --- scripts/rolling-issue.sh ---


def test_rolling_issue_script_is_executable_and_strict():
    text = SCRIPT.read_text(encoding="utf-8")
    assert text.startswith("#!/usr/bin/env bash\n")
    assert os.access(SCRIPT, os.X_OK), "the workflows run it directly"
    assert re.search(r"(?m)^set -euo pipefail$", text)
    assert (
        'RUN_URL="$GITHUB_SERVER_URL/$GITHUB_REPOSITORY/actions/runs/$GITHUB_RUN_ID"'
        in text
    )


def test_rolling_issue_assigns_owner():
    text = SCRIPT.read_text(encoding="utf-8")
    assert len(re.findall(r"--assignee coatless\b", text)) == 1
    assert "gh label create" in text
    assert "--force" in text


def rolling_issue(tmp_path: Path, issues: list[dict], *args: str, **env: str):
    return run_shell(
        tmp_path,
        ["bash", str(SCRIPT), *args],
        ("gh",),
        STUB_ISSUES=json.dumps(issues),
        **env,
    )


def gh_calls(calls: list[list[str]], *words: str) -> list[list[str]]:
    return [call for call in calls if call[1 : 1 + len(words)] == list(words)]


def flag(call: list[str], name: str) -> str:
    return call[call.index(name) + 1]


def names_the_repository(calls: list[list[str]]) -> bool:
    """Whether every call says which repository it is for, as the script must so that
    it works without a checkout."""
    return bool(calls) and all(
        flag(call, "--repo") == "py-observatory/data" for call in calls
    )


TITLE = "[pypi-registry] scheduled run failed"


@needs_jq
def test_rolling_issue_failure_opens_one_issue_for_the_owner(tmp_path):
    done, calls = rolling_issue(tmp_path, [], "pypi-registry", "failure")

    assert done.returncode == 0, done.stderr
    [created] = gh_calls(calls, "issue", "create")
    assert flag(created, "--title") == TITLE
    assert flag(created, "--assignee") == "coatless"
    assert flag(created, "--label") == "stream:pypi-registry"
    assert RUN_URL in flag(created, "--body")
    [label] = gh_calls(calls, "label", "create")
    assert label[3] == "stream:pypi-registry"
    assert "--force" in label  # the label may be there already
    assert calls.index(label) < calls.index(created), "the label must exist first"
    assert not gh_calls(calls, "issue", "comment")
    assert not gh_calls(calls, "issue", "close")
    assert names_the_repository(calls)


@needs_jq
def test_rolling_issue_failure_comments_on_the_open_issue(tmp_path):
    open_issue = [{"number": 7, "title": TITLE}]

    done, calls = rolling_issue(tmp_path, open_issue, "pypi-registry", "failure")

    assert done.returncode == 0, done.stderr
    [comment] = gh_calls(calls, "issue", "comment")
    assert comment[3] == "7"
    assert RUN_URL in flag(comment, "--body")
    assert not gh_calls(calls, "issue", "create")
    assert not gh_calls(calls, "label", "create")
    assert names_the_repository(calls)


@needs_jq
def test_rolling_issue_success_closes_the_open_issue_with_the_run_url(tmp_path):
    open_issue = [{"number": 7, "title": TITLE}]

    done, calls = rolling_issue(tmp_path, open_issue, "pypi-registry", "success")

    assert done.returncode == 0, done.stderr
    [closed] = gh_calls(calls, "issue", "close")
    assert closed[3] == "7"
    assert flag(closed, "--comment") == f"Recovered: {RUN_URL}"
    assert not gh_calls(calls, "issue", "create")
    assert names_the_repository(calls)


@needs_jq
def test_rolling_issue_success_with_no_open_issue_changes_nothing(tmp_path):
    done, calls = rolling_issue(tmp_path, [], "kit-smoke", "success")

    assert done.returncode == 0, done.stderr
    assert [call[1:3] for call in calls] == [["issue", "list"]]


@needs_jq
def test_rolling_issue_uses_the_stream_in_the_title_and_the_label(tmp_path):
    done, calls = rolling_issue(tmp_path, [], "kit-smoke", "failure")

    assert done.returncode == 0, done.stderr
    [created] = gh_calls(calls, "issue", "create")
    assert flag(created, "--title") == "[kit-smoke] scheduled run failed"
    assert flag(created, "--label") == "stream:kit-smoke"
    [listed] = gh_calls(calls, "issue", "list")
    assert flag(listed, "--label") == "stream:kit-smoke"
    assert flag(listed, "--state") == "open"


@needs_jq
def test_rolling_issues_of_the_tick_and_the_backfill_are_separate(tmp_path):
    """The tick's issue is open, and a backfill that fails is not that: it opens its own,
    and a backfill that succeeds leaves the tick's alone."""
    ticks = [{"number": 7, "title": TITLE}]

    failed, failed_calls = rolling_issue(
        tmp_path / "failure", ticks, "pypi-registry-backfill", "failure"
    )
    passed, passed_calls = rolling_issue(
        tmp_path / "success", ticks, "pypi-registry-backfill", "success"
    )

    assert failed.returncode == 0 and passed.returncode == 0
    [created] = gh_calls(failed_calls, "issue", "create")
    assert flag(created, "--title") == "[pypi-registry-backfill] scheduled run failed"
    assert flag(created, "--label") == "stream:pypi-registry-backfill"
    assert not gh_calls(failed_calls, "issue", "comment")
    assert not gh_calls(passed_calls, "issue", "close")


@needs_jq
def test_rolling_issue_leaves_other_issues_with_the_label_alone(tmp_path):
    """The label is free for people to use, so an issue that carries it is the
    stream's rolling issue only if its title says so."""
    someone_elses = [{"number": 3, "title": "Please add the field yanked_at"}]

    failed, failed_calls = rolling_issue(
        tmp_path / "failure", someone_elses, "pypi-registry", "failure"
    )
    passed, passed_calls = rolling_issue(
        tmp_path / "success", someone_elses, "pypi-registry", "success"
    )

    assert failed.returncode == 0 and passed.returncode == 0
    assert len(gh_calls(failed_calls, "issue", "create")) == 1
    assert not gh_calls(failed_calls, "issue", "comment")
    assert not gh_calls(passed_calls, "issue", "close")


@needs_jq
def test_rolling_issue_picks_its_issue_among_others(tmp_path):
    issues = [
        {"number": 9, "title": "Please add the field yanked_at"},
        {"number": 5, "title": TITLE},
    ]

    done, calls = rolling_issue(tmp_path, issues, "pypi-registry", "success")

    assert done.returncode == 0, done.stderr
    [closed] = gh_calls(calls, "issue", "close")
    assert closed[3] == "5"


@needs_jq
@pytest.mark.parametrize("outcome", ["failure", "success"])
def test_rolling_issue_does_not_read_a_failed_listing_as_no_issue(tmp_path, outcome):
    """If gh cannot list the issues, an issue may well be open: opening another, or
    leaving the open one open as if nothing had happened, would be wrong."""
    done, calls = rolling_issue(
        tmp_path, [], "pypi-registry", outcome, STUB_LIST_EXIT="1"
    )

    assert done.returncode != 0
    assert [call[1:3] for call in calls] == [["issue", "list"]]


@pytest.mark.parametrize(
    "args",
    [(), ("pypi-registry",), ("pypi-registry", "recovered"), ("a", "failure", "x")],
)
def test_rolling_issue_refuses_other_arguments(tmp_path, args):
    done, calls = rolling_issue(tmp_path, [], *args)

    assert done.returncode == 2
    assert "usage" in done.stderr
    assert calls == []


# --- .github/dependabot.yml ---


# --- scripts/keepalive.sh ---

HEAD_SHA = "a" * 40
TREE_SHA = "b" * 40
NEW_SHA = "c" * 40
REPO = "repos/py-observatory/data"
HEAD_COMMIT = f"{REPO}/commits/main"
TREE_READ = f"{REPO}/git/commits/{HEAD_SHA}"
COMMIT_CREATE = f"{REPO}/git/commits"
MAIN_REF = f"{REPO}/git/refs/heads/main"


def run_keepalive(tmp_path: Path, age_days: int, **env: str):
    """Run the script with a stand-in for gh whose main is `age_days` days and an hour
    old, with the head, tree and new commit named above."""
    made = datetime.now(UTC) - timedelta(days=age_days, hours=1)
    replies = {
        HEAD_COMMIT: {
            "sha": HEAD_SHA,
            "commit": {"committer": {"date": made.strftime("%Y-%m-%dT%H:%M:%SZ")}},
        },
        TREE_READ: {"tree": {"sha": TREE_SHA}},
        COMMIT_CREATE: {"sha": NEW_SHA},
    }
    return run_shell(
        tmp_path,
        [str(KEEPALIVE)],
        ("gh",),
        STUB_API_REPLIES=json.dumps({k: json.dumps(v) for k, v in replies.items()}),
        **env,
    )


def test_keepalive_script_is_executable_and_strict():
    text = KEEPALIVE.read_text(encoding="utf-8")
    assert text.startswith("#!/usr/bin/env bash\n")
    assert os.access(KEEPALIVE, os.X_OK), "the workflow runs it directly"
    assert re.search(r"(?m)^set -euo pipefail$", text)
    assert "60 days" in text, "the header says why the script exists"
    assert "--force" not in text and "force=" not in text, "main must not be forced"


@needs_jq
def test_keepalive_leaves_a_recent_main_alone(tmp_path):
    done, calls = run_keepalive(tmp_path, 44)
    assert done.returncode == 0, done.stderr
    assert [call[:4] for call in calls] == [["gh", "api", HEAD_COMMIT, "--jq"]]
    assert "44" in done.stdout


@needs_jq
def test_keepalive_commits_when_main_is_45_days_old(tmp_path):
    done, calls = run_keepalive(tmp_path, 45)
    assert done.returncode == 0, done.stderr
    assert [c for c in calls if "-X" in c or "-f" in c] == [
        [
            *["gh", "api", COMMIT_CREATE],
            *["-f", "message=Keep the scheduled workflows enabled"],
            *["-f", f"tree={TREE_SHA}", "-f", f"parents[]={HEAD_SHA}"],
            *["--jq", ".sha"],
        ],
        ["gh", "api", "-X", "PATCH", MAIN_REF, "-f", f"sha={NEW_SHA}"],
    ]
    assert [call[2] if call[2] != "-X" else call[4] for call in calls] == [
        HEAD_COMMIT,
        TREE_READ,
        COMMIT_CREATE,
        MAIN_REF,
    ]


@needs_jq
@pytest.mark.parametrize(
    ("failing", "calls_made"),
    [(HEAD_COMMIT, 1), (TREE_READ, 2), (COMMIT_CREATE, 3), (MAIN_REF, 4)],
)
def test_keepalive_fails_when_a_call_fails(tmp_path, failing, calls_made):
    """A call that fails stops the script, so it never goes on with an empty tree or an
    empty commit, and it exits with an error for the step to show."""
    done, calls = run_keepalive(tmp_path, 45, STUB_API_FAILS=failing)
    assert done.returncode != 0
    assert len(calls) == calls_made


def test_keepalive_needs_the_repository(tmp_path):
    env = {k: v for k, v in os.environ.items() if k != "GITHUB_REPOSITORY"}
    done = subprocess.run(
        [str(KEEPALIVE)], env=env, capture_output=True, text=True, check=False
    )
    assert done.returncode != 0
    assert "GITHUB_REPOSITORY" in done.stderr


def test_dependabot_groups_actions_monthly():
    text = DEPENDABOT.read_text(encoding="utf-8")
    assert re.search(r"(?m)^version: 2$", text)
    updates = entry(text, "updates", 0)[1]
    assert re.findall(r"(?m)^\s+- package-ecosystem:\s*(\S+)", updates) == [
        "github-actions"
    ]
    assert scalar(updates, "directory", 4) in ("/", '"/"')
    assert scalar(entry(updates, "schedule", 4)[1], "interval", 6) == "monthly"
    groups = entry(updates, "groups", 4)[1]
    assert re.findall(r"(?m)^ {6}([\w-]+):", groups) == ["actions"]
    patterns = scalar(entry(groups, "actions", 6)[1], "patterns", 8)
    assert patterns in ('["*"]', "['*']")
