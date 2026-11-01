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
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_DIR = ROOT / ".github" / "workflows"
SCRIPT = ROOT / "scripts" / "rolling-issue.sh"
DEPENDABOT = ROOT / ".github" / "dependabot.yml"
EXPECTED = ("backfill.yml", "smoke.yml", "test.yml", "tick.yml")

# Every job that writes: the token's `contents: write` is for these alone.
PUBLISH_JOBS = {
    ("tick.yml", "pypi-registry"),
    ("backfill.yml", "sweep"),
    ("smoke.yml", "smoke"),
}
REGISTRY_GROUP = {"group": "stream-pypi-registry", "cancel-in-progress": "false"}
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


def steps(job: str) -> list[str]:
    """The text of each step of a job (they come last in a job's text)."""
    return ["- " + part for part in re.split(r"(?m)^      - ", job)[1:]]


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


# --- stand-ins for uv and gh, and a way to run shell against them ---

STUB = """#!{python}
import json, os, subprocess, sys

name, args = os.path.basename(sys.argv[0]), sys.argv[1:]
with open(os.environ["STUB_LOG"], "a", encoding="utf-8") as log:
    log.write(json.dumps([name, *args]) + "\\n")
if name == "gh" and args[:2] == ["issue", "list"]:
    if os.environ.get("STUB_LIST_EXIT", "0") != "0":
        print("gh: HTTP 502", file=sys.stderr)
        sys.exit(int(os.environ["STUB_LIST_EXIT"]))
    listing = os.environ.get("STUB_ISSUES", "[]")
    if "--jq" in args:  # as gh does with its own jq
        expression = args[args.index("--jq") + 1]
        listing = subprocess.run(
            ["jq", "-r", expression], input=listing, capture_output=True,
            text=True, check=True,
        ).stdout
    sys.stdout.write(listing)
    sys.exit(0)
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
    tmp_path: Path, script: str, **env: str
) -> tuple[subprocess.CompletedProcess[str], list[list[str]]]:
    """Run a step's script as the runner does (`bash -e`), with a stand-in for uv."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    file = tmp_path / "step.sh"
    file.write_text(script, encoding="utf-8")
    return run_shell(tmp_path, ["bash", "-e", str(file)], ("uv",), **env)


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
    assert len(jobs) >= 5
    for file, name, body in jobs:
        value = scalar(body, "timeout-minutes", 4)
        assert value is not None, f"{file}: job {name} has no timeout-minutes"
        assert value.isdigit() and 1 <= int(value) <= 360, f"{file}: {name}: {value}"


def test_job_timeouts_leave_room_after_the_run_budget():
    """The run stops starting projects after 18,000 s and the sweep after 16,200 s,
    and then compresses, uploads and publishes."""
    assert scalar(job_of("tick.yml", "pypi-registry"), "timeout-minutes", 4) == "330"
    assert scalar(job_of("backfill.yml", "sweep"), "timeout-minutes", 4) == "300"
    assert scalar(job_of("smoke.yml", "smoke"), "timeout-minutes", 4) == "15"


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
    writers: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for file, name, body in all_jobs():
        granted = mapping(body, "permissions", 4)
        if granted is None:  # the job has what the top level gives it
            continue
        assert isinstance(granted, dict), f"{file}: {name}: {granted!r}"
        for scope, level in granted.items():
            assert (scope, level) in {
                ("contents", "write"),
                ("issues", "write"),
                ("actions", "write"),
            }, f"{file}: {name} asks for {scope}: {level}"
            writers[scope].add((file, name))
    assert writers["contents"] == PUBLISH_JOBS
    assert writers["issues"] == PUBLISH_JOBS  # each opens and closes its issue
    assert writers["actions"] == {("backfill.yml", "redispatch")}
    redispatch = job_of("backfill.yml", "redispatch")
    assert mapping(redispatch, "permissions", 4) == {"actions": "write"}


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


def test_smoke_job_has_its_own_concurrency_group():
    smoke = job_of("smoke.yml", "smoke")
    assert mapping(smoke, "concurrency", 4) == {
        "group": "stream-kit-smoke",
        "cancel-in-progress": "false",
    }


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
    assert list(jobs) == ["sweep", "redispatch"]
    sweep, redispatch = jobs["sweep"], jobs["redispatch"]

    assert "gh workflow run" not in sweep
    assert entry(redispatch, "concurrency", 4) is None
    assert scalar(redispatch, "needs", 4) == "sweep"
    assert scalar(redispatch, "if", 4) == "needs.sweep.outputs.continue == 'true'"
    assert scalar(sweep, "outputs", 4) == ""
    assert scalar(sweep, "continue", 6) == "${{ steps.sweep.outputs.continue }}"
    assert one_script("backfill.yml", "redispatch", "gh workflow run") == (
        "gh workflow run backfill.yml -f task=sweep"
    )
    token = re.search(r"(?m)^\s+GH_TOKEN:\s*(.*?)\s*$", redispatch)
    assert token and token[1] == "${{ github.token }}"
    assert re.search(r"(?m)^\s+GH_REPO:", redispatch), "no checkout, so no repository"
    assert not re.search(r"uses:\s*actions/checkout", redispatch)


def test_nothing_dispatches_from_inside_a_concurrency_group():
    for file, name, body in all_jobs():
        if "gh workflow run" in body:
            assert entry(body, "concurrency", 4) is None, f"{file}: {name}"


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
        [call] = calls
        assert call[:3] == ["uv", "run", "--package"]
        assert call[3:] == ["pypi-registry", "python", "-m", "pypi_registry", "sweep"]


def test_tick_passes_bootstrap_and_a_reason_only_when_asked(tmp_path):
    script = one_script("tick.yml", "pypi-registry", "pypi_registry run")
    base = ["uv", "run", "--package", "pypi-registry", "python", "-m"]
    base += ["pypi_registry", "run"]
    cases = [
        # (what the inputs hold, the arguments after `run`)
        ({}, []),  # a scheduled run has no inputs
        ({"BOOTSTRAP": "false", "OVERRIDE_REASON": ""}, []),
        ({"BOOTSTRAP": "true", "OVERRIDE_REASON": ""}, ["--bootstrap"]),
        (
            {"BOOTSTRAP": "false", "OVERRIDE_REASON": "PyPI removed 2,000 names"},
            ["--override-reason", "PyPI removed 2,000 names"],
        ),
        (
            {"BOOTSTRAP": "true", "OVERRIDE_REASON": 'it\'s "odd" $HOME; ls'},
            ["--bootstrap", "--override-reason", 'it\'s "odd" $HOME; ls'],
        ),
    ]
    for number, (inputs, extra) in enumerate(cases):
        done, calls = run_step(tmp_path / str(number), script, **inputs)
        assert done.returncode == 0, done.stderr
        assert calls == [[*base, *extra]], inputs


def test_a_failing_run_fails_the_tick_step(tmp_path):
    script = one_script("tick.yml", "pypi-registry", "pypi_registry run")
    done, _ = run_step(tmp_path, script, STUB_EXIT="1")
    assert done.returncode == 1


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
    the variable."""
    scripts = [s for _, _, body in all_jobs() for s in run_scripts(body)]
    assert len(scripts) >= 10
    assert [s for s in scripts if "${{" in s] == []


def test_triggers():
    tick = split_jobs(workflows()["tick.yml"])[0]
    assert re.search(r"(?m)^    - cron: [\"']23 \*/6 \* \* \*[\"']\s*$", tick)
    inputs = entry(entry(tick, "workflow_dispatch", 2)[1], "inputs", 4)[1]
    bootstrap = entry(inputs, "bootstrap", 6)[1]
    reason = entry(inputs, "override_reason", 6)[1]
    assert scalar(bootstrap, "type", 8) == "boolean"
    assert scalar(bootstrap, "default", 8) == "false"
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
    """A run that fails opens (or adds to) the stream's issue, and a tick or a smoke
    run that succeeds closes it. A sweep never closes it: a good sweep says nothing
    about whether the daily run is well."""
    expected = {
        ("tick.yml", "pypi-registry"): ("pypi-registry", ["failure", "success"]),
        ("backfill.yml", "sweep"): ("pypi-registry", ["failure"]),
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
    """Nothing here pushes, so the token the jobs hold is not left in .git/config."""
    for file, name, body in all_jobs():
        for step in steps(body):
            if "actions/checkout" in step and (file, name) in PUBLISH_JOBS:
                assert re.search(r"(?m)^\s+persist-credentials:\s*false\s*$", step), (
                    f"{file}: {name}"
                )


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
