"""Point the Customs Databricks jobs at this GitHub repo instead of the workspace.

Each notebook task is rewritten from

    source: WORKSPACE
    notebook_path: /Workspace/Users/<user>/.../1. Customs/<rel>

to

    source: GIT
    notebook_path: notebooks/<rel>

and a job-level ``git_source`` is added pinning the branch. Databricks then
checks the repo out fresh on every run, so whatever is on ``main`` is what runs -
editing a notebook in the workspace UI no longer affects scheduled runs.

    python scripts/jobs_use_git.py --dry-run   # show what would change
    python scripts/jobs_use_git.py             # apply

Requires a Git credential for the repo owner in the workspace
(Settings -> Linked accounts). Private repos will not check out without it.
"""

import argparse
import json
import os

from databricks.sdk import WorkspaceClient

GIT_URL = "https://github.com/garethalt675/customs-etl"
GIT_PROVIDER = "gitHub"
GIT_BRANCH = "main"

WS_BASE = os.environ.get(
    "CUSTOMS_WS_BASE",
    "/Workspace/Users/tuckeyhue@gmail.com/Market Research/1. Data ETL/1. Customs",
)
REPO_PREFIX = "notebooks"

JOB_IDS = [
    834607708504289,    # totals
    272686436789081,    # countries
    943749699682688,    # fdi
    127808821627251,    # provinces
    20569819731044,     # transportation
    712991216447767,    # curated
    1089606027374023,   # orchestrator (run_job tasks only - no notebooks)
]


def to_repo_path(workspace_path):
    """Map a workspace notebook path to its repo-relative equivalent."""
    base = WS_BASE.rstrip("/")
    if not workspace_path.startswith(base + "/"):
        raise ValueError(f"notebook path outside the Customs tree: {workspace_path}")
    return f"{REPO_PREFIX}/{workspace_path[len(base) + 1:]}"


def convert(settings):
    """Return (new_settings, list_of_changes). Idempotent."""
    changes = []
    tasks = settings.get("tasks") or []
    has_notebooks = any("notebook_task" in t for t in tasks)

    for task in tasks:
        nb = task.get("notebook_task")
        if not nb:
            continue
        if nb.get("source") == "GIT":
            continue
        old = nb["notebook_path"]
        new = to_repo_path(old)
        nb["notebook_path"] = new
        nb["source"] = "GIT"
        changes.append(f"  {task['task_key']:24s} {old}\n      -> GIT :: {new}")

    if has_notebooks:
        wanted = {
            "git_url": GIT_URL,
            "git_provider": GIT_PROVIDER,
            "git_branch": GIT_BRANCH,
        }
        if settings.get("git_source") != wanted:
            settings["git_source"] = wanted
            changes.append(f"  git_source -> {GIT_URL} @ {GIT_BRANCH}")

    return settings, changes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    w = WorkspaceClient(profile="DEFAULT")

    creds = list(w.git_credentials.list())
    if not creds:
        raise SystemExit(
            "No Git credential in this workspace - a private repo will not check "
            "out. Add one under Settings -> Linked accounts first."
        )
    print(f"git credential: {creds[0].git_provider} / {creds[0].git_username}\n")

    for job_id in JOB_IDS:
        job = w.jobs.get(job_id)
        settings = job.settings.as_dict()
        name = settings.get("name")

        settings, changes = convert(settings)
        if not changes:
            print(f"{name}  (job {job_id})\n  already on git, nothing to do\n")
            continue

        print(f"{name}  (job {job_id})")
        print("\n".join(changes))

        if args.dry_run:
            print("  [dry run - not applied]\n")
            continue

        w.api_client.do(
            "POST", "/api/2.2/jobs/reset",
            body={"job_id": job_id, "new_settings": settings},
        )
        print("  applied\n")

    if args.dry_run:
        print("dry run complete - rerun without --dry-run to apply")


if __name__ == "__main__":
    main()
