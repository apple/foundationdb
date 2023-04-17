#! /usr/bin/env python3

import argparse
import json
import re
import subprocess
import sys

from typing import List


FDB_BASE_REPOSITORY = "https://github.com/apple/foundationdb"


def _args():
    parser = argparse.ArgumentParser(usage="cherry-pick-upstream.py [PR id] [PR id] ...")
    parser.add_argument("pr", nargs="+", type=int, help="The ID of the pull request")
    parser.add_argument("--dry-run", action="store_true", help="Do not trigger actual cherry-pick")

    return parser.parse_args()


def _get_current_branch() -> str:
    return subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"]).decode(
        "utf-8"
    ).strip()


def _fetch_upstream():
    subprocess.check_call(["git", "fetch", "upstream"])


def _cherry_pick(commit_id: str):
    subprocess.check_call(["git", "cherry-pick", commit_id])


def _is_merge(commit_id: str) -> bool:
    cat_file = subprocess.check_output(["git", "cat-file", "-p", commit_id])
    # If the commit has two parents, it has to be a merge commit
    decoded = cat_file.decode("utf-8")
    return len(re.findall(r"parent [0-9a-f]+\n", decoded)) == 2


def _get_commits_from_pr(pr: int):
    # e.g.
    #   gh pr -R https://github.com/apple/foundationdb view 9930 --json commits
    output_raw = subprocess.check_output(
        [
            "gh",
            "pr",
            "-R",
            FDB_BASE_REPOSITORY,
            "view",
            str(pr),
            "--json",
            "commits",
            "--json",
            "mergeCommit",
        ]
    )
    output = json.loads(output_raw)
    commits = [commit["oid"] for commit in output["commits"]]

    merge_commit = output["mergeCommit"]["oid"]

    return {"commits": commits, "merge_commit": merge_commit}


def _get_commits_from_merge_hash(merge: str) -> List[str]:
    output_raw = subprocess.check_output(["git", "log", f"{merge}^-", "--pretty=%H"])
    hashes = [h.decode("utf-8") for h in output_raw.splitlines()]

    if _is_merge(hashes[0]):
        hashes = hashes[1:]
    hashes = list(reversed(hashes))

    return hashes


def _is_already_committed(commit: str, branch: str) -> bool:
    output = subprocess.check_output(
        ["git", "branch", branch, "--contains", commit]
    ).decode("utf-8")
    return len(output.splitlines()) == 1


def _main():
    args = _args()
    prs = args.pr

    _fetch_upstream()
    current_branch = _get_current_branch()
    for pr in prs:
        print(f"Cherry-picking pull request {pr}")
        commits = _get_commits_from_pr(pr)
        commits_to_be_cherry_picked = _get_commits_from_merge_hash(
            commits["merge_commit"]
        )
        print("Commits: {}".format(" ".join(commits_to_be_cherry_picked)))
        for commit in commits_to_be_cherry_picked:
            if _is_already_committed(commit, current_branch):
                print(f"Commit {commit} is already in branch {current_branch}")
                continue
            if not args.dry_run:
                _cherry_pick(commit)
            else:
                print(f"Will cherry-pick {commit}")


if __name__ == "__main__":
    sys.exit(_main())
