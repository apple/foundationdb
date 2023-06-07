#! /usr/bin/env python3

import argparse
import json
import re
import subprocess
import sys

from typing import List, Dict, Any


FDB_BASE_REPOSITORY = "https://github.com/apple/foundationdb"


def _args():
    parser = argparse.ArgumentParser(
        usage="cherry-pick-upstream.py [PR id] [PR id] ..."
    )
    parser.add_argument("pr", nargs="+", type=int, help="The ID of the pull request")
    parser.add_argument(
        "--dry-run", action="store_true", help="Do not trigger actual cherry-pick"
    )

    return parser.parse_args()


def _get_current_branch() -> str:
    return (
        subprocess.check_output(["git", "rev-parse", "--abbrev-ref", "HEAD"])
        .decode("utf-8")
        .strip()
    )


CURRENT_BRANCH = _get_current_branch()


def _fetch_upstream():
    subprocess.check_call(["git", "fetch", "--all"])


def _cherry_pick(commit_id: str):
    command = ["git", "cherry-pick", commit_id]
    if _is_merge(commit_id):
        # FIXME Do we nee to do something?
        return
    subprocess.check_call(command)


def _is_merge(commit_id: str) -> bool:
    if commit_id is None:
        return False
    cat_file = subprocess.check_output(["git", "cat-file", "-p", commit_id])
    # If the commit has two parents, it has to be a merge commit
    decoded = cat_file.decode("utf-8")
    return len(re.findall(r"parent [0-9a-f]+\n", decoded)) == 2


def _get_pull_request_info(pr: int):
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
            "commits,mergeCommit,headRepository,headRepositoryOwner",
        ]
    )
    output = json.loads(output_raw)
    commits = [commit["oid"] for commit in output["commits"]]

    return {
        "commits": commits,
        "merge_commit": (output.get("mergeCommit") or {}).get("oid"),
        "repository": output["headRepository"]["name"],
        "owner": output["headRepositoryOwner"]["login"],
    }


def _get_commits_from_merge_hash(merge: str) -> List[str]:
    output_raw = subprocess.check_output(["git", "log", f"{merge}^-", "--pretty=%H"])
    hashes = [h.decode("utf-8") for h in output_raw.splitlines()]
    # Drop the merge hash
    hashes = hashes[1:]
    hashes = list(reversed(hashes))

    return hashes


def _is_already_committed(commit: str, branch: str) -> bool:
    output = subprocess.check_output(
        ["git", "branch", branch, "--contains", commit]
    ).decode("utf-8")
    return len(output.splitlines()) == 1


def _prepare_pull_request(pr: int) -> None:
    subprocess.check_call(["gh", "pr", "-R", FDB_BASE_REPOSITORY, "checkout", str(pr)])
    subprocess.check_call(["git", "checkout", CURRENT_BRANCH])


def _tag(pr: int):
    tag = f"cherry-pick-pr-{pr}"
    try:
        subprocess.check_call(["git", "rev-parse", tag])
        subprocess.check_call(["git", "tag", "-d", f"cherry-pick-pr-{pr}"])
    except subprocess.CalledProcessError:
        # The tag does not exist
        pass
    subprocess.check_call(["git", "tag", f"cherry-pick-pr-{pr}"])


def _main():
    args = _args()
    prs = args.pr

    _fetch_upstream()
    for pr in prs:
        print(f"Cherry-picking pull request {pr}")
        pr_info = _get_pull_request_info(pr)
        commits_to_be_cherry_picked = []
        if _is_merge(pr_info["merge_commit"]):
            print(f"Using {pr_info['merge_commit']} as the merge commit")
            commits_to_be_cherry_picked = _get_commits_from_merge_hash(
                pr_info["merge_commit"]
            )
        else:
            print("Using pull request branch")
            # Might be a fast-forward merge without merge hash, need to cherry-pick from the original owner's repository
            _prepare_pull_request(pr)
            commits_to_be_cherry_picked = pr_info["commits"]
        print("Commits: {}".format(" ".join(commits_to_be_cherry_picked)))
        for commit in commits_to_be_cherry_picked:
            if _is_already_committed(commit, CURRENT_BRANCH):
                print(f"Commit {commit} is already in branch {CURRENT_BRANCH}")
                continue
            if not args.dry_run:
                _cherry_pick(commit)
            else:
                print(f"Will cherry-pick {commit}")
        # Tag the pr
        _tag(pr)


if __name__ == "__main__":
    sys.exit(_main())
