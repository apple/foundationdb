#! /usr/bin/env python3

import argparse
import dataclasses
import json
import os.path
import re
import subprocess
import sys

from typing import Dict, List, Union


FDB_BASE_REPOSITORY = "https://github.com/apple/foundationdb"


def _silent_subprocess(command, mute_even_failed: bool=False, *args, **kwargs) -> str:
    output = None
    try:
        output = (
            subprocess.check_output(command, *args, **kwargs).decode("utf-8").strip()
        )
        return output
    except:
        if not mute_even_failed:
            print(f"Error running subprocess [{' '.join(command)}]:\n{output}", file=sys.stderr)
        raise


CURRENT_BRANCH = _silent_subprocess(["git", "rev-parse", "--abbrev-ref", "HEAD"])
CURRENT_HEAD = _silent_subprocess(["git", "rev-parse", "--verify", "HEAD"])
GIT_ROOT = _silent_subprocess(["git", "rev-parse", "--show-toplevel"])


_fetch_upstream = lambda: _silent_subprocess(["git", "fetch", "--all"])


# TODO: Rewrite this as a module
class MergeOperation:
    MERGE_FILE_PATH = os.path.join(GIT_ROOT, ".git", "__cherrypick_merge.status")

    class AlreadyMergeInProgressException(RuntimeError):
        pass

    @staticmethod
    def is_in_merge_process() -> bool:
        """Check if currently in a merge process"""
        return os.path.exists(MergeOperation.MERGE_FILE_PATH)

    @dataclasses.dataclass
    class MergeProgress:
        # ID of the pull request
        pr_id: int
        # The id of the merge commit
        merge_id: Union[str, None]
        # The commit id on main branch right before the merge
        merge_on: str
        # Commits that merged to main
        commit_ids: List[str]
        # Number of commits in the merge
        total_commits: int
        # Number of commits that are already cherry-picked
        cherry_picked: int

    class MergeProgressEncoder(json.JSONEncoder):
        def default(self, o: object):
            if isinstance(o, MergeOperation.MergeProgress):
                return {
                    "pr_id": o.pr_id,
                    "merge_id": o.merge_id,
                    "merge_on": o.merge_on,
                    "commit_ids": o.commit_ids,
                    "total_commits": o.total_commits,
                    "cherry_picked": o.cherry_picked,
                }
            return json.JSONEncoder.default(self, o)

    @staticmethod
    def _as_merge_progress(dct: Dict) -> "MergeOperation.MergeProgress":
        return MergeOperation.MergeProgress(
            pr_id=int(dct.get("pr_id")),
            merge_id=str(dct.get("merge_id")),
            merge_on=str(dct.get("merge_on")),
            commit_ids=list(dct.get("commit_ids")),
            total_commits=int(dct.get("total_commits")),
            cherry_picked=int(dct.get("cherry_picked")),
        )

    @staticmethod
    def delete_merge_progress():
        if os.path.exists(MergeOperation.MERGE_FILE_PATH):
            os.remove(MergeOperation.MERGE_FILE_PATH)

    @staticmethod
    def read_merge_progress() -> MergeProgress:
        with open(MergeOperation.MERGE_FILE_PATH) as stream:
            return json.loads(
                stream.read(), object_hook=MergeOperation._as_merge_progress
            )

    @staticmethod
    def write_merge_progress(merge_progress: MergeProgress):
        with open(MergeOperation.MERGE_FILE_PATH, "w") as stream:
            stream.write(
                json.dumps(merge_progress, cls=MergeOperation.MergeProgressEncoder)
            )

    @staticmethod
    def is_merge(commit_id: str) -> bool:
        """Check if the commit is a merge commit, a merge commit has two parents"""
        if not commit_id:
            return False
        cat_file = _silent_subprocess(["git", "cat-file", "-p", commit_id])
        # If the commit has two parents, it has to be a merge commit
        return len(re.findall(r"parent [0-9a-f]+\n", cat_file)) == 2

    @staticmethod
    def get_commits_from_merge_hash(merge: str) -> List[str]:
        """Get the commits in a merge"""
        output_raw = _silent_subprocess(["git", "log", f"{merge}^-", "--pretty=%H"])
        hashes = [h for h in output_raw.splitlines()]
        hashes = list(reversed(hashes))

        return hashes

    @staticmethod
    def cherry_pick_commit(commit_id: str):
        command = ["git", "cherry-pick", commit_id, "--allow-empty"]
        if MergeOperation.is_merge(commit_id):
            # FIXME Do we need to do something?
            return
        _silent_subprocess(command)


def _get_pull_request_info(pr: int):
    # e.g.
    #   gh pr -R https://github.com/apple/foundationdb view 9930 --json commits
    output_raw = _silent_subprocess(
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


def _is_already_committed(commit: str, branch: str) -> bool:
    output = _silent_subprocess(["git", "branch", branch, "--contains", commit])
    return len(output.splitlines()) == 1


def _prepare_pull_request(pr: int) -> None:
    """Check the pull request locally"""
    _silent_subprocess(["gh", "pr", "-R", FDB_BASE_REPOSITORY, "checkout", str(pr)])
    _silent_subprocess(["git", "checkout", CURRENT_BRANCH])


def _tag(pr: int):
    tag = f"cherry-pick-pr-{pr}"
    try:
        _silent_subprocess(["git", "rev-parse", tag], mute_even_failed=True)
        _silent_subprocess(["git", "tag", "-d", f"cherry-pick-pr-{pr}"])
    except subprocess.CalledProcessError:
        # The tag does not exist
        pass
    _silent_subprocess(["git", "tag", tag])
    print(f"Created tag {tag}")


def _start_merge(pr: int) -> MergeOperation.MergeProgress:
    print(f"Cherry-picking pull request {pr}")
    pr_info = _get_pull_request_info(pr)

    if MergeOperation.is_merge(pr_info["merge_commit"]):
        print(f"Using {pr_info['merge_commit']} as the merge commit")
        commits_to_be_cherry_picked = MergeOperation.get_commits_from_merge_hash(
            pr_info["merge_commit"]
        )
        return MergeOperation.MergeProgress(
            pr_id=pr,
            merge_id=commits_to_be_cherry_picked[-1],
            merge_on=CURRENT_HEAD,
            commit_ids=commits_to_be_cherry_picked[:-1],
            total_commits=len(commits_to_be_cherry_picked) - 1,
            cherry_picked=0,
        )
    else:
        print(f"Using pull request branch")
        # Might be a fast-forward merge without merge hash, need to cherry-pick from the original owner's repository
        _prepare_pull_request(pr)
        commits_to_be_cherry_picked = pr_info["commits"]
        return MergeOperation.MergeProgress(
            pr_id=pr,
            merge_id=None,
            merge_on=CURRENT_HEAD,
            commit_ids=commits_to_be_cherry_picked,
            total_commits=len(commits_to_be_cherry_picked),
            cherry_picked=0,
        )


def _report_merge_status():
    merge_progress = MergeOperation.read_merge_progress()
    print(
        f"Current merging PR {merge_progress.pr_id} on commit {merge_progress.merge_on}"
    )
    print(
        f"Merge progress: {merge_progress.cherry_picked}/{merge_progress.total_commits}"
    )
    if merge_progress.cherry_picked > 0:
        print("Cherry-picked:")
        for index in range(0, merge_progress.cherry_picked):
            print(f"  {merge_progress.commit_ids[index]}")
    print("Undergoing:")
    for index in range(merge_progress.cherry_picked, merge_progress.total_commits):
        print(f"  {merge_progress.commit_ids[index]}")


def _abort_merge():
    merge_progress = MergeOperation.read_merge_progress()
    print(f"Resetting to previous HEAD: {merge_progress.merge_on}")
    _silent_subprocess(["git", "reset", "--hard", merge_progress.merge_on])
    MergeOperation.delete_merge_progress()
    print("Merge process file deleted")


def _continue_merge():
    merge_progress = MergeOperation.read_merge_progress()
    start_index = merge_progress.cherry_picked
    for index in range(start_index, merge_progress.total_commits):
        commit = merge_progress.commit_ids[index]
        if _is_already_committed(commit, CURRENT_BRANCH):
            print(f"Commit {commit} is already in branch {CURRENT_BRANCH}")
            continue
        else:
            print(f"Cherry-picking {commit}")
            MergeOperation.cherry_pick_commit(commit)
            merge_progress.cherry_picked += 1
            MergeOperation.write_merge_progress(merge_progress)


    MergeOperation.delete_merge_progress()

    # Tag the pr id to the last merge
    _tag(merge_progress.pr_id)


def _drop_current_cherrypick():
    if os.path.exists(os.path.join(GIT_ROOT, ".git", "CHERRY_PICK_HEAD")):
        raise RuntimeError("Cherry pick unresolved")
    merge_progress = MergeOperation.read_merge_progress()
    print(f"Assuming commit {merge_progress.commit_ids[merge_progress.cherry_picked]} as resolved")
    merge_progress.cherry_picked += 1
    MergeOperation.write_merge_progress(merge_progress)


def _args():
    parser = argparse.ArgumentParser(usage="prx.py [PR id] ...")

    subparser = parser.add_subparsers(dest="action")

    merge_parser = subparser.add_parser("merge", help="Merge a pull request")
    merge_parser.add_argument(
        "pr", nargs=1, type=int, help="The ID of the pull request"
    )
    merge_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not trigger actual cherry-pick, only print out the list of commits",
    )

    continue_parser = subparser.add_parser(
        "continue", help="Continue the current merge process"
    )

    abort_parser = subparser.add_parser("abort", help="Abort the current merge process")

    status_parser = subparser.add_parser(
        "status", help="Status of the current merge process"
    )

    return parser.parse_args()


def _main():
    args = _args()
    is_in_merge_process = MergeOperation.is_in_merge_process()

    if args.action == "merge":
        if is_in_merge_process:
            raise RuntimeError("Currently in a merge process")
        merge_progress = _start_merge(args.pr[0])
        MergeOperation.write_merge_progress(merge_progress)

        if args.dry_run:
            _report_merge_status()
            MergeOperation.delete_merge_progress()
        else:
            _continue_merge()

        return

    if not is_in_merge_process:
        raise RuntimeError("Currently not in a merge process")

    if args.action == "continue":
        _drop_current_cherrypick()
        _continue_merge()
    elif args.action == "abort":
        _abort_merge()
    elif args.action == "status":
        _report_merge_status()


if __name__ == "__main__":
    sys.exit(_main())
