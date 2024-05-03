#!/usr/bin/env python3

# branch_diff.py
#
#  * Diff two branches FROM..TO
#  * Record all commits that in TO but not in FROM
#  * List the Pull Requests related to the commits together with authors in a Markdown format

import argparse
import json
import subprocess
import sys

FOUNDATIONDB_GITHUB = "https://github.com/apple/foundationdb"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--from", dest="from_branch", required=True, help="The from branch"
    )
    parser.add_argument("--to", dest="to_branch", required=True, help="The to branch")
    parser.add_argument("--output", dest="output", help="The output file")
    return parser.parse_args()


def diff_branches_by_commit(from_branch, to_branch):
    command_line = [
        "git",
        "log",
        f"{from_branch}..{to_branch}",
        "--no-merges",
        "--oneline",
    ]
    process = subprocess.run(
        command_line, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    if process.returncode != 0:
        raise Exception(
            f"git log failed with command {' '.join(command_line)}: \n {process.stderr}"
        )

    return [line[:9] for line in process.stdout.split("\n")]


class PullRequestInfo:
    def __init__(self, number, author_name, title):
        self.number = number
        self.author_name = author_name
        self.title = title
        self.commit_hashes = []

    def add_commit(self, commit_hash):
        self.commit_hashes.append(commit_hash)

    def __str__(self):
        return f"{self.number:6} {self.title[:50]:50} ({self.author_name})"


def get_pr_from_commit(commit_hash):
    command_line = [
        "gh",
        "pr",
        "list",
        "-R",
        FOUNDATIONDB_GITHUB,
        "--search",
        commit_hash,
        "--state=merged",
        "--json",
        "number,author,title",
    ]
    process = subprocess.run(
        command_line, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    if process.returncode != 0:
        raise Exception(
            f"gh pr failed with command {command_line}: \n {process.stderr}"
        )

    result = json.loads(process.stdout)
    if not result:
        sys.stderr.write(f"Failed to parse {process.stdout}")
        return None
    else:
        try:
            number = result[0]["number"]
            author_name = result[0]["author"]["name"]
            title = result[0]["title"]
            return PullRequestInfo(number, author_name, title)
        except:
            sys.stderr.write(f"Unrecognized output: {process.stdout}")
            return None


def render_markdown_table(stream, pull_requests):
    stream.write("|PR ID|Author|Title|\n")
    stream.write("|-----|------|-----|\n")
    for item in pull_requests.values():
        stream.write(f"|{item.number}|{item.author_name}|{item.title}|\n")


def main():
    args = parse_args()

    diff_commits = diff_branches_by_commit(args.from_branch, args.to_branch)

    total = len(diff_commits)
    pull_requests = {}
    for index, commit_hash in enumerate(diff_commits):
        pr_info = get_pr_from_commit(commit_hash)
        if not pr_info:
            print(f"Did not find pull request for {commit_hash}", file=sys.stderr)
            continue

        if pr_info.number not in pull_requests:
            pull_requests[pr_info.number] = pr_info
        pull_requests[pr_info.number].add_commit(commit_hash)

        print(f"\r {index}/{total} done", end="")

    print()

    stream = sys.stdout
    if args.output:
        stream = open(args.output, "w")
    render_markdown_table(stream, pull_requests)


if __name__ == "__main__":
    main()
