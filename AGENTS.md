# AGENTS.md

This file provides guidance to AI coding agents working in this repository.

## Pull Requests

Use the root `pull_request_template.md` as the default PR description format.
Read it before creating or updating a PR. Keep its headings, replace its guidance
comments with the actual description, and report only checks and results that
you verified. Follow this format even when supplying the body through `gh`, an
API, or another tool; commit messages alone do not replace the template.

## Release Branches

Changes to a release branch must be cherry-picks from the next newer release
branch, or from `main` for the newest release branch. Explain why the release
branch needs the change in the PR description or a linked GitHub issue.
