## PLEASE DELETE THIS TEMPLATE WHEN YOU POST YOUR PR FOR REVIEW

Please describe the following in your PR description:
  * The problem being solved. If this is a bug you found, please describe how/where you encountered it.
  * The nature of the solution.
  * Testing you have done, if any.

We encourage you to use AI coding assistants to review diffs in advance. This can be done by saving your diff to a file:
```
git diff upstream/main -- . > /tmp/pr.diff
```

Then have your agent review the diff. We have had good results with the following prompt:

```
Review /tmp/pr.diff. What is it trying to do? Is it correct? Are there bugs? Are there omissions? Are there better ways of doing things? Should this CL be LGTMd?
```

## END OF TEMPLATE TO DELETE
