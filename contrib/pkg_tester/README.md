# pkg_tester

This is a test suite that can be used to validate properties of generated package files.

To use it, first build the package files as described in the main [README](https://github.com/apple/foundationdb#linux)

Then setup a virtualenv:

```
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip install -r requirements.txt
```

Then you can run the tests with pytest:

```
$ BUILDDIR=<BUILDDIR> python -m pytest -s -v
```

These are snapshot tests, so you may need to update the snapshots with

```
$ BUILDDIR=<BUILDDIR> python -m pytest -s -v --snapshot-update
```

Use discretion about whether or not the behavior change is acceptable.

A helpful tip for debugging: if you run pytest with `--pdb`, then it will pause
the tests at the first error which gives you a chance to run some `docker exec`
commands to try and see what's wrong.

There's a small chance that this will leak an image (especially if you interrupt the test with ctrl-c). Consider running

```
$ docker image prune
```

after.

# Requirements

docker, python

# Future work?

- [x] Test rpms
- [x] Test debs
- [ ] Test versioned packages
- [ ] Test that upgrades preserve data/config
