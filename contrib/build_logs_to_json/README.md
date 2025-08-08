This is a small script to convert a few interesting log files to json for easier ingestion into Snowflake e.g.

`$BUILDDIR/.ninja_logs` has timing information for different targets in the build (if you built with ninja)
`$BUILDDIR/Testing/**/*.xml` has information about ctest timings and output etc (if you run ctest with `-T Test`)
Uses [xmltodict](https://github.com/martinblech/xmltodict) to convert to json.

Writes json files to the `$BUILDDIR/packages` directory, the idea being that the packages directory is basically everything you want to archive after a build.

Example usage:

```
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip install -r requirements.txt
$ python build_logs_to_json.py --build-dir $BUILDDIR
```
