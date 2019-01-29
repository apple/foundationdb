#!/usr/bin/env bash

cat $1 | grep '<PackageName>' | sed -e 's,^[^>]*>,,' -e 's,<.*,,'
