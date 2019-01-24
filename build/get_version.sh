#!/usr/bin/env bash

cat $1 | grep '<Version>' | sed -e 's,^[^>]*>,,' -e 's,<.*,,'

