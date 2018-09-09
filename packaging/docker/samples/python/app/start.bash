#! /bin/bash

/app/create_cluster_file.bash
FLASK_APP=server.py flask run --host=0.0.0.0