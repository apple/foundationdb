FROM python:3.6

RUN apt-get update; apt-get install -y dnsutils

RUN mkdir -p /app
WORKDIR /app

COPY --from=foundationdb:5.2.5 /usr/lib/libfdb_c.so /usr/lib
COPY --from=foundationdb:5.2.5 /usr/bin/fdbcli /usr/bin/
COPY --from=foundationdb:5.2.5 /var/fdb/scripts/create_cluster_file.bash /app

COPY requirements.txt /app
RUN pip install -r requirements.txt

COPY start.bash /app
COPY server.py /app
RUN chmod u+x /app/start.bash

CMD /app/start.bash

ENV FLASK_APP=server.py
ENV FLASK_ENV=development