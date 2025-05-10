FROM rclone/rclone:1 AS rclone

FROM restic/restic:0.18.0 AS restic

FROM python:3.13-alpine

COPY --from=rclone /usr/local/bin/rclone /usr/local/bin/rclone
COPY --from=restic /usr/bin/restic /usr/local/bin/restic

RUN apk update \
    && apk add \
        dcron \
        mariadb-client \
        postgresql-client \
        mariadb-connector-c-dev

ADD . /restic-compose-backup
WORKDIR /restic-compose-backup
RUN pip install -U pip setuptools wheel && pip install .
ENV XDG_CACHE_HOME=/cache

ENTRYPOINT []
CMD ["./entrypoint.sh"]
