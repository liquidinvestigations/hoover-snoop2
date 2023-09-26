ARG BASE_TAG=0-base
FROM liquidinvestigations/hoover-snoop2:${BASE_TAG}

ARG USER_NAME=liquid
ARG UID=666
ARG GID=666
RUN groupadd -g $GID -o $USER_NAME
RUN useradd -m -u $UID -g $GID -o -s /bin/bash $USER_NAME


# install snoop
RUN mkdir -p /opt/hoover/snoop/static
WORKDIR /opt/hoover/snoop

ADD Pipfile Pipfile.lock ./
RUN pipenv install --system --deploy --ignore-pipfile


COPY . .
COPY .git .
RUN chmod +x /opt/hoover/snoop/docker-entrypoint.sh

COPY ./runserver /runserver

RUN chown -R $UID:$GID /runserver && chmod +x /runserver
RUN chown -R $UID:$GID /opt/libpst

ENV USER_NAME $USER_NAME
ENV UID $UID
ENV GID $GID

ENV CELERY_DB_REUSE_MAX=0
ENV OTEL_TRACES_EXPORTER=none OTEL_METRICS_EXPORTER=none OTEL_LOGS_EXPORTER=none

RUN set -e \
 && SECRET_KEY=temp SNOOP_URL_PREFIX=snoop/ SNOOP_DB='postgresql://snoop:snoop@snoop-pg:5432/snoop' ./manage.py collectstatic --noinput

RUN git config --global --add safe.directory "*"

ENTRYPOINT ["/opt/hoover/snoop/docker-entrypoint.sh"]

ENV GUNICORN_WORKER_CLASS=sync
ENV GUNICORN_WORKERS=2
ENV GUNICORN_THREADS=1
ENV GUNICORN_MAX_REQUESTS=1

CMD /wait && /runserver
