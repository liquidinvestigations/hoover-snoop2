FROM liquidinvestigations/hoover-snoop2:0.16-base

ARG USER_NAME=liquid
ARG UID=666
ARG GID=666
RUN groupadd -g $GID -o $USER_NAME
RUN useradd -m -u $UID -g $GID -o -s /bin/bash $USER_NAME

# deps, install s3fs, fuse-7z-ng and concat-fuse
RUN apt-get update && apt-get install -y gosu s3fs cmake libfuse-dev build-essential libfuse-dev libmhash-dev libminizip-dev     build-essential pkg-config cmake g++ clang libfuse-dev libmhash-dev libminizip-dev zlib1g-dev libssl-dev libgtest-dev
RUN mkdir /opt/fuse-7z \
 && git clone https://github.com/liquidinvestigations/fuse-7z-ng /opt/fuse-7z \
 && /opt/fuse-7z/build.sh \
 && rm -rf /opt/fuse-7z

# install snoop
RUN mkdir -p /opt/hoover/snoop/static
WORKDIR /opt/hoover/snoop

ADD Pipfile Pipfile.lock ./
RUN pipenv install --system --deploy --ignore-pipfile

COPY . .

ENV THREAD_COUNT 20
RUN set -e \
 && echo '#!/bin/bash -e' > /runserver \
 && echo 'waitress-serve --threads $THREAD_COUNT --port 8080 snoop.wsgi:application' >> /runserver \
 && chmod +x /runserver

RUN chown -R $UID:$GID /runserver
RUN chown -R $UID:$GID /opt/libpst

ENV DATA_DIR "/opt/hoover/snoop"
ENV USER_NAME $USER_NAME
ENV UID $UID
ENV GID $GID

ENV CELERY_DB_REUSE_MAX=0
RUN set -e \
 && SECRET_KEY=temp SNOOP_URL_PREFIX=snoop/ SNOOP_DB='postgresql://snoop:snoop@snoop-pg:5432/snoop' ./manage.py collectstatic --noinput

ENTRYPOINT ["/opt/hoover/snoop/docker-entrypoint.sh"]

CMD /wait && /runserver
