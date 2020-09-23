From liquidinvestigations/hoover-snoop2:base

ARG UNAME=liquid
ARG UID=666
ARG GID=666
RUN groupadd -g $GID -o $UNAME
RUN useradd -m -u $UID -g $GID -o -s /bin/bash $UNAME

RUN apt-get update && apt-get install sudo

# install snoop
RUN mkdir -p /opt/hoover/snoop
WORKDIR /opt/hoover/snoop

ADD Pipfile Pipfile.lock ./
RUN pipenv install --system --deploy --ignore-pipfile

COPY . .

ENV THREAD_COUNT 20
RUN set -e \
 && echo '#!/bin/bash -e' > /runserver \
 && echo 'waitress-serve --threads $THREAD_COUNT --port 80 snoop.wsgi:application' >> /runserver \
 && chmod +x /runserver

ENV DATA_DIR "/opt/hoover/snoop"
ENV USER_NAME $UNAME
ENV UID $UID
ENV GID $GID

ENTRYPOINT ["/opt/hoover/snoop/docker-entrypoint.sh"]

RUN whoami

RUN set -e \
 && SECRET_KEY=temp SNOOP_DB='postgresql://snoop:snoop@snoop-pg:5432/snoop' ./manage.py collectstatic --noinput

RUN whoami

CMD /wait && /runserver
