From liquidinvestigations/hoover-snoop2:base

# download others
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.3.0/wait /wait

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
 && chmod +x /runserver /wait

RUN set -e \
 && SECRET_KEY=temp SNOOP_DB='postgresql://snoop:snoop@snoop-pg:5432/snoop' ./manage.py collectstatic --noinput

CMD /wait && /runserver
