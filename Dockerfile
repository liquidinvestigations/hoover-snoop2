From liquidinvestigations/hoover-snoop2:base

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

RUN set -e \
 && SECRET_KEY=temp SNOOP_DB='postgresql://snoop:snoop@snoop-pg:5432/snoop' ./manage.py collectstatic --noinput

CMD /wait && /runserver
