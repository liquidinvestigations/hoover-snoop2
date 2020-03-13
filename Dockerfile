FROM python:3-buster
ENV PYTHONUNBUFFERED 1

RUN set -e \
 && echo 'deb http://deb.debian.org/debian buster non-free' >> /etc/apt/sources.list \
 && echo 'deb http://deb.debian.org/debian buster-updates non-free' >> /etc/apt/sources.list \
 && echo 'deb http://security.debian.org buster/updates non-free' >> /etc/apt/sources.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends \
     p7zip-full p7zip-rar \
     cpanminus \
     poppler-utils \
     libgsf-1-dev \
     postgresql-client \
     python-numpy \
     libicu-dev \
 && cpanm --notest Email::Outlook::Message \
 && apt-get clean && rm -rf /var/lib/apt/lists/* \
 && pip install pipenv

RUN mkdir -p /opt/hoover/snoop
WORKDIR /opt/hoover/snoop

ADD Pipfile Pipfile.lock ./
RUN pipenv install --system --deploy --ignore-pipfile

RUN cd /opt \
  && git clone https://github.com/liquidinvestigations/magic-definitions.git \
  && ( cd magic-definitions && ./build.sh )
ENV PATH="/opt/magic-definitions/file/bin:${PATH}"
ENV MAGIC_FILE="/opt/magic-definitions/magic.mgc"

ENV LIBPST_VERSION libpst-0.6.74

RUN wget http://www.five-ten-sg.com/libpst/packages/$LIBPST_VERSION.tar.gz --progress=dot:giga \
  && tar zxvf $LIBPST_VERSION.tar.gz \
  && rm -f $LIBPST_VERSION.tar.gz \
  && mv $LIBPST_VERSION /opt/libpst \
  && cd /opt/libpst \
  && ./configure --disable-python --prefix="`pwd`" \
  && make \
  && make install
ENV PATH="/opt/libpst/bin:${PATH}"

COPY . .

ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.3.0/wait /wait

ENV THREAD_COUNT 20
RUN set -e \
 && echo '#!/bin/bash -e' > /runserver \
 && echo 'waitress-serve --threads $THREAD_COUNT --port 80 snoop.wsgi:application' >> /runserver \
 && chmod +x /runserver /wait

RUN set -e \
 && SECRET_KEY=temp SNOOP_DB='postgresql://snoop:snoop@snoop-pg:5432/snoop' ./manage.py collectstatic --noinput

CMD /wait && /runserver
