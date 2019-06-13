FROM python:3.6-stretch
ENV PYTHONUNBUFFERED 1

RUN set -e \
 && echo 'deb http://deb.debian.org/debian stretch non-free' >> /etc/apt/sources.list \
 && echo 'deb http://deb.debian.org/debian stretch-updates non-free' >> /etc/apt/sources.list \
 && echo 'deb http://security.debian.org stretch/updates non-free' >> /etc/apt/sources.list \
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
  && git clone https://github.com/hoover/magic-definitions.git \
  && ( cd magic-definitions && ( ./build.sh ) && cp magic.mgc /opt/hoover/snoop/ )
ENV PATH="/opt/magic-definitions/file/bin:${PATH}"

RUN wget http://www.five-ten-sg.com/libpst/packages/libpst-0.6.71.tar.gz --progress=dot:giga \
  && tar zxvf libpst-0.6.71.tar.gz \
  && rm -f libpst-0.6.71.tar.gz \
  && mv libpst-0.6.71 /opt/libpst \
  && cd /opt/libpst \
  && ./configure --disable-python --prefix="`pwd`" \
  && make \
  && make install
ENV PATH="/opt/libpst/bin:${PATH}"

COPY . .

ADD https://github.com/ufoscout/docker-compose-wait/releases/download/2.3.0/wait /wait

RUN set -e \
 && echo '#!/bin/bash -e' > /runserver \
 && echo 'waitress-serve --port 80 snoop.wsgi:application' >> /runserver \
 && chmod +x /runserver /wait

RUN set -e \
 && SECRET_KEY=temp SNOOP_AMQP_URL='amqp://localhost' SNOOP_DB='postgresql://snoop:snoop@snoop-pg:5432/snoop' ./manage.py collectstatic --noinput

CMD /wait && /runserver
