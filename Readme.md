# Snoop Mk2
A lean and mean rewrite of [snoop][], the Hoover collection analyzer.

[snoop]: https://github.com/hoover/snoop

[![Build Status](https://travis-ci.org/hoover/snoop2.svg?branch=master)](https://travis-ci.org/hoover/snoop2)

### Setup
Snoop2 requires Python 3.6, and we recommend installing dependencies in a
virtualenv.

```shell
pip install -r requirements.txt
./manage.py migrate
```

It also requires a [RabbitMQ](http://www.rabbitmq.com/) server. On
Debian/Ubuntu you can install it with apt:
```shell
sudo apt install rabbitmq-server
```

### Configuration
To override the default settings, create a file `snoop/localsettings.py`,
import the default settings, and override any values:

```python
from .defaultsettings import *
ALLOWED_HOSTS = ['localhost']
```

### Analyzing a collection
Snoop's job is to scan a directory from disk and analyze the files inside. For
this example, we'll clone the [testdata repository][], but you can use any
local directory.

[testdata repository]: https://github.com/hoover/testdata

```shell
git clone https://github.com/hoover/testdata /tmp/testdata
```

First tell snoop about this new collection:
```shell
./manage.py createcollection testdata /tmp/testdata/data
```

Then start the dispatcher, which will periodically scan the directory, and
launch analysis jobs for any new or modified files. You only need one
dispatcher, it will scan all the collections registered in this snoop instance.
```shell
./manage.py rundispatcher
```

You also need at least one worker to do the actual processing:
```shell
./manage.py runworker
```


### Development
Create an admin user:

```shell
./manage.py createsuperuser
```


Run the local development server:

```shell
./manage.py runserver
```

Then, open http://localhost:8000/ in your browser.
