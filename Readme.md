# Snoop Mk2
A lean and mean rewrite of [snoop](), the Hoover collection analyzer.

### Setup
Snoop2 requires Python 3.6, and we recommend installing dependencies in a
virtualenv.

```shell
pip install -r requirements.txt
./manage.py migrate
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

Then, open http://localhost:5000/ in your browser.
