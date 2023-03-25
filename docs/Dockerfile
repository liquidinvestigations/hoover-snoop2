FROM python:3.9

RUN mkdir -p /tmp-docs
WORKDIR /tmp-docs

ADD ./requirements-mkdocs.txt ./
RUN pip install -r requirements-mkdocs.txt

ADD ./mkdocstrings_handlers_python_override.py ./
RUN cp mkdocstrings_handlers_python_override.py /usr/local/lib/python3.9/site-packages/mkdocstrings/handlers/python.py

# RUN pip install pip-tools
