# Navigating the documentation

This project implements data processing, data storage and metadata extraction,
with the end result being an index that can be used for complex searches.


## Collections

Data processed by this service is split into Collections, which store their
data in instances of databases of various types. See the
[snoop.data.collections][] module for more details.


### SQL Database

For each collection, the database models defined in [snoop.data.models][] are
deployed in a separate database for it. The structures defined here are mapped
by Django ORM to SQL; we currently use PostgresQL as a backend.


### Blobs / object storage

The rows in [snoop.data.models.Blob][] have their data stored in a separate
object storage (now just some files on disk). Since the primary key is
extracted from the content hash, this table de-duplicates all data in the
collection, both for input data, and also for intermediary and output data.


### Elasticsearch Index

The results of the metadata extraction are sent to Elasticsearch for searching
The searching itself is not handled on this service. See
[snoop.data.indexing][] for the details on Elasticsearch data mapping, and the
implementation of indexing.


## Tasks

Work done by this service is tracked and scheduled using an improvised system
for running directed acyclic graphs (DAGs) of [idempotent
  tasks][snoop.data.models.Task].


#### Ingestion and archive extraction tasks

The data processing pipeline starts in [snoop.data.filesystem][] with tasks
that walk directories, extract archives and extract attachments from their
emails. Documents may also be converted into more usable formats here.


#### Document processing and metadata extraction tasks

With the de-duplicated documents identified from the previous step, further
processing is done in [snoop.data.digests][]. Tasks run here are only concerned
with extracting metadata from a single document, and then making it available
through search and API. The [snoop.data.digests.launch][] task schedules more
tasks from [snoop.data.analyzers.__init__][] to run, and then another task
[snoop.data.digests.gather][] combines their results.


#### Indexing tasks

After [snoop.data.digests.gather][] finishes running, its result is stored as
the [Digest result][snoop.data.models.Digest.result]. The last task in the
chain, [snoop.data.digests.index][], uploads this result to elasticsearch.

The results are stored as Blobs for easy re-indexing and fast API response
times.


## Django

We use tend to use Django framework to its fullest, so all usual conventions apply here:

- Scripts are called [`management commands`][snoop.data.management.commands.__init__]
  and are run with `./manage.py`.
- URL routes are in [snoop.urls][] and [snoop.data.urls][].
- Settings are in [snoop.defaultsettings][].
