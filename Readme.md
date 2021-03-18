# Snoop Mk2

Collection analyzer for Hoover.

## Setup

Snoop is recommended to run in Nomad using [Liquid Investigations][].

[Liquid Investigations]: https://github.com/liquidinvestigations


### Run tests locally

Install the drone CLI binary from their website onto your PATH. Install Docker CE, latest version.

Then, run `./run-tests` with arguments you'd normally pass to `py.test`, like this:

    ./run-tests -vvv -x -k mime

You need at least 8GB of RAM and 10GB of free disk space on `/opt` for all the docker images. The tests will take around 2-3min to start, then run for another 5-15min depending on your CPU and bandwidth.


### Documentation

You can build and view the documentation by running:

    ./serve-docs

The page at `http://localhost:8000` will auto-refresh when you edit the code.


### Authentication and Security

This website leaves out access control, authentication and user management to other components.

The admin sites are public, logging in any visitor as an administrator called "root".
Please firewall the exposed port and run it behind authentication.
