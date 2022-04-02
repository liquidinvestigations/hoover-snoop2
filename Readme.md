# Snoop Mk2

Collection analyzer for Hoover.

## Setup

Snoop is recommended to run in Nomad using [Liquid Investigations][].

[Liquid Investigations]: https://github.com/liquidinvestigations


### Run tests locally

Install the drone CLI binary from their website onto your PATH. Install Docker
CE, latest version.

Then, run `./run-tests` with arguments you'd normally pass to `py.test`, like this:

    ./run-tests -vvv -x -k mime

You need at least 8GB of RAM and 10GB of free disk space on `/opt` for all the
docker images. The tests will take around 2-3min to start, then run for another
5-15min depending on your CPU and bandwidth.


If you want to interactively work on the tests, do the following:

    # after the first failure, sleep for a very long time; leave this command running:
    ./run-tests '-x || sleep 10000'

    # in another shell, identify the container running the test
    docker ps  | grep hoover-snoop2
    # and exec a shell into it (where xxxx is your docker container ID from above)
    docker exec -it xxxx bash

    # run tests as desired, with all the extra services staying alive!
    py.test


**Note:** if you need to close/restart `./run-tests` script, remember to do
`docker rm -f $(docker ps -qa)`, because interrupting `drone exec` does not
clean up docker containers.



### [Documentation](https://hoover-snoop2.readthedocs.io/)

Documentation is hosted by [https://readthedocs.org/]().

You can build and view the documentation by running:

    ./serve-docs

The page at [http://localhost:8000]() will auto-refresh when you edit the code.


### Authentication and Security

This website leaves out access control, authentication and user management to other components.

The admin sites are public, logging in any visitor as an administrator called "root".
Please firewall the exposed port and run it behind authentication.
