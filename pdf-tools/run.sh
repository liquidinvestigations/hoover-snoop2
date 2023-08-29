#!/bin/bash -ex

cd "$(dirname ${BASH_SOURCE[0]})"

nodejs extract-text.js "$@"
