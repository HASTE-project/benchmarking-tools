#!/usr/bin/env bash

SERVER=ben-stream-src-2

rsync -rvd ../.. $SERVER:~/benchmarking-tools

ssh -tt $SERVER 'cd ~/benchmarking-tools; python3 -m haste.benchmarking.streaming_server'