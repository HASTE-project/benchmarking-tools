#!/usr/bin/env bash

SERVER=ben-stream-src-4

rsync -rvd ../.. $SERVER:~/benchmarking-tools

# force pseudo-TTY allocation (to forward SIGINT)
ssh -tt $SERVER 'cd ~/benchmarking-tools; python3 -m haste.benchmarking.streaming_server'