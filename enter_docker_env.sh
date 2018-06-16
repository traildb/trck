#!/bin/bash

DOCKER_ENV="771945457201.dkr.ecr.us-west-2.amazonaws.com/trck:latest"

docker run --rm -v $PWD:/opt/trck -v /mnt/data:/mnt/data -w /opt/trck -it $DOCKER_ENV /bin/bash
