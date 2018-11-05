#!/bin/bash
#
# Build production image from the currently checked out version
# of ListenBrainz and push it to Docker Hub, with an optional
# tag (which defaults to "latest")
#
# Usage:
#   $ ./push.sh [tag]

cd "$(dirname "${BASH_SOURCE[0]}")/"

TAG=${2:-beta}
echo "building for env $ENV tag $TAG"
docker build -t metabrainz/hadoop-yarn:$TAG -f Dockerfile . && \
    docker push metabrainz/hadoop-yarn:$TAG
