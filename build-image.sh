#!/bin/bash

echo ""

echo -e "\nbuild docker hadoop image\n"
docker build -t metabrainz/hadoop:beta .

echo ""
