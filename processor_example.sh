#!/bin/bash

echo "Processing: $1"
sleep $[$[RANDOM] % 5]
echo "Finished: $1"
exit 0
