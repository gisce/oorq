#!/bin/bash

# Usage: ./create_burst_workers.sh #workers queues
# Example: ./create_burst_workers.sh 5 sii import_xml

for i in `seq 1 $1`;
do
    rq worker --burst ${@:2} &
done
