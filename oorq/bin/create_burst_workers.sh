#!/bin/bash

# Usage: ./create_burst_workers.sh #workers queues
# Example: ./create_burst_workers.sh 5 sii import_xml

echo "Creating $1 workers"

for i in `seq 1 $1`;
do
    rq worker --burst ${@:2} &
done
