#!/bin/bash

# Usage: ./create_burst_workers.sh #workers queues
# Example: ./create_burst_workers.sh 5 sii import_xml

echo "Creating $1 workers"

if [ -z "$1" ] && [ -z "$2" ]; then    
    $worker_name = $2
else
    $worker_name = "default"
fi

for i in `seq 1 $1`;
do
    rq worker $worker_name --burst ${@:2} &
done
