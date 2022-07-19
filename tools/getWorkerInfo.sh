#!/bin/bash

output_file=/tmp/system_props.yaml
for worker_ip in "$@"
do
    echo "Fetching system information from worker - $worker_ip"
    scp -q ./discoveryScript.sh "$worker_ip":/tmp
    ssh "$worker_ip" "bash /tmp/discoveryScript.sh $output_file"
    scp -q "$worker_ip":$output_file ./
    echo -e "\nYAML file copied to driver"
done
