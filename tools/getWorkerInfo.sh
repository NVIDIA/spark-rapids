#!/bin/bash

output_file=/tmp/system_props.yaml
num_workers=$1
worker_ip=$2
echo "Fetching system information from worker - $worker_ip"
scp -q ./discoveryScript.sh "$worker_ip":/tmp
ssh "$worker_ip" "bash /tmp/discoveryScript.sh $num_workers $output_file"
scp -q "$worker_ip":$output_file ./
echo -e "\nYAML file copied to driver"

