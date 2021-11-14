#!/usr/bin/env bash

script_dir="$(cd "$(dirname "$0")" && pwd)"
log_dir="${script_dir}/stdout_logs"
chunk_size=15000000
controller_port_for_clients=25000
controller_port_for_nodes=25100
node_port=25200
root_dir="/bigdata/bpporter/p1-file-system/"

source "${script_dir}/nodes.sh"

echo "Installing..."
go install ../controller/controller.go   || exit 1 # Exit if compile+install fails
go install ../node/node.go || exit 1 # Exit if compile+install fails
echo "Done!"

echo "Creating log directory: ${log_dir}"
mkdir -pv "${log_dir}"

echo "Starting Controller..."
ssh "${controller}" "${HOME}/go/bin/controller ${controller_port_for_clients} ${controller_port_for_nodes} ${chunk_size}" &> "${log_dir}/controller.log" &

echo "Starting Storage Nodes..."
for node in ${nodes[@]}; do
    echo "${node}"
    ssh "${node}" "${HOME}/go/bin/node ${node_port} ${root_dir} ${controller} ${controller_port_for_nodes}" &> "${log_dir}/${node}.log" &
done

echo "Startup complete!"
