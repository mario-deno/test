#!/bin/sh
#Version 2.1 with dcos-cli dockers support
#dcos-cli run on docker in this case

env=$1
json=$2
#echo "$json"
serviceId=$(cat "$json" | jq -r '.id')
#echo SERVICEID -> "$serviceId"
test=$(docker exec -i dcos-$env dcos marathon app list)
echo "$test" | grep "$serviceId "
if [ $? -eq 0 ];
then docker exec -i dcos-$env dcos marathon app update $serviceId < $json;
else docker exec -i dcos-$env dcos marathon app add < $json;
fi
