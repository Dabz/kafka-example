#!/bin/bash
set -e

export SCHEMA_REGISTRY_URL=http://archlinux:8081
export SCHEMA_REGISTRY_USER_INFO=mds:secret
export BOOTSTRAP_SERVER=archlinux:9092
./setup-schema.sh

for i in `seq 10`; do
  echo '{ "firstName": "Damien", "lastName": "Gasparina", "ssn": "'$i'"}' | docker run -i --network host confluentinc/cp-schema-registry:7.5.1 \
    kafka-avro-console-producer \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --property schema.registry.url=$SCHEMA_REGISTRY_URL \
    --property basic.auth.credentials.source=USER_INFO \
    --property basic.auth.user.info=$SCHEMA_REGISTRY_USER_INFO \
    --property value.schema.id=`curl -u $SCHEMA_REGISTRY_USER_INFO $SCHEMA_REGISTRY_URL/subjects/customer-value/versions/1 | jq .id` \
    --topic customer
done

for i in `seq 10`; do
  echo '{ "firstName": "Damien", "lastName": "Gasparina", "ssn": '$i'}' | docker run -i --network host confluentinc/cp-schema-registry:7.5.1 \
    kafka-avro-console-producer \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --property schema.registry.url=$SCHEMA_REGISTRY_URL \
    --property basic.auth.credentials.source=USER_INFO \
    --property basic.auth.user.info=$SCHEMA_REGISTRY_USER_INFO \
    --property value.schema.id=`curl -u $SCHEMA_REGISTRY_USER_INFO $SCHEMA_REGISTRY_URL/subjects/customer-value/versions/2 | jq .id` \
    --topic customer
done
