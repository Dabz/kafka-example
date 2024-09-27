#!/bin/bash
set -e

export SCHEMA_REGISTRY_URL=https://psrc-08kx6.europe-west1.gcp.confluent.cloud
export SCHEMA_REGISTRY_USER_INFO=LK5UDLPU5FNIABCO:/eaTgBJN1T8d9VtbqBusBPKMRIRTozGnvrLA3TuwVnebfTfMD81mkQOmjhpnKhwY
export BOOTSTRAP_SERVER=pkc-4r297.europe-west1.gcp.confluent.cloud:9092
export KAFKA_API_KEY=T4EDQLMDMUBGHIGC
export KAFKA_API_SECRET=yQNm8U4CLG/lcuT7r7rPeYTgN93sL/0OhWfFv/wlM1glmjsldDAo8BIjgWCOCyA8
./setup-schema.sh

for i in `seq 10`; do
  echo '{ "firstName": "Damien", "lastName": "Gasparina", "ssn": "'$i'"}' | docker run -i --network host confluentinc/cp-schema-registry:7.5.1 \
    kafka-avro-console-producer \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --property schema.registry.url=$SCHEMA_REGISTRY_URL \
    --property basic.auth.credentials.source=USER_INFO \
    --property basic.auth.user.info=$SCHEMA_REGISTRY_USER_INFO \
    --property value.schema.id=`curl -u $SCHEMA_REGISTRY_USER_INFO $SCHEMA_REGISTRY_URL/subjects/customer-value/versions/1 | jq .id` \
    --producer-property security.protocol=SASL_SSL \
    --producer-property "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='T4EDQLMDMUBGHIGC' password='yQNm8U4CLG/lcuT7r7rPeYTgN93sL/0OhWfFv/wlM1glmjsldDAo8BIjgWCOCyA8';" \
    --producer-property sasl.mechanism=PLAIN \
    --topic customer
done

for i in `seq 10`; do
  echo '{ "firstName": "Damien", "lastName": "Gasparina", "etsn": '$i'}' | docker run -i --network host confluentinc/cp-schema-registry:7.5.1 \
    kafka-avro-console-producer \
    --bootstrap-server $BOOTSTRAP_SERVER \
    --property schema.registry.url=$SCHEMA_REGISTRY_URL \
    --property basic.auth.credentials.source=USER_INFO \
    --property basic.auth.user.info=$SCHEMA_REGISTRY_USER_INFO \
    --property value.schema.id=`curl -u $SCHEMA_REGISTRY_USER_INFO $SCHEMA_REGISTRY_URL/subjects/customer-value/versions/2 | jq .id` \
    --producer-property security.protocol=SASL_SSL \
    --producer-property "sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username='T4EDQLMDMUBGHIGC' password='yQNm8U4CLG/lcuT7r7rPeYTgN93sL/0OhWfFv/wlM1glmjsldDAo8BIjgWCOCyA8';" \
    --producer-property sasl.mechanism=PLAIN \
    --topic customer
done
