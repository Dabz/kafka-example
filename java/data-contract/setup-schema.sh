#!/bin/bash
set -e

SCHEMA_V1=`cat customer-v1.json | tr -d '\n' | jq tojson`
SCHEMA_V2=`cat customer-v2.json | tr -d '\n' | jq tojson`

SR_CONFIG=`cat << EOF
{
  "compatibilityGroup": "application.major.version",
  "defaultMetadata": {
    "properties": {
       "application.major.version": "1"
    }
  }
}
EOF
`


API_V1=`cat << EOF
{
  "schema": $SCHEMA_V1,
  "schemaType": "AVRO",
  "metadata": {
    "properties": {
      "application.major.version": "1"
    }
  },
  "ruleSet": {
    "domainRules": [
    ]
  }
}
EOF
`

API_V2=`cat << EOF
{
  "schema": $SCHEMA_V2,
  "schemaType": "AVRO",
  "metadata": {
    "properties": {
      "application.major.version": "2"
    }
  },
  "ruleSet": {
     "migrationRules": [
       {
         "name": "ssnToInt",
         "kind": "TRANSFORM",
         "type": "JSONATA",
         "mode": "UPGRADE",
         "expr": "\\$merge([\\$sift(\\$, function(\\$v, \\$k) {\\$k != 'ssn'}), {'ssn': \\$number(\\$.'ssn')}])"
       },
         {
         "name": "ssnToString",
         "kind": "TRANSFORM",
         "type": "JSONATA",
         "mode": "DOWNGRADE",
         "expr": "\\$merge([\\$sift(\\$, function(\\$v, \\$k) {\\$k != 'ssn'}), {'ssn': \\$formatNumber(\\$.'ssn', '00')}])"
         }
     ]
  }
}
EOF
`


curl -u $SCHEMA_REGISTRY_USER_INFO $SCHEMA_REGISTRY_URL/config/customer-value -H 'Content-Type:application/json' -X PUT --data "$SR_CONFIG"
curl -u $SCHEMA_REGISTRY_USER_INFO $SCHEMA_REGISTRY_URL/subjects/customer-value/versions -H 'Content-Type:application/json' -X POST --data "$API_V1"
curl -u $SCHEMA_REGISTRY_USER_INFO $SCHEMA_REGISTRY_URL/subjects/customer-value/versions -H 'Content-Type:application/json' -X POST --data "$API_V2"