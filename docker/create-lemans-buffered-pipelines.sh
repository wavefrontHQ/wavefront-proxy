#!/usr/bin/env bash
set -uo pipefail

#if [ -z "$STREAM_NAME" ]; then
#  echo "Set STREAM_NAME before running this script"
#  exit 1
#fi
STREAM_NAME="wf-test-stream"
RECEIVER_NAME="$STREAM_NAME-receiver"
LEMANS_RESOURCE_SERVER="lemans-resource-server:8001"
RECEIVER_URI=http://uberserver:8082/report/le-mans
CSP_SECRET="hnIsmQ7VH1JPvshExJMmEY7Hm1FAxLstxOI:f3js2cGPbfLerbXUGquhRarARiK3IcVRCJTby7LMODFCWpCSF5"

token_file=$(mktemp)

printf "\n\nauthenticating\n"
curl --fail-with-body --location --request POST 'https://console-stg.cloud.vmware.com/csp/gateway/am/api/auth/authorize' \
  --header 'Content-Type: application/x-www-form-urlencoded' \
  --data-urlencode 'grant_type=client_credentials' \
  -u $CSP_SECRET \
  -o "$token_file"

CSP_AUTH_TOKEN="$(jq -r .access_token  "$token_file")"

printf "\n\ncreating consumer receiver for wavefront\n"

receiver_json_file="$(mktemp)"
printf "\n$receiver_json_file\n"
cat <<-JSON >"$receiver_json_file"
{
  "name": "$RECEIVER_NAME",
  "address": "$RECEIVER_URI",
  "useHttp2": false
}
JSON

curl --location --fail-with-body --request POST "http://$LEMANS_RESOURCE_SERVER/le-mans/v2/resources/receivers" \
  --header "x-xenon-auth-token: $CSP_AUTH_TOKEN" \
  --header 'Content-Type: application/json' \
  --data @"$receiver_json_file"

printf "\n\ncreating consumer receiver starter for wavefront\n"

consumer_receiver_starter_json_file="$(mktemp)"
cat <<-JSON >"$consumer_receiver_starter_json_file"
{
   "name": "$STREAM_NAME-consumer",
   "factoryLink": "/le-mans/consumers/kafka",
   "startJsonState": "{'topic': '$STREAM_NAME', 'retryTopic': '$STREAM_NAME', 'statusCodesToRetryIndefinitely': [503], 'contentType': 'text/plain', 'receiverLink': '/le-mans/v2/resources/receivers/$STREAM_NAME-receiver', 'maxRetryLimit': 10000, 'kafkaProperties': {'group.id': 'le-mans', 'fetch.min.bytes': 1, 'key.deserializer': 'org.apache.kafka.common.serialization.StringDeserializer', 'max.poll.records': 150, 'max.partition.fetch.bytes': 2097152, 'auto.offset.reset': 'latest', 'bootstrap.servers': 'kafka:9092', 'value.deserializer': 'org.apache.kafka.common.serialization.StringDeserializer'}, 'kafkaProducerPath': '/le-mans/receivers/kafka-producer/$STREAM_NAME-producer'}"
}
JSON

curl --location --fail-with-body --request POST "http://$LEMANS_RESOURCE_SERVER/le-mans/v2/resources/receiver-starters" \
  --header "x-xenon-auth-token: $CSP_AUTH_TOKEN" \
  --header 'Content-Type: application/json' \
  --data @"$consumer_receiver_starter_json_file"

printf "\n\ncreating producer receiver starter\n"

producer_receiver_starter_json_file="$(mktemp)"
cat <<-JSON >"$producer_receiver_starter_json_file"
{
    "name": "$STREAM_NAME-producer",
    "factoryLink": "/le-mans/receivers/kafka-producer",
    "startJsonState": "{'topicName':'$STREAM_NAME','numberOfPartitions':'1','replicationFactor':'1','retentionPeriod':'7200000','kafkaProperties':{'bootstrap.servers':'kafka:9092','key.serializer':'org.apache.kafka.common.serialization.StringSerializer','value.serializer':'org.apache.kafka.common.serialization.StringSerializer','retries':'0','linger.ms':'5','lemans.KafkaProducerService.KAFKA_KEY':'org_id','partitioner.class':'com.vmware.lemans.receivers.kafka.RoundRobinPartitioner'}}"

}
JSON

curl --location --fail-with-body --request POST "http://$LEMANS_RESOURCE_SERVER/le-mans/v2/resources/receiver-starters" \
  --header "x-xenon-auth-token: $CSP_AUTH_TOKEN" \
  --header 'Content-Type: application/json' \
  --data @"$producer_receiver_starter_json_file"

printf "\n\ncreating producer receiver\n"

producer_receiver_json_file="$(mktemp)"
printf "\n$producer_receiver_json_file"
cat <<-JSON >"$producer_receiver_json_file"
{
    "name": "$STREAM_NAME-kafka-producer-receiver",
    "address": "/le-mans/receivers/kafka-producer/$STREAM_NAME-producer",
    "useHttp2": true
}
JSON

curl --location --fail-with-body --request POST "http://$LEMANS_RESOURCE_SERVER/le-mans/v2/resources/receivers" \
  --header "x-xenon-auth-token: $CSP_AUTH_TOKEN" \
  --header 'Content-Type: application/json' \
  --data @"$producer_receiver_json_file"

printf "\n\ncreating stream\n"

stream_json_file="$(mktemp)"
printf "\n$stream_json_file\n"

cat <<-JSON >"$stream_json_file"
{
    "name": "$STREAM_NAME",
    "deliveryPolicy": "WAIT_ALL",
    "receiverLinks": ["/le-mans/v2/resources/receivers/$STREAM_NAME-kafka-producer-receiver"]
}
JSON

curl --location --fail-with-body --request POST "http://$LEMANS_RESOURCE_SERVER/le-mans/v2/resources/streams" \
  --header "x-xenon-auth-token: $CSP_AUTH_TOKEN" \
  --header 'Content-Type: application/json' \
  --data @"$stream_json_file"

#echo $STREAM_NAME > tmp/lemans_stream_name