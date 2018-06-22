/*
 * kafka-producer.cc
 * Copyright (C) 2018 gaspar_d </var/spool/mail/gaspar_d>
 *
 * Distributed under terms of the MIT license.
 */

#include <iostream>
#include <librdkafka/rdkafka.h>


int main(int argc, char **argv) {
	char errstr[512];

	/* Kafka configuration */
	auto conf = rd_kafka_conf_new();
	rd_kafka_conf_set(conf, "bootstrap.servers", "localhost:9092", errstr, sizeof(errstr));
	auto producer = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
	auto topic = rd_kafka_topic_new(producer, "test", NULL);

	/* Writing to Kafka */
	while (true) {
		rd_kafka_produce(
				topic,
				RD_KAFKA_PARTITION_UA,
				RD_KAFKA_MSG_F_BLOCK,
				(void*) "Hello!", sizeof("Hello!"),
				NULL, 0,
				NULL);
	}

	return 0;
}
