#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 gaspar_d </var/spool/mail/gaspar_d>
#
# Distributed under terms of the MIT license.

"""

"""

import confluent_kafka


consumer = confluent_kafka.Consumer({'bootstrap.servers': 'localhost:9092', 'group.id': 'simple_consumer'})

consumer.subscribe(['test'])

while True:
    msg = consumer.poll(60)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break
    print('received message %s' % msg.value().decode('utf-8'))

consumer.close()
        


