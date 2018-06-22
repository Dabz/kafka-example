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

producer = confluent_kafka.Producer({'bootstrap.servers': 'localhost:9092', 
    'linger.ms': 100, 
    'batch.num.messages': 1000,
    'queue.buffering.max.messages': 5000,
    'queue.buffering.max.ms': 1000,
    'buffer.memory': 1024 * 1024 * 100
    })


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))

while True:
    producer.produce('test', 'hello!', 'a string as a key', callback=delivery_report)
