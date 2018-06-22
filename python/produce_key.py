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
import random 

producer = confluent_kafka.Producer({'bootstrap.servers': 'localhost:9092'})
counter = 0
while True:
    key = random.randint(0, 1000)
    producer.produce('test', 'some value, hi mom!', str(key))
    counter = counter + 1
    if ((counter % 100000) == 0):
        producer.flush(60)
