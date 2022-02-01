#! /bin/sh
#
# test.sh
# Copyright (C) 2022 gaspar_d </var/spool/mail/gaspar_d>
#
# Distributed under terms of the MIT license.
#


kafka-topics --bootstrap-server localhost:9092 --create --topic dummy --partitions 1 --if-not-exists
for i in `seq 10`; do echo $i | kafka-console-producer --topic dummy --bootstrap-server localhost:9093; done
timeout -s KILL 50 kafka-console-consumer --bootstrap-server localhost:9092 --topic dummy --group test --from-beginning --consumer-property session.timeout.ms=7000
timeout -s KILL 150 kafka-console-consumer --bootstrap-server localhost:9092 --topic dummy --group test --from-beginning --consumer-property session.timeout.ms=7000 --consumer-property enable.auto.commit=false

echo
echo RESTARTING AFTER 150 seconds
echo

sleep 40

timeout 60 kafka-console-consumer --bootstrap-server localhost:9092 --topic dummy --group test --from-beginning
