# prometheus-kafka-offsets

## Build
    sbt package


## create an RPM

    sbt clean rpm:packageBin

RPM structure:
    /etc/default/prometheus-kafka-offsets
    /etc/init.d/prometheus-kafka-offsets
    /usr/share/prometheus-kafka-offsets/bin/prometheus-kafka-offsets
    /usr/share/prometheus-kafka-offsets/conf
    /usr/share/prometheus-kafka-offsets/conf/app.conf
    /usr/share/prometheus-kafka-offsets/conf/prometheus-kafka-offsets-jaas.conf
    /usr/share/prometheus-kafka-offsets/lib/*
    /var/log/prometheus-kafka-offsets
    /var/run/prometheus-kafka-offsets

## Install RPM

    rpm -ivh prometheus-kafka-offsets-1.0-1.noarch.rpm
    
## Start application

    service prometheus-kafka-offsets start
    
## Check if application is working

open a browser on url: http://localhost:8000/metrics

This should give a response like:

    kafka_logSize{topic="tweets",partition="19"} 124396
    kafka_offset{topic="tweets",consumer="kafka-to-neo4j",partition="19"} 124396
    kafka_lag{topic="tweets",consumer="kafka-to-neo4j",partition="19"} 0
    kafka_offset{topic="tweets",consumer="kafka-to-neo4j2",partition="19"} 71086
    kafka_lag{topic="tweets",consumer="kafka-to-neo4j2",partition="19"} 53310
    kafka_logSize{topic="tweets",partition="35"} 124414
    kafka_offset{topic="tweets",consumer="kafka-to-neo4j",partition="35"} 124414
    kafka_lag{topic="tweets",consumer="kafka-to-neo4j",partition="35"} 0
    kafka_offset{topic="tweets",consumer="kafka-to-neo4j2",partition="35"} 71101
    kafka_lag{topic="tweets",consumer="kafka-to-neo4j2",partition="35"} 53313