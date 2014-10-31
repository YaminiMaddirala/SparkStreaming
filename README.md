SparkStreaming
==============

My Kafka Cheat Sheet for streaming exmaples:
********************************************
Start Zookeeper : bin/zookeeper-server-start.sh config/zookeeper.properties;

Start Apache Kafka : bin/kafka-server-start.sh config/server.properties

Create a Topic : bin/kafka-topics.sh --create --zookeeper host:port --replication-factor 1 --partitions 1 --topic TopicName

List Topics : bin/kafka-topics.sh --list --zookeeper host:port

Produce Mesages : bin/kafka-console-producer.sh --broker-list host:port --topic TopicName

Consuming Messages : bin/kafka-console-consumer.sh --zookeeper host:port --topic TopicName --from-beginning
