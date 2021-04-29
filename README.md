# kafka-sample

To start services in background:
`docker-compose up -d`

To stop services:
`docker-compose down`

Create topic:
`docker exec -t kafka kafka-topics.sh --bootstrap-server :9092 --create --topic seb-demo --partitions 1 --replication-factor 1`

List topics:
`docker exec -t kafka kafka-topics.sh --bootstrap-server :9092 --list`

Describe topics:
`docker exec -t kafka kafka-topics.sh --bootstrap-server :9092 --describe --topic seb-demo`

Seb Producer:
`docker exec -it kafka kafka-console-producer.sh --broker-list :9092 --topic seb-demo`

Consumer:
`docker exec -it kafka kafka-console-consumer.sh --bootstrap-server :9092 --topic seb-demo`