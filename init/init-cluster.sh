#!/bin/bash
set -e

BOOTSTRAP=kafka-0:9093

cat > /tmp/admin.properties <<EOF
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="admin" password="admin-secret";
ssl.truststore.location=/certs/truststore.jks
ssl.truststore.password=551288d
EOF

until kafka-broker-api-versions --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties > /dev/null 2>&1; do
  echo "Ждем кластер..."
  sleep 3
done

echo "Создание топиков..."
# 3 партиции, коэффициент репликации 3 (все брокеры кластера)
kafka-topics --create --topic user --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --partitions 3 --replication-factor 3 --if-not-exists
kafka-topics --create --topic topic-nifi --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --partitions 3 --replication-factor 3 --if-not-exists

echo "Настройка топиков..."
kafka-configs --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --entity-type topics --entity-name user --alter --add-config cleanup.policy=delete,retention.ms=300000,segment.bytes=2000

echo "Конфигурация ACLs..."
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --add --allow-principal User:consumer --operation Read --group my-consumer-group-1
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --add --allow-principal User:consumer --operation Describe --topic user
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --add --allow-principal User:consumer --operation Read --topic user

kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --add --allow-principal User:producer --operation Describe --topic user
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --add --allow-principal User:producer --operation Write --topic user

kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --add --allow-principal User:producer_nifi --operation Describe --topic topic-nifi
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --add --allow-principal User:producer_nifi --operation Write --topic topic-nifi

kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --add --allow-principal User:consumer --operation Read --group my-consumer-group-2
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --add --allow-principal User:consumer --operation Describe --topic topic-nifi
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --add --allow-principal User:consumer --operation Read --topic topic-nifi
kafka-acls --bootstrap-server "$BOOTSTRAP" --command-config /tmp/admin.properties --add --allow-principal User:consumer --operation Read --group my-consumer-group-3

echo "Инициализация кластера завершена."
