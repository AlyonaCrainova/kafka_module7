package org.example;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.common.header.internals.RecordHeader;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class Producer {
    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) {

        String schemaRegistryUrl = System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081");
        String topic = "user";
        Properties props = new Properties();
        //получаем из переменной среды адрес брокера для первого подключения
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            System.err.println("Ошибка: переменная KAFKA_BOOTSTRAP_SERVERS не установлена!");
            return;
        }

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers); // устанавливаем адрес для первого подключения
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName());
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("auto.register.schemas", true);
        props.put("use.latest.version", true);

        // Явные таймауты: при недоступности брокеров send() вернёт ошибку, а не будет молча висеть
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000"); // лимит ожидания метаданных и места в буфере
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "120000"); // общий лимит на доставку сообщения

        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, System.getenv().getOrDefault("SSL_TRUSTSTORE_LOCATION", "kafka-0-creds/kafka.kafka-0.truststore.jks")); // Truststore
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, System.getenv().getOrDefault("SSL_TRUSTSTORE_PASSWORD", "551288d")); // Truststore password
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ""); // Отключение проверки hostname

        // Конфигурация SASL
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"producer\" password=\"password\";");

        SchemaRegistryClient schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryUrl, 10);
        // Register schema
        String userSchema = "{ \"$schema\": \"http://json-schema.org/draft-07/schema#\", \"title\": \"User\", \"type\": \"object\", \"properties\": { \"name\": { \"type\": \"string\" }, \"favoriteNumber\": { \"type\": \"integer\" }, \"favoriteColor\": { \"type\": \"string\" } }, \"required\": [\"name\", \"favoriteNumber\", \"favoriteColor\"] }";
        SchemaRegistryHelper.registerSchema(schemaRegistryClient, topic, userSchema);

        // Один продюсер на все сообщения: один SSL-контекст, одно SASL-подключение, один сетевой поток
        User[] users = {
                new User("First user", 42, "blue"),
                new User("Second user", 7, "black"),
                new User("Second user", 0, "pink")
        };
        String[] keys = {"first-user", "second-user", "second-user"};

        try (KafkaProducer<String, User> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < users.length; i++) {
                ProducerRecord<String, User> record = new ProducerRecord<>(topic, keys[i], users[i]);
                if (i == 0) { // тестовый заголовок на первом сообщении
                    record.headers().add(new RecordHeader("myTestHeader", "header values are binary".getBytes()));
                }

                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        logger.info("Сообщение успешно отправлено в Kafka: " + metadata.toString());
                        System.out.println(metadata);
                    } else {
                        logger.error("Ошибка при отправке сообщения", exception);
                    }
                });
            }

            producer.flush(); // дожидаемся доставки всех сообщений перед закрытием
        } catch (Throwable e) {
            logger.error("Ошибка в Kafka Producer", e);
        }
    }
}
