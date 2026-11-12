package org.example;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.WakeupException;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class UserConsumer {
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

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers); // устанавливаем адрес для первого подключения

        String groupId = System.getenv("GROUP_ID");
        if (groupId == null || groupId.isEmpty()) {
            System.err.println("Ошибка: переменная GROUP_ID не установлена!");
            return;
        }

        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId); // уникальный идентификатор группы
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Отключаем автокоммит
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // чтение с самого начала

        props.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, System.getenv().getOrDefault("SSL_TRUSTSTORE_LOCATION", "kafka-0-creds/kafka.kafka-0.truststore.jks")); // Truststore
        props.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, System.getenv().getOrDefault("SSL_TRUSTSTORE_PASSWORD", "551288d")); // Truststore password
        props.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, ""); // Отключение проверки hostname

        // Конфигурация SASL
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=\"consumer\" password=\"password\";");

        KafkaConsumer<String, User> consumer = new KafkaConsumer<>(props);

        //Graceful shutdown для корректной остановки
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                System.out.println("Ctrl+C...");
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt(); // Восстанавливаем статус прерывания потока
                }
            }
        });


        try {

            consumer.subscribe(Collections.singletonList(topic));
            while (true) {
                ConsumerRecords<String, User> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) {

                    continue;
                }

                // Обрабатываем сообщения из пачки
                for (ConsumerRecord<String, User> record : records) {
                    System.out.printf("Message on %s:%n%s%n", record.topic(), record.value());
                    if (record.headers() != null) {
                        System.out.printf("Headers: %s%n", record.headers());
                    }
                }

                consumer.commitAsync(); // ручной коммит смещения для пачки
            }

        } catch (WakeupException e) { // выброшено Graceful shutdown
            // Прерываем цикл while при получении сигнала остановки
            System.out.println("Получен сигнал WakeupException. Выход из цикла опроса.");
        } catch (Exception e) {
            // Перехватываем непредвиденные критические ошибки
            System.err.println("Критическая ошибка: " + e.getMessage());
        } finally {
            try {
                consumer.commitSync(Duration.ofSeconds(5)); //синхронная фиксация перед закрытием потребителя или перебалансировкой
            } catch (Exception e) {
                // ...мы его перехватим и прологируем.
                System.err.println("Не удалось выполнить финальный коммит: " + e.getMessage());
            } finally {
                System.out.println("Закрываем консьюмер...");
                consumer.close(); // освобождаем ресурсы
                System.out.println("Консьюмер корректно закрыт.");
            }
        }
    }
}
