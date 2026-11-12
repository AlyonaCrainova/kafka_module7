package org.example;


import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;


import java.io.IOException;


public class SchemaRegistryHelper {
    public static void registerSchema(SchemaRegistryClient client, String topic, String schemaString) {
        ParsedSchema schema = new JsonSchema(schemaString);
        try {
            client.register(topic, schema);
            System.out.println("Schema registered for " + topic);
        } catch (IOException | RestClientException e) {
            System.err.println("Failed to register schema: " + e.getMessage());
        }
    }
}