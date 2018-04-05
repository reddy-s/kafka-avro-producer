package io.dataglitter.kafka.producer.config;

import java.util.Map;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;

import io.dataglitter.kafka.commons.KafkaAvroSerializerWithSchemaName;

/**
 * Created by reddys on 10/03/2018.
 */

@Configuration
@EnableKafka
public class KafkaConfig {

    private static final String SCHEMA_REGISTRY_URL_KEY = "schema.registry.url";

    @Value("${schema.registry.url}")
    private String schemaRegistryUrl;

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    private final KafkaProperties properties;

    @Autowired
    public KafkaConfig( KafkaProperties properties) {
        this.properties = properties;
    }

    @Bean
    public ProducerFactory<?, ?> kafkaProducerFactory() {
        Map<String, Object> producerProperties = properties.buildProducerProperties();
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializerWithSchemaName.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializerWithSchemaName.class);
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerProperties.put(SCHEMA_REGISTRY_URL_KEY, schemaRegistryUrl);
        return new DefaultKafkaProducerFactory<Object, Object>(producerProperties);
    }

}
