package com.flipkart.gap.nfrLoadGenerator.KafkaProducer;

public class Constants {
    /**
     * Parameter for setting the Kafka brokers; for example, "kafka01:9092,kafka02:9092".
     */
    public static String PARAMETER_KAFKA_BROKERS = "kafka_brokers";

    /**
     * Parameter for setting the Kafka topic name.
     */
    public static String PARAMETER_KAFKA_TOPIC = "kafka_topic";

    /**
     * Parameter for setting the Kafka key.
     */
    public static String PARAMETER_KAFKA_KEY = "kafka_key";

    /**
     * Parameter for setting the Kafka message.
     */
    public static String PARAMETER_KAFKA_MESSAGE = "kafka_message";

    /**
     * Parameter for setting Kafka's {@code serializer.class} property.
     */
    public static String PARAMETER_KAFKA_MESSAGE_SERIALIZER = "kafka_message_serializer";

    /**
     * Parameter for setting Kafka's {@code key.serializer.class} property.
     */
    public static String PARAMETER_KAFKA_KEY_SERIALIZER = "kafka_key_serializer";

    /**
     * Parameter for setting encryption. It is optional.
     */
    public static String PARAMETER_KAFKA_COMPRESSION_TYPE = "kafka_compression_type";

    /**
     * Parameter for setting the partition. It is optional.
     */
    public static String PARAMETER_KAFKA_PARTITION = "kafka_partition";

    /**
     * No of producers. It is optional. Defaults to 1
     */
    public static String PARAMETER_KAFKA_PRODUCER_COUNT = "kafka_producer_count";
}
