package com.flipkart.gap.nfrLoadGenerator.KafkaProducer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.jmeter.config.Arguments;

import org.apache.jorphan.logging.LoggingManager;
import org.apache.kafka.clients.producer.*;
import org.apache.log.Logger;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;
import java.util.Random;

public class KafkaProducerSampler extends AbstractJavaSamplerClient {

    private static final Logger log = LoggingManager.getLoggerForClass();
    private Producer<String, byte[]> producers[];
    private ObjectMapper mapper;
    private Random random;

    @Override
    public void setupTest(JavaSamplerContext context) {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, context.getParameter(Constants.PARAMETER_KAFKA_BROKERS));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, context.getParameter(Constants.PARAMETER_KAFKA_KEY_SERIALIZER));
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, context.getParameter(Constants.PARAMETER_KAFKA_MESSAGE_SERIALIZER));
        props.put(ProducerConfig.ACKS_CONFIG, "1");

        String compressionType = context.getParameter(Constants.PARAMETER_KAFKA_COMPRESSION_TYPE);
        if (!Strings.isNullOrEmpty(compressionType)) {
            props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        }

        int noOfProducers = Integer.parseInt(context.getParameter(Constants.PARAMETER_KAFKA_PRODUCER_COUNT));

        if (noOfProducers < 1) {
            noOfProducers = 1;
        }

        producers = new Producer[noOfProducers];
        for (int i = 0; i < noOfProducers; ++i) {
            producers[i] = new KafkaProducer<>(props);
        }

        mapper = new ObjectMapper();

        random = new Random();
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        for (Producer producer : producers) {
            producer.flush();
            producer.close();
        }
    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments defaultParameters = new Arguments();
        defaultParameters.addArgument(Constants.PARAMETER_KAFKA_BROKERS, "${PARAMETER_KAFKA_BROKERS}");
        defaultParameters.addArgument(Constants.PARAMETER_KAFKA_TOPIC, "${PARAMETER_KAFKA_TOPIC}");
        defaultParameters.addArgument(Constants.PARAMETER_KAFKA_KEY, "${PARAMETER_KAFKA_KEY}");
        defaultParameters.addArgument(Constants.PARAMETER_KAFKA_MESSAGE, "${PARAMETER_KAFKA_MESSAGE}");
        defaultParameters.addArgument(Constants.PARAMETER_KAFKA_MESSAGE_SERIALIZER, "org.apache.kafka.common.serialization.ByteArraySerializer");
        defaultParameters.addArgument(Constants.PARAMETER_KAFKA_KEY_SERIALIZER, "org.apache.kafka.common.serialization.StringSerializer");
        defaultParameters.addArgument(Constants.PARAMETER_KAFKA_COMPRESSION_TYPE, null);
        defaultParameters.addArgument(Constants.PARAMETER_KAFKA_PARTITION, null);
        defaultParameters.addArgument(Constants.PARAMETER_KAFKA_PRODUCER_COUNT, "1");
        return defaultParameters;
    }

    @Override
    public SampleResult runTest(JavaSamplerContext context) {
        SampleResult result = newSampleResult();
        String topic = context.getParameter(Constants.PARAMETER_KAFKA_TOPIC);
        String key = context.getParameter(Constants.PARAMETER_KAFKA_KEY);
        String message = context.getParameter(Constants.PARAMETER_KAFKA_MESSAGE);
        sampleResultStart(result, message);

        ProducerRecord<String, byte[]> producerRecord = null;
        String partitionString = context.getParameter(Constants.PARAMETER_KAFKA_PARTITION);
        if (Strings.isNullOrEmpty(partitionString)) {
            try {
                producerRecord = new ProducerRecord<>(topic, key, mapper.writeValueAsBytes(message));
            } catch (JsonProcessingException e) {
                log.error("Byte conversion failure", e);
            }
        } else {
            final int partitionNumber = Integer.parseInt(partitionString);
            try {
                producerRecord = new ProducerRecord<>(topic, partitionNumber, key, mapper.writeValueAsBytes(message));
            } catch (JsonProcessingException e) {
                log.error("Byte conversion failure", e);
            }
        }

        try {
            if (null == producerRecord) {
                log.error("producer record is null");
            } else {
                getProducer().send(producerRecord);
                sampleResultSuccess(result, null);
            }
        } catch (Exception e) {
            sampleResultFailed(result, "500", e);
        }
        return result;
    }

    private SampleResult newSampleResult() {
        SampleResult result = new SampleResult();
        result.setDataEncoding("UTF-8");
        result.setDataType(SampleResult.TEXT);
        return result;
    }

    private void sampleResultStart(SampleResult result, String data) {
        result.setSamplerData(data);
        result.sampleStart();
    }

    private void sampleResultFailed(SampleResult result, String reason) {
        result.sampleEnd();
        result.setSuccessful(false);
        result.setResponseCode(reason);
    }

    private void sampleResultFailed(SampleResult result, String reason, Exception exception) {
        sampleResultFailed(result, reason);
        result.setResponseMessage("Exception: " + exception);
        result.setResponseData(getStackTrace(exception), "UTF-8");
    }

    private String getStackTrace(Exception exception) {
        StringWriter stringWriter = new StringWriter();
        exception.printStackTrace(new PrintWriter(stringWriter));
        return stringWriter.toString();
    }


    private void sampleResultSuccess(SampleResult result, /* @Nullable */ String response) {
        result.sampleEnd();
        result.setSuccessful(true);
        result.setResponseCodeOK();
        if (response != null) {
            result.setResponseData(response, "UTF-8");
        } else {
            result.setResponseData("No response required", "UTF-8");
        }
    }

    private Producer<String, byte[]> getProducer() {
        return producers[random.nextInt(producers.length)];
    }

}
