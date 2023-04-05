package com.example.pocsqsspring;

import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.awspring.cloud.messaging.core.QueueMessagingTemplate;
import jakarta.jms.Session;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.core.annotation.Order;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.converter.MappingJackson2MessageConverter;
import org.springframework.jms.support.converter.MessageType;
import org.springframework.jms.support.destination.DynamicDestinationResolver;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.io.IOException;
import java.util.Map;

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

@Testcontainers
@SpringBootTest
@ActiveProfiles("test")
class ListenerTest {

    private static final String QUEUE_NAME = "TestQueuePablo";

    @Container
    static LocalStackContainer localStack =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:0.13.0"))
                    .withEnv("DEFAULT_REGION","us-east-1")
                    .withServices(LocalStackContainer.Service.SQS);

    @BeforeAll
    static void beforeAll() throws IOException, InterruptedException {
        localStack.execInContainer("awslocal", "sqs", "create-queue", "--queue-name", QUEUE_NAME);
    }
    @DynamicPropertySource
    static void overrideConfiguration(DynamicPropertyRegistry registry) {
        registry.add("event-processing.order-event-queue", () -> QUEUE_NAME);
        registry.add("cloud.aws.sqs.endpoint", () -> localStack.getEndpointOverride(SQS));
        registry.add("cloud.aws.credentials.accessKey", localStack::getAccessKey);
        registry.add("cloud.aws.credentials.secretKey", localStack::getSecretKey);
        registry.add("cloud.aws.region.static", localStack::getRegion);
    }

    @TestConfiguration
    static class AwsTestConfig {
        @Bean
        @Order(1)
        public AWSCredentials amazonAWSCredentials() {
            return localStack.getDefaultCredentialsProvider().getCredentials();
        }
        @Bean
        @Order(2)
        public ObjectMapper objectMapper() {
            return new ObjectMapper();
        }
        @Bean
        public SQSConnectionFactory createConnectionFactory() {
            AwsCredentialsProvider awsCredentialsProvider = () -> new AwsCredentials() {
                @Override
                public String accessKeyId() {
                    return localStack.getAccessKey();
                }

                @Override
                public String secretAccessKey() {
                    return localStack.getSecretKey();
                }
            };
            SqsClient client = SqsClient.builder()
                    .credentialsProvider(awsCredentialsProvider)
                    .endpointOverride(localStack.getEndpointOverride(SQS))
                    .region(Region.of(localStack.getRegion()))
                    .build();
            return new SQSConnectionFactory(
                    new ProviderConfiguration(),
                    client);
        }
        @Bean
        public AmazonSQSAsync amazonSQS() {
            return AmazonSQSAsyncClientBuilder.standard()
                    .withCredentials(localStack.getDefaultCredentialsProvider())
                    .withEndpointConfiguration(localStack.getEndpointConfiguration(SQS))
                    .build();
        }
        @Bean
        public QueueMessagingTemplate queueMessagingTemplate(AmazonSQSAsync amazonSQSAsync) {
            return new QueueMessagingTemplate(amazonSQSAsync);
        }

        @Bean // Serialize message content to json using TextMessage
        public MappingJackson2MessageConverter jacksonJmsMessageConverter(ObjectMapper objectMapper) {
            MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
            converter.setObjectMapper(objectMapper);
            converter.setTargetType(MessageType.TEXT);
            converter.setTypeIdPropertyName("_type");
            return converter;
        }

        @Bean
        public DefaultJmsListenerContainerFactory jmsListenerContainerFactory(SQSConnectionFactory connectionFactory,
                                                                              MappingJackson2MessageConverter jacksonJmsMessageConverter) {
            DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
            factory.setConnectionFactory(connectionFactory);
            factory.setDestinationResolver(new DynamicDestinationResolver());
            factory.setMaxMessagesPerTask(100);
            factory.setConcurrency("1-20");
            factory.setMessageConverter(jacksonJmsMessageConverter);
            factory.setSessionAcknowledgeMode(Session.CLIENT_ACKNOWLEDGE);
            return factory;
        }

        @Bean
        public JmsTemplate defaultJmsTemplate(SQSConnectionFactory connectionFactory, ObjectMapper objectMapper) {
            JmsTemplate jmsTemplate = new JmsTemplate(connectionFactory);
            jmsTemplate.setMessageConverter(jacksonJmsMessageConverter(objectMapper));
            return jmsTemplate;
        }
    }

    @Autowired
    private QueueMessagingTemplate queueMessagingTemplate;

    @Test
    void messageShouldBeUploadedToBucketOnceConsumedFromQueue() {
        queueMessagingTemplate.send(QUEUE_NAME, new GenericMessage<>("""
        {
           "message": "teste"
        }
      """, Map.of("_type", "application/json")));
    }
}