/*
 * Copyright 2023 INDATACORE io.indatacore.kafka.remote.jxTransformers
 *
 * Licensed under the INDATACORE License;
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License by Contacting INDATACORE
 *
 * http://www.indatacore.com
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.testkafka.kafka.producer;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;


@Service
@RequiredArgsConstructor
public class TopicProducer {

    @Value("${kafka.default.topic}")
    private String topicName;
    @Value("${kafka.default.bootstrapServers}")
    private String bootstrapServerss;
    @Value("${kafka.default.message}")
    private String message;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private static final Logger LOGGER = LoggerFactory.getLogger(TopicProducer.class);


    private String responseStatusOfKafka;

    @Value("${security.protocol.kafka}")
    private String securityProtocol;

    @Value("${sasl.mechanism.kafka}")
    private String saslMechanism;

    @Value("${sasl.jaas.config.kafka}")
    private String saslJaasConfig;


    public void send() {
        System.out.println("Payload enviado: " + message);
        System.out.println("topicName : " + topicName);
        try {
            System.out.println("step : producerConfigs");
            final Map<String, Object> producerConfigs = new HashMap<>(kafkaTemplate.getProducerFactory().getConfigurationProperties());
            producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServerss);
            producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            producerConfigs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);
            producerConfigs.put("security.protocol", securityProtocol);
            producerConfigs.put("sasl.mechanism", saslMechanism);
            producerConfigs.put("sasl.jaas.config", saslJaasConfig);
            final long timestampMillis = System.currentTimeMillis();
            final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
            final String timestampString = dateFormat.format(new Date(timestampMillis));
            final ProducerRecord<String, String> record = new ProducerRecord<>(topicName, message);
            record.headers().add("timestamp", timestampString.getBytes("UTF-8"));
            final ProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerConfigs);
            final KafkaTemplate<String, String> modifiedKafkaTemplate = new KafkaTemplate<>(producerFactory);
            final ListenableFuture<SendResult<String, String>> future = modifiedKafkaTemplate.send(record);
            final SendResult<String, String> result = future.get();
            future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

                @Override
                public void onSuccess(final SendResult<String, String> result) {
                    System.out.println("onSuccess : " + result);
                    System.out.println("responseStatusOfKafka : " + responseStatusOfKafka);
                }

                @Override
                public void onFailure(final Throwable ex) {
                    System.out.println("onFailure : " + ex.getMessage());
                    LOGGER.error("Error in send Message Throwable result: " + ex.toString());
                    System.out.println("responseStatusOfKafka : " + responseStatusOfKafka);
                }


            });
        } catch (final Exception e) {
            LOGGER.error("Error in send Message Execution : " + e.toString());
        }
    }
}
