package com.nikitocheck.sample

import org.apache.kafka.clients.consumer.ConsumerConfig.*
import org.apache.kafka.common.serialization.IntegerDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.kafka.receiver.*
import java.text.SimpleDateFormat
import java.util.*


private val log = LoggerFactory.getLogger(SampleConsumer::class.java)

class SampleConsumer(bootstrapServers: String) {
    private val receiverOptions: ReceiverOptions<Int, String> = ReceiverOptions.create(
        mapOf<String, Any>(
            BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            CLIENT_ID_CONFIG to "com.nikitocheck.sample-consumer",
            GROUP_ID_CONFIG to "com.nikitocheck.sample-group",
            KEY_DESERIALIZER_CLASS_CONFIG to IntegerDeserializer::class.java,
            VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            AUTO_OFFSET_RESET_CONFIG to "earliest"
        )
    )
    private val dateFormat = SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy")


    fun consumeMessages(topic: String): Flux<SampleMessage> {
        val options: ReceiverOptions<Int, String> = receiverOptions.subscription(listOf(topic))
            .addAssignListener { partitions: Collection<ReceiverPartition?>? ->
                log.debug(
                    "onPartitionsAssigned {}",
                    partitions
                )
            }
            .addRevokeListener { partitions: Collection<ReceiverPartition?>? ->
                log.debug(
                    "onPartitionsRevoked {}",
                    partitions
                )
            }
        val kafkaFlux = KafkaReceiver.create(options).receive()
        return kafkaFlux.map { record: ReceiverRecord<Int, String> ->
            val offset = record.receiverOffset()
            System.out.printf(
                "Received message: topic-partition=%s offset=%d timestamp=%s key=%d value=%s\n",
                offset.topicPartition(),
                offset.offset(),
                dateFormat.format(Date(record.timestamp())),
                record.key(),
                record.value()
            )
            offset.acknowledge()

            SampleMessage(record.key(), record.value(), record.receiverOffset())
        }
    }
}