import org.apache.kafka.clients.producer.ProducerConfig.*
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.IntegerSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.kafka.sender.KafkaSender
import reactor.kafka.sender.SenderOptions
import reactor.kafka.sender.SenderRecord
import reactor.kafka.sender.SenderResult
import java.text.SimpleDateFormat
import java.util.*

private val log = LoggerFactory.getLogger(SampleProducer::class.java)

class SampleProducer(bootstrapServers: String) {


    private val sender = KafkaSender.create(
        SenderOptions.create<Int, String>(
            mapOf<String, Any>(
                BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
                CLIENT_ID_CONFIG to "sample-producer",
                ACKS_CONFIG to "all",
                KEY_SERIALIZER_CLASS_CONFIG to IntegerSerializer::class.java,
                VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java
            )
        )
    )

    private val dateFormat = SimpleDateFormat("HH:mm:ss:SSS z dd MMM yyyy")

    fun sendMessages(topic: String?, count: Int): Flux<SenderResult<Int>> {
        return sender.send(Flux.range(1, count)
            .map { SenderRecord.create(ProducerRecord(topic, it, "Message_$it"), it) })
            .doOnError { e -> log.error("Send failed", e) }
            .doOnNext { r ->
                val metadata: RecordMetadata = r.recordMetadata()
                System.out.printf(
                    "Message %d sent successfully, topic-partition=%s-%d offset=%d timestamp=%s\n",
                    r.correlationMetadata(),
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset(),
                    dateFormat.format(Date(metadata.timestamp()))
                )
            }
    }

    fun close() {
        sender.close()
    }
}