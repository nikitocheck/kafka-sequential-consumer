import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitLast
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName

const val SAMPLE_TOPIC = "sample-topic"
const val MESSAGES_COUNT = 100

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Test {


    private val log = LoggerFactory.getLogger(javaClass)
    private val container = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"))
        .withEmbeddedZookeeper()

    @BeforeAll
    fun setup() {
        container.start()

    }

    @Test
    fun test() = runBlocking<Unit> {

        val producer = SampleProducer(bootstrapServers = container.bootstrapServers)
        val consumer = SampleConsumer(bootstrapServers = container.bootstrapServers)

        val channel = Channel<SampleMessage>()

        val processorJob = async {
            while (!channel.isClosedForReceive) {
                val message = channel.receive()
                println("Processing message=$message. Please wait patiently.")
                message.receiverOffset.commit().onErrorContinue { e, v ->
                    println("Error during commit of message=$message. Error=$e. value =$v")
                }
                println("Committed message=$message.")
                delay(1000)
            }
        }

        val kafkaProducerJob = async {
            producer.sendMessages(SAMPLE_TOPIC, MESSAGES_COUNT).awaitLast()
        }

        val kafkaConsumerJob = async {
            consumer.consumeMessages(SAMPLE_TOPIC).asFlow().map {
                println("Consumed message=$it. Sending to channel...")
                channel.send(it)
            }
                .take(MESSAGES_COUNT)
                .collect()

            channel.close()
        }
        awaitAll(processorJob, kafkaConsumerJob, kafkaProducerJob)
    }

    @AfterAll
    fun tearDown() {
        container.stop()
    }
}