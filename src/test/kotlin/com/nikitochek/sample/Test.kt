package com.nikitochek.sample

import com.nikitocheck.sample.SampleConsumer
import com.nikitocheck.sample.SampleMessage
import com.nikitocheck.sample.SampleProducer
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.reactive.asFlow
import kotlinx.coroutines.reactive.awaitLast
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Test
import org.testcontainers.containers.KafkaContainer
import org.testcontainers.utility.DockerImageName
import sendToChannel

const val SAMPLE_TOPIC = "com.nikitocheck.sample-topic"
const val MESSAGES_COUNT = 100

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@kotlinx.coroutines.ExperimentalCoroutinesApi
class Test {

    private val container = KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"))
        .withEmbeddedZookeeper()

    @BeforeAll
    fun setup() {
        container.start()
    }

    @Test
    fun `should process all messages sequentially`() = runBlocking<Unit> {

        val producer = SampleProducer(bootstrapServers = container.bootstrapServers)
        val consumer = SampleConsumer(bootstrapServers = container.bootstrapServers)

        val channel = Channel<SampleMessage>()

        // pretend slow processing of kafka messages
        val processorJob: Deferred<List<Int>> = async {
            val ids = mutableListOf<Int>()
            while (!channel.isClosedForReceive) {
                val message = channel.receive()
                println("Processing message=$message. Please wait patiently.")
                message.receiverOffset.commit().onErrorContinue { e, v ->
                    println("Error during commit of message=$message. Error=$e. value =$v")
                }.awaitSingleOrNull()
                ids.add(message.key)
                println("Committed message=$message.")
                delay(1000)
            }
            return@async ids
        }

        // producer sends messages to kafka
        val kafkaProducerJob = async {
            producer.sendMessages(SAMPLE_TOPIC, MESSAGES_COUNT).awaitLast()
        }

        // consumer receives messages from kafka as they come
        val kafkaConsumerJob = async {
            consumer.consumeMessages(SAMPLE_TOPIC).asFlow().sendToChannel(channel)
                // limit messages count to finish this test
                .take(MESSAGES_COUNT)
                .collect()

            channel.close()
        }
        val results = awaitAll(processorJob, kafkaConsumerJob, kafkaProducerJob)

        Assertions.assertEquals(results.first(), (1..100).toList())
    }

    @AfterAll
    fun tearDown() {
        container.stop()
    }
}