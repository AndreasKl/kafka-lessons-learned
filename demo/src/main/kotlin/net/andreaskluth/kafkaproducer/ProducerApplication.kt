package net.andreaskluth.kafkaproducer

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG
import org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.Properties

fun main(args: Array<String>) {
    SimpleProducer().produce()
}

class SimpleProducer {

    companion object {
        val log: Logger = LoggerFactory.getLogger(SimpleProducer::class.java.simpleName)
    }

    private fun config(): Properties {
        val config = Properties()
        config[BOOTSTRAP_SERVERS_CONFIG] = "127.0.0.1:9092"
        config[KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        config[VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.java.name
        config[REQUEST_TIMEOUT_MS_CONFIG] = 10_000

        config[ACKS_CONFIG] = "all"

        config[ENABLE_IDEMPOTENCE_CONFIG] = true
        config[CLIENT_ID_CONFIG] = "transactional-producer"
        config["transactional.id"] = "demo"
        config["transaction.state.log.replication.factor"] = 1 // <--- For testing set to 1!!

        return config
    }

    fun produce() {
        KafkaProducer<String, String>(config()).use { producer ->
            producer.initTransactions()
            for (i in 1..1_000) {
                producer.beginTransaction()
                log.info("Sending: $i")

                val record = ProducerRecord(
                        "rewe-topic",
                        i.toString(),
                        "Hallo Kafka #" + i.toString())

                producer.send(record, logAsyncResponse(producer, i))
                producer.commitTransaction()
                Thread.sleep(200)
            }
        }
    }


    private fun logAsyncResponse(producer: KafkaProducer<String, String>, i: Int): (RecordMetadata?, Exception?) -> Unit {
        return fun(metadata: RecordMetadata?, exception: Exception?) {
            exception?.run {
                log.info("Skipping: $i due to ${exception.message}")
                return
            }
            metadata?.run {
                log.info("Success: $i")
            }
        }
    }


}
