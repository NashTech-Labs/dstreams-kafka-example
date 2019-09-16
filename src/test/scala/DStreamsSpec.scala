
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.scalatest.WordSpec
import org.apache.spark.streaming.Seconds

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import org.apache.spark.streaming.kafka010._


class DStreamsSpec extends WordSpec with EmbeddedKafka {

  implicit val serializer = new StringSerializer()

  def publishWords(start: Int, end: Int) = {
    start to end foreach { recordNum =>
      //TODO Add Serializer and dser
      publishToKafka("words", recordNum.toString)
      println(recordNum)
      Thread.sleep(1000)
    }
  }

  "DStreams" should {

    "print every 5 seconds" in {

      withRunningKafka {

        val kafkaParams = Map(
          "bootstrap.servers" -> "localhost:6001",
          "key.deserializer" -> classOf[StringDeserializer],
          "value.deserializer" -> classOf[StringDeserializer],
          "group.id" -> "spark-streaming-example",
          "auto.offset.reset" -> "earliest"
        )

        val topics = List("words")

        val sparkConf = new SparkConf()
          .setAppName("testapp")
          .setMaster("local[2]")

        val streamingContext = new StreamingContext(sparkConf, Seconds(5))

        val sc = streamingContext.sparkContext
        sc.setLogLevel("WARN")

        val inputDstream =
          KafkaUtils.createDirectStream(
            streamingContext,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](topics, kafkaParams)
          )

        inputDstream.map { record =>
          record.value.toString
        }.print()

        //Generate 1,000 words with a velocity of 1 Record / Sec
        Future {
          publishWords(1, 1000)
        }

        streamingContext.start()


        streamingContext.awaitTermination()

      }
    }

  }
}
