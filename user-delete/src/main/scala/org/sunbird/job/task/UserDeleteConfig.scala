package org.sunbird.job.task

import java.util
import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig
//import org.sunbird.job.userdelete.model.ObjectParent

import scala.collection.JavaConverters._

class UserDeleteConfig(override val config: Config) extends BaseJobConfig(config, "user-delete") {

  private val serialVersionUID = 2905979434303791379L

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])
 // implicit val objectParentTypeInfo: TypeInformation[ObjectParent] = TypeExtractor.getForClass(classOf[ObjectParent])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  println(kafkaInputTopic);
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")
  override val parallelism: Int = config.getInt("task.parallelism")
  val linkCollectionParallelism: Int = if (config.hasPath("task.link-collection.parallelism"))
    config.getInt("task.link-collection.parallelism") else 1

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val skippedEventCount = "skipped-events-count"

  // Consumers
  val eventConsumer = "user-delete-consumer"
  val UserDeleteFunction = "user-delete"
  val programServiceBaseUrl: String = config.getString("program-service-baseUrl")

  val UserDeleteEventProducer = "user-delete-producer"
  val linkCollectionFunction = "link-collection-process"

  // Tags
  //val linkCollectionOutputTag: OutputTag[ObjectParent] = OutputTag[ObjectParent]("link-collection")
  val sourceBaseUrl: String = config.getString("source.baseUrl")
  val configVersion = "1.0"

  def getConfig() = config
}
