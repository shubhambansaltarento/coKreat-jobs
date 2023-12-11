package org.sunbird.job.task

import java.io.File
import java.util

import com.typesafe.config.ConfigFactory
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.util.{FlinkUtil, HttpUtil}
import org.slf4j.LoggerFactory
import org.sunbird.job.userdelete.domain.Event
import org.sunbird.job.userdelete.functions.UserDeleteFunction

class UserDeleteStreamTask(config: UserDeleteConfig, kafkaConnector: FlinkKafkaConnector, httpUtil: HttpUtil) {
  private[this] val logger = LoggerFactory.getLogger(classOf[UserDeleteStreamTask])

  def process(): Unit = {
    implicit val env: StreamExecutionEnvironment = FlinkUtil.getExecutionContext(config)
    implicit val eventTypeInfo: TypeInformation[Event] = TypeExtractor.getForClass(classOf[Event])
    implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
    implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

    val coKreatUserDeleteStream = env.addSource(kafkaConnector.kafkaJobRequestSource[Event](config.kafkaInputTopic)).name(config.eventConsumer)
      .uid(config.eventConsumer).setParallelism(config.kafkaConsumerParallelism)
      .rebalance
      .process(new UserDeleteFunction(config, httpUtil))
      .name(config.UserDeleteFunction)
      .uid(config.UserDeleteFunction)
      .setParallelism(config.parallelism)

    env.execute(config.jobName)
  }
}

// $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked within flink cluster
object UserDeleteStreamTask {

  def main(args: Array[String]): Unit = {
    val configFilePath = Option(ParameterTool.fromArgs(args).get("config.file.path"))
    val config = configFilePath.map {
      path => ConfigFactory.parseFile(new File(path)).resolve()
    }.getOrElse(ConfigFactory.load("user-delete.conf").withFallback(ConfigFactory.systemEnvironment()))
    val userDeleteConfig = new UserDeleteConfig(config)
    val kafkaUtil = new FlinkKafkaConnector(userDeleteConfig)
    val httpUtil = new HttpUtil
    val task = new UserDeleteStreamTask(userDeleteConfig, kafkaUtil, httpUtil)
    task.process()
  }
}

// $COVERAGE-ON$
