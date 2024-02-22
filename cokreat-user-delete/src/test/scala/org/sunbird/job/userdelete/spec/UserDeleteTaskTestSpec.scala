package org.sunbird.job.userdelete.spec

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.mockito.Mockito
import org.mockito.Mockito.when
import org.sunbird.job.userdelete.domain.Event
import org.sunbird.job.userdelete.fixture.EventFixture
import org.sunbird.job.connector.FlinkKafkaConnector
import org.sunbird.job.task.{UserDeleteConfig, UserDeleteStreamTask}
import org.sunbird.job.util.{HttpUtil, JSONUtil}
import org.sunbird.spec.{BaseMetricsReporter, BaseTestSpec}

import java.util


class UserDeleteTaskTestSpec extends BaseTestSpec {

  implicit val mapTypeInfo: TypeInformation[java.util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[java.util.Map[String, AnyRef]])

  val flinkCluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder()
    .setConfiguration(testConfiguration())
    .setNumberSlotsPerTaskManager(1)
    .setNumberTaskManagers(1)
    .build)
  val mockKafkaUtil: FlinkKafkaConnector = mock[FlinkKafkaConnector](Mockito.withSettings().serializable())
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: UserDeleteConfig = new UserDeleteConfig(config)
  var currentMilliSecond = 1605816926271L
  val mockHttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())

  override protected def beforeAll(): Unit = {
    BaseMetricsReporter.gaugeMetrics.clear()
    flinkCluster.before()
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    flinkCluster.after()
    super.afterAll()
  }

  "event" should " process the input map and return metadata " in {
    val event = new Event(JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_2), 0, 1)
    event.isValid should be(true)
    event.action should be("delete-user")
    event.eData.size should be(9)
  }

   ignore should "generate event" in {
    when(mockKafkaUtil.kafkaMapSource(jobConfig.kafkaInputTopic)).thenReturn(new UserDeleteMapSource)
    new UserDeleteStreamTask(jobConfig, mockKafkaUtil, mockHttpUtil).process()
  }
}

class UserDeleteMapSource extends SourceFunction[util.Map[String, AnyRef]] {

  override def run(ctx: SourceContext[util.Map[String, AnyRef]]) {
  }

  override def cancel(): Unit = {}
}