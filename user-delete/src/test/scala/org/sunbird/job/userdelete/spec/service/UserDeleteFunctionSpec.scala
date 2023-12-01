package org.sunbird.job.userdelete.spec.service

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.mockito.Mockito
import org.sunbird.job.userdelete.fixture.EventFixture
import org.sunbird.job.userdelete.functions.UserDeleteFunction
import org.sunbird.job.task.UserDeleteConfig
import org.sunbird.job.util.{HttpUtil, JSONUtil}
import org.sunbird.spec.BaseTestSpec

import java.util

class UserDeleteFunctionSpec extends BaseTestSpec {
  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  val config: Config = ConfigFactory.load("test.conf")
  val mockHttpUtil = mock[HttpUtil](Mockito.withSettings().serializable())
  lazy val jobConfig: UserDeleteConfig = new UserDeleteConfig(config)
  lazy val coKreatUserDelete: UserDeleteFunction = new UserDeleteFunction(jobConfig, mockHttpUtil)

  override protected def beforeAll(): Unit = {
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
  }

  "CokreatUserDeleteService" should "generate event" in {
    val inputEvent:util.Map[String, Any] = JSONUtil.deserialize[util.Map[String, Any]](EventFixture.EVENT_1)
  }
}
