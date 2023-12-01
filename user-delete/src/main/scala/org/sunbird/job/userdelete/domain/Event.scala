package org.sunbird.job.userdelete.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

	private val jobName = "user-delete"

	//private val objectTypes = List("Question", "QuestionSet")

	def eData: Map[String, AnyRef] = readOrDefault("edata", Map()).asInstanceOf[Map[String, AnyRef]]

	//def metadata: Map[String, AnyRef] = readOrDefault("edata.metadata", Map())

	//def collection: List[Map[String, String]] = readOrDefault("edata.collection", List(Map())).asInstanceOf[List[Map[String, String]]]

	def action: String = readOrDefault[String]("edata.action", "")

	//def mimeType: String = readOrDefault[String]("edata.metadata.mimeType", "")

	def userId: String = readOrDefault[String]("edata.userId", "")

	//def objectType: String = readOrDefault[String]("edata.objectType", "")

	//def repository: Option[String] = read[String]("edata.repository")

	//def downloadUrl: String = readOrDefault[String]("edata.metadata.downloadUrl", "")

	def isValid(): Boolean = {
		(StringUtils.equals("delete-user", action) && StringUtils.isNotBlank(userId));
	}
}