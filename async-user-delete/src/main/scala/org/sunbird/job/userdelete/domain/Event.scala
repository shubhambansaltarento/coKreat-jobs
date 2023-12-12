package org.sunbird.job.userdelete.domain

import org.apache.commons.lang3.StringUtils
import org.sunbird.job.domain.reader.JobRequest

class Event(eventMap: java.util.Map[String, Any], partition: Int, offset: Long) extends JobRequest(eventMap, partition, offset) {

	private val jobName = "user-delete"

	def eData: Map[String, AnyRef] = readOrDefault("edata", Map()).asInstanceOf[Map[String, AnyRef]]
	def action: String = readOrDefault[String]("edata.action", "")
	def userId: String = readOrDefault[String]("edata.userId", "")

	def isValid(): Boolean = {
		(StringUtils.equals("delete-user", action) && StringUtils.isNotBlank(userId));
	}
}