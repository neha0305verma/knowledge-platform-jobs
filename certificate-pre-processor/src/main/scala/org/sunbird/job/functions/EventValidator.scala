package org.sunbird.job.functions

import java.util

import org.apache.commons.collections.MapUtils
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.task.CertificatePreProcessorConfig

class EventValidator(config: CertificatePreProcessorConfig) {

  private[this] val logger = LoggerFactory.getLogger(classOf[EventValidator])

  def isValidEvent(edata: util.Map[String, AnyRef]): Boolean = {
    val action = edata.getOrDefault(config.action, "").asInstanceOf[String]
    val courseId = edata.getOrDefault(config.courseId, "").asInstanceOf[String]
    val batchId = edata.getOrDefault(config.batchId, "").asInstanceOf[String]
    val userId = edata.getOrDefault(config.userId, "").asInstanceOf[String]

    StringUtils.equalsIgnoreCase(action, config.generateCourseCertificate) &&
      StringUtils.isNotBlank(courseId) && StringUtils.isNotBlank(batchId) && StringUtils.isNotBlank(userId)
  }

  def validateTemplate(edata: util.Map[String, AnyRef])(implicit metrics: Metrics): Unit = {
    val template = edata.getOrDefault(config.template, new util.HashMap).asInstanceOf[util.Map[String, AnyRef]]
    if (MapUtils.isEmpty(template)) {
      logger.error("Certificate template is not available for batchId : " + edata.get(config.batchId).asInstanceOf[String] + " and courseId : " + edata.get(config.courseId).asInstanceOf[String])
      metrics.incCounter(config.skippedEventCount)
    }
  }

}