package org.sunbird.job.functions

import java.util

import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.task.CertificatePreProcessorConfig

import scala.collection.JavaConverters._

object EventValidator {

  lazy private val mapper: ObjectMapper = new ObjectMapper()
  private[this] val logger = LoggerFactory.getLogger(classOf[CertificatePreProcessor])


  def isValidEvent(edata: util.Map[String, AnyRef], config: CertificatePreProcessorConfig): Boolean = {
    val action = edata.getOrDefault(config.action, "").asInstanceOf[String]
    val courseId = edata.getOrDefault(config.courseId, "").asInstanceOf[String]
    val batchId = edata.getOrDefault(config.batchId, "").asInstanceOf[String]
    val userIds = edata.getOrDefault(config.userIds, new util.ArrayList[String](){}).asInstanceOf[util.ArrayList[String]]
    StringUtils.equalsIgnoreCase(action, config.issueCertificate) &&
      StringUtils.isNotBlank(courseId) && StringUtils.isNotBlank(batchId) && CollectionUtils.isNotEmpty(userIds)
  }

  def validateTemplate(certTemplates: util.Map[String, AnyRef], edata: util.Map[String, AnyRef],
                       config: CertificatePreProcessorConfig)(implicit metrics: Metrics) {
    println("validateTemplate called : " + certTemplates)
    if (MapUtils.isEmpty(certTemplates)) {
      metrics.incCounter(config.skippedEventCount)
      throw new Exception("Certificate template is not available for batchId : " + edata.get(config.batchId) + " and courseId : " + edata.get(config.courseId))
    }
  }

  def validateCriteria(template: util.Map[String, AnyRef], config: CertificatePreProcessorConfig)
                      (implicit metrics: Metrics): util.Map[String, AnyRef] = {
    println("validateCriteria called : " + template.get(config.criteria).asInstanceOf[String])
    val criteriaString = template.getOrDefault(config.criteria, "").asInstanceOf[String]
    if (StringUtils.isEmpty(criteriaString)) {
      metrics.incCounter(config.skippedEventCount)
      throw new Exception("Certificate template has empty criteria: " + template.toString)
    }
    val criteria = mapper.readValue(criteriaString, classOf[util.Map[String, AnyRef]])
    if (MapUtils.isEmpty(criteria) && CollectionUtils.isEmpty(CollectionUtils.intersection(criteria.keySet(), config.certFilterKeys.asJava))) {
      metrics.incCounter(config.skippedEventCount)
      throw new Exception("Certificate template has empty/invalid criteria: " + template.toString)
    }
    criteria
  }

  def isValidAssessUser(actualScore: Double, criteria: Map[String, AnyRef]): Boolean = {
    val operation = criteria.head._1
    val score = criteria.asJava.get(operation).asInstanceOf[Int].toDouble
    operation match {
      case "EQ" => actualScore == score
      case "eq" => actualScore == score
      case "=" => actualScore == score
      case ">" => actualScore > score
      case "<" => actualScore < score
      case ">=" => actualScore >= score
      case "<=" => actualScore <= score
      case "ne" => actualScore != score
      case "!=" => actualScore != score
      case _ => false
    }
  }

  def validateTemplateUrl(edata: util.Map[String, AnyRef], certTemplate: util.Map[String, AnyRef], templateId: String, config: CertificatePreProcessorConfig)(implicit metrics: Metrics): Unit = {
    println("validateTemplateUrl edata : " + edata +" certTemplate : " + certTemplate)
    val template = certTemplate.getOrDefault(config.issuer, new util.HashMap[String, AnyRef]()).asInstanceOf[util.Map[String, AnyRef]]
    println("validateTemplateUrl template : " + edata +" certTemplate : " + certTemplate)
    val templateUrl = template.get(config.url).asInstanceOf[String]
    println("validateTemplateUrl templateUrl : " + edata +" certTemplate : " + certTemplate)
    if (StringUtils.isBlank(templateUrl) || !StringUtils.endsWith(templateUrl, ".svg")) {
      logger.info("validateTemplateUrl : Certificate is not generated for batchId : " +
        edata.get(config.batchId).asInstanceOf[String] + ", courseId : " +
        edata.get(config.courseId).asInstanceOf[String] + " and userIds : " +
        edata.get(config.userIds).asInstanceOf[util.ArrayList[String]] + ". TemplateId: " +
        certTemplate.get(templateId).asInstanceOf[String] + " with Url: " + templateUrl + " is not supported.")
      metrics.incCounter(config.skippedEventCount)
      throw new Exception("Certificate is not generated for batchId : " +
        edata.get(config.batchId).asInstanceOf[String] + ", courseId : " +
        edata.get(config.courseId).asInstanceOf[String] + " and userIds : " +
        edata.get(config.userIds).asInstanceOf[util.ArrayList[String]] + ". TemplateId: " +
        certTemplate.get(templateId).asInstanceOf[String] + " with Url: " + templateUrl + " is not supported.")
    }
  }

}