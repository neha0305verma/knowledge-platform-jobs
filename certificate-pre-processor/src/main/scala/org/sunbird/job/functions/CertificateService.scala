package org.sunbird.job.functions

import java.util

import com.datastax.driver.core.querybuilder.QueryBuilder
import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.sunbird.job.Metrics
import org.sunbird.job.cache.DataCache
import org.sunbird.job.task.{CertificatePreProcessorConfig, CertificatePreProcessorStreamTask}
import org.sunbird.job.util.CassandraUtil

import scala.collection.JavaConverters._

class CertificateService(config: CertificatePreProcessorConfig)
                        (implicit val metrics: Metrics,
                         @transient var cassandraUtil: CassandraUtil = null) {

  lazy private val mapper: ObjectMapper = new ObjectMapper()
  private[this] val logger = LoggerFactory.getLogger(classOf[CertificateService])

  def readUserCertificate(edata: util.Map[String, AnyRef]) {
    val selectQuery = QueryBuilder.select().all().from(config.dbKeyspace, config.dbTable)
    selectQuery.where.and(QueryBuilder.eq(config.userEnrolmentsPrimaryKey(0), edata.get(config.userId).asInstanceOf[String])).
      and(QueryBuilder.eq(config.userEnrolmentsPrimaryKey(1), edata.get(config.courseId).asInstanceOf[String])).
      and(QueryBuilder.eq(config.userEnrolmentsPrimaryKey(2), edata.get(config.batchId).asInstanceOf[String]))
    val rows = cassandraUtil.find(selectQuery.toString)
    metrics.incCounter(config.dbReadCount)
    if (CollectionUtils.isNotEmpty(rows)) {
      edata.put(config.issued_certificates, rows.asScala.head.getObject(config.issued_certificates))
      edata.put(config.issuedDate, rows.asScala.head.getDate(config.completedOn))
    } else {
      errorMessage("User : " + edata.get(config.userId) + " is not enrolled for batch :  "
        + edata.get(config.batchId) + " and course : " + edata.get(config.courseId))
    }
  }

  def readContent(courseId: String, cache: DataCache): util.Map[String, AnyRef] = {
    val httpResponse = CertificatePreProcessorStreamTask.httpUtil.get(config.lmsBaseUrl + "/content/v3/read/" + courseId)
    if (httpResponse.status == 200) {
      logger.info("Content read success: " + httpResponse.body)
      val response = mapper.readValue(httpResponse.body, classOf[util.Map[String, AnyRef]])
      val result = response.getOrDefault("result", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
      val content = result.getOrDefault("content", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
      if (MapUtils.isEmpty(content)) {
        errorMessage("Content is empty for courseId : " + courseId)
      }
      content
    } else {
      errorMessage("Content read failed for courseId : " + courseId + " " + httpResponse.status + " :: " + httpResponse.body)
      null
    }
  }

  def getUserDetails(userId: String): util.Map[String, AnyRef] = {
    val httpRequest = s"""{"request":{"filters":{"identifier":"${userId}"},"fields":["firstName", "lastName", "userName", "rootOrgName", "rootOrgId","maskedPhone"]}}"""
    val httpResponse = CertificatePreProcessorStreamTask.httpUtil.post(config.searchBaseUrl + "/private/user/v1/search", httpRequest)
    if (httpResponse.status == 200) {
      logger.info("User search success: " + httpResponse.body)
      val response = mapper.readValue(httpResponse.body, classOf[util.Map[String, AnyRef]])
      val result = response.getOrDefault("result", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
      val contents = result.getOrDefault("content", new util.ArrayList[util.Map[String, AnyRef]]()).asInstanceOf[util.List[util.Map[String, AnyRef]]]
      val userDetails = contents.get(0)
      if (MapUtils.isEmpty(userDetails)) {
        errorMessage("User not found for userId : " + userId)
      }
      userDetails
    } else {
      errorMessage("User not found for userId : " + userId + " " + httpResponse.status + " :: " + httpResponse.body)
      null
    }
  }

  def readOrgKeys(rootOrgId: String) : util.Map[String, AnyRef] = {
    val httpRequest = s"""{"request":{"organisationId":"${rootOrgId}"}}}"""
    val httpResponse = CertificatePreProcessorStreamTask.httpUtil.post(config.lmsBaseUrl + "/v1/org/read", httpRequest)
    if (httpResponse.status == 200) {
      logger.info("Org read success: " + httpResponse.body)
      val response = mapper.readValue(httpResponse.body, classOf[util.Map[String, AnyRef]])
      val result = response.getOrDefault("result", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
      val keys = result.getOrDefault("keys", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
      if (MapUtils.isNotEmpty(keys) && CollectionUtils.isNotEmpty(keys.get("signKeys").asInstanceOf[util.List[util.Map[String, AnyRef]]])) {
        val signKeys = new util.HashMap[String, AnyRef]() {{
          put("id", keys.get("signKeys").asInstanceOf[util.List[util.Map[String, AnyRef]]].get(0))
        }}
        signKeys
      }
      keys
    } else {
      errorMessage("Error while reading organisation  : " + rootOrgId + " " + httpResponse.status + " :: " + httpResponse.body)
      null
    }
  }

  def errorMessage(message: String) {
    logger.error(message)
    metrics.incCounter(config.skippedEventCount)
    throw new Exception(message)
  }
}
