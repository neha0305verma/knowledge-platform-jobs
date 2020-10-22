package org.sunbird.job.functions

import java.util

import org.apache.commons.collections.{CollectionUtils, MapUtils}
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.sunbird.job.cache.DataCache
import org.sunbird.job.task.{CertificatePreProcessorConfig, CertificatePreProcessorStreamTask}

import scala.collection.JavaConverters._

object CertificateApiService {

  lazy private val mapper: ObjectMapper = new ObjectMapper()

  def getUsersFromUserCriteria(userCriteria: util.Map[String, AnyRef], userIds: List[String], config: CertificatePreProcessorConfig): List[String] = {
    val batchSize = 50
    val batchList = userIds.grouped(batchSize).toList
    println("getUsersFromUserCriteria called : " + batchList)
    batchList.flatMap(batch => {
      val httpRequest = s"""{"request":{"filters":{"identifier":"${batch}, ${userCriteria}"},"fields":["identifier"]}}"""
      val httpResponse = CertificatePreProcessorStreamTask.httpUtil.post(config.searchBaseUrl + "/private/user/v1/search", httpRequest)
      if (httpResponse.status == 200) {
        println("User search success: " + httpResponse.body)
        val response = mapper.readValue(httpResponse.body, classOf[util.Map[String, AnyRef]])
        val result = response.getOrDefault("result", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
        val contents = result.getOrDefault("content", new util.ArrayList[util.Map[String, AnyRef]]()).asInstanceOf[util.ArrayList[util.Map[String, AnyRef]]]

        println("getUsersFromUserCriteria : contents from search response : " + contents)
        val userList = contents.asScala.map(content => content.getOrDefault(config.identifier, "").asInstanceOf[String]).toList
        if (userList.isEmpty) throw new Exception("User not found for userCriteria : " + userCriteria)
        println("getUsersFromUserCriteria : User found Batch : " + userList)

        userList
      } else throw new Exception("Search users for given criteria failed to fetch data : " + userCriteria + " " + httpResponse.status + " :: " + httpResponse.body)
    })
  }

  def readContent(courseId: String, cache: DataCache, config: CertificatePreProcessorConfig): util.Map[String, AnyRef] = {
    val httpResponse = CertificatePreProcessorStreamTask.httpUtil.get(config.lmsBaseUrl + "/content/v3/read/" + courseId)
    if (httpResponse.status == 200) {
      println("Content read success: " + httpResponse.body)
      val response = mapper.readValue(httpResponse.body, classOf[util.Map[String, AnyRef]])
      val result = response.getOrDefault("result", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
      val content = result.getOrDefault("content", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
      if (MapUtils.isEmpty(content)) throw new Exception("Content is empty for courseId : " + courseId)
      content
    } else throw new Exception("Content read failed for courseId : " + courseId + " " + httpResponse.status + " :: " + httpResponse.body)
  }

  def getUserDetails(userId: String, config: CertificatePreProcessorConfig): util.Map[String, AnyRef] = {
    val httpRequest = s"""{"request":{"filters":{"identifier":"${userId}"},"fields":["firstName", "lastName", "userName", "rootOrgName", "rootOrgId","maskedPhone"]}}"""
    val httpResponse = CertificatePreProcessorStreamTask.httpUtil.post(config.searchBaseUrl + "/private/user/v1/search", httpRequest)
    if (httpResponse.status == 200) {
      println("User search success: " + httpResponse.body)
      val response = mapper.readValue(httpResponse.body, classOf[util.Map[String, AnyRef]])
      val result = response.getOrDefault("result", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
      val contents = result.getOrDefault("content", new util.ArrayList[util.Map[String, AnyRef]]()).asInstanceOf[util.List[util.Map[String, AnyRef]]]
      val userDetails = contents.get(0)
      if (MapUtils.isEmpty(userDetails))
        throw new Exception("User not found for userId : " + userId)
      userDetails
    } else throw new Exception("User not found for userId : " + userId + " " + httpResponse.status + " :: " + httpResponse.body)
  }

  def readOrgKeys(rootOrgId: String, config: CertificatePreProcessorConfig): util.Map[String, AnyRef] = {
    val httpRequest = s"""{"request":{"organisationId":"${rootOrgId}"}}}"""
    val httpResponse = CertificatePreProcessorStreamTask.httpUtil.post(config.lmsBaseUrl + "/v1/org/read", httpRequest)
    if (httpResponse.status == 200) {
      println("Org read success: " + httpResponse.body)
      val response = mapper.readValue(httpResponse.body, classOf[util.Map[String, AnyRef]])
      val result = response.getOrDefault("result", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
      val keys = result.getOrDefault("keys", new util.HashMap()).asInstanceOf[util.Map[String, AnyRef]]
      if (MapUtils.isNotEmpty(keys) && CollectionUtils.isNotEmpty(keys.get("signKeys").asInstanceOf[util.List[util.Map[String, AnyRef]]])) {
        val signKeys = new util.HashMap[String, AnyRef]() {
          {
            put("id", keys.get("signKeys").asInstanceOf[util.List[util.Map[String, AnyRef]]].get(0))
          }
        }
        signKeys
      } else keys
    } else throw new Exception("Error while reading organisation  : " + rootOrgId + " " + httpResponse.status + " :: " + httpResponse.body)
  }
}