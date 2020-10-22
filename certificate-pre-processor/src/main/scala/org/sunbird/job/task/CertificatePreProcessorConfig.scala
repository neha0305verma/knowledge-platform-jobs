package org.sunbird.job.task

import java.util

import com.typesafe.config.Config
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.streaming.api.scala.OutputTag
import org.sunbird.job.BaseJobConfig

class CertificatePreProcessorConfig(override val config: Config) extends BaseJobConfig(config, "certificate-pre-processor") {

  implicit val mapTypeInfo: TypeInformation[util.Map[String, AnyRef]] = TypeExtractor.getForClass(classOf[util.Map[String, AnyRef]])
  implicit val stringTypeInfo: TypeInformation[String] = TypeExtractor.getForClass(classOf[String])

  // Kafka Topics Configuration
  val kafkaInputTopic: String = config.getString("kafka.input.topic")
  val kafkaOutputTopic: String = config.getString("kafka.output.topic")
  override val kafkaConsumerParallelism: Int = config.getInt("task.consumer.parallelism")

  // Metric List
  val totalEventsCount = "total-events-count"
  val successEventCount = "success-events-count"
  val failedEventCount = "failed-events-count"
  val skippedEventCount = "skipped-event-count"
  val dbReadCount = "db-read-count"
  val dbUpdateCount = "db-update-count"

  // Consumers
  val certificatePreProcessorConsumer = "certificate-pre-processor-consumer"

  // Tags
  val generateCertificateOutputTagName = "generate-certificate-request"
  val generateCertificateOutputTag: OutputTag[String] = OutputTag[String](generateCertificateOutputTagName)

  // Producers
  val generateCertificateProducer = "generate-certificate-sink"

  // Cassandra Configurations
  val dbBatchTable: String = config.getString("lms-cassandra.batchTable")
  val dbUserTable: String = config.getString("lms-cassandra.userTable")
  val dbAssessmentAggregator: String = config.getString("lms-cassandra.assessmentAggregatorTable")
  val dbKeyspace: String = config.getString("lms-cassandra.keyspace")
  val dbHost: String = config.getString("lms-cassandra.host")
  val dbPort: Int = config.getInt("lms-cassandra.port")
  val userEnrolmentsPrimaryKey: List[String] = List("userid","courseid","batchid")
  val courseBatchPrimaryKey: List[String] = List("courseid","batchid")

  // Redis Configurations
  val collectionCacheStore: Int = config.getInt("redis.database.collectionCache.id")

  // BaseUrl
  val lmsBaseUrl = config.getString("lms.basePath")
  val searchBaseUrl = config.getString("user.search.basePath")
  val contentBaseUrl = config.getString("content.basePath")

  // certFilterKeys
  val certFilterKeys: List[String] = List("enrollment","assessment", "user")

  // Constants
  val issueCertificate = "issue-certificate"
  val certTemplates = "cert_templates"
  val courseBatch = "CourseBatch"
  val userId = "userId"
  val courseId = "courseId"
  val batchId = "batchId"
  val userIds = "userIds"
  val eData = "edata"
  val action = "action"
  val template = "template"
  val templates = "templates"
  val generateCourseCertificate = "generate-course-certificate"
  val reIssue = "reIssue"
  val oldId = "oldId"
  val certificates = "certificates"
  val completedOn = "completedon"
  val issuedDate = "issuedDate"
  val issued_certificates = "issued_certificates"
  val name = "name"
  val identifier = "identifier"
  val firstName = "firstName"
  val lastName = "lastName"
  val rootOrgId = "rootOrgId"
  val orgId = "orgId"
  val criteria = "criteria"
  val artifactUrl = "artifactUrl"
  val issuer = "issuer"
  val signatoryList = "signatoryList"
  val notifyTemplate = "notifyTemplate"
}