package org.sunbird.job.functions

import com.google.gson.Gson
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.slf4j.LoggerFactory
import org.sunbird.job.{BaseProcessFunction, Metrics}
import org.sunbird.job.task.CertificatePreProcessorConfig

class CertificatePreProcessor(config: CertificatePreProcessorConfig)
                             (implicit val stringTypeInfo: TypeInformation[String])
  extends BaseProcessFunction[java.util.Map[String, AnyRef], String](config) {

  private[this] val logger = LoggerFactory.getLogger(classOf[CertificatePreProcessor])
  lazy private val mapper: ObjectMapper = new ObjectMapper()
  lazy private val gson = new Gson()

  override def metricsList(): List[String] = {
    List(config.successEventCount, config.failedEventCount, config.skippedEventCount, config.totalEventsCount)
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def close(): Unit = {
    super.close()
  }


  override def processElement(event: java.util.Map[String, AnyRef], context: ProcessFunction[java.util.Map[String, AnyRef], String]#Context, metrics: Metrics): Unit = {
    // remove it latter
    println("event", event)
    logger.info("event : " + event)
    val eData = event.get("edata").asInstanceOf[java.util.Map[String, AnyRef]]
    if (isValidEvent(eData)) {
      val rootId = eData.get("identifier").asInstanceOf[String]
      logger.info("CertificatePreProcessor:processElement::Processing - identifier: " + rootId)
      // send event to next topic to generate certificate
      generateCertificate(eData)
      context.output(config.generateCertificateOutputTag, gson.toJson(event))
      metrics.incCounter(config.successEventCount)
    } else {
      metrics.incCounter(config.skippedEventCount)
    }
    metrics.incCounter(config.totalEventsCount)
  }

  private def isValidEvent(eData: java.util.Map[String, AnyRef]): Boolean = {
    val action = eData.getOrDefault("action", "").asInstanceOf[String]
    val identifier = eData.getOrDefault("identifier", "").asInstanceOf[String]

    StringUtils.equalsIgnoreCase(action, "certificate-pre-process") &&
      StringUtils.isNotBlank(identifier)
  }

  private def generateCertificate(eData: java.util.Map[String, AnyRef]): java.util.Map[String, AnyRef] = {
    eData.put( "svgTemplate", "template-url.svg")
    eData
  }

}
