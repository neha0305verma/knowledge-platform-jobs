package org.sunbird.job.function.spec

import java.util
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import org.sunbird.job.Metrics
import org.sunbird.job.fixture.EventFixture
import org.sunbird.job.functions.EventValidator
import org.sunbird.job.task.CertificatePreProcessorConfig
import org.sunbird.spec.BaseTestSpec

class EventValidatorSpec extends BaseTestSpec{

  lazy private val gson = new Gson()
  val config: Config = ConfigFactory.load("test.conf")
  val jobConfig: CertificatePreProcessorConfig = new CertificatePreProcessorConfig(config)
  val metrics = Metrics(new ConcurrentHashMap[String, AtomicLong]() {
    {
      put(jobConfig.skippedEventCount, new AtomicLong())
    }
  })

  it should "return true for valid event" in {
    val edata =  gson.fromJson(EventFixture.USER_UTIL_EDATA, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]]
    val valid = EventValidator.isValidEvent(edata, jobConfig)
    assert(valid)
  }

  it should "return false for invalid event" in {
    val edata =  gson.fromJson(EventFixture.INVALID_EVENT, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]]
    val valid = EventValidator.isValidEvent(edata, jobConfig)
    assert(!valid)
  }

  it should "validateTemplateUrl" in {
    val edata =  gson.fromJson(EventFixture.USER_UTIL_EDATA, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]]
    val template =  gson.fromJson(EventFixture.USER_UTIL_TEMPLATE, new util.LinkedHashMap[String, AnyRef]().getClass).asInstanceOf[util.Map[String, AnyRef]]
    EventValidator.validateTemplateUrl(edata, template, "template_01_dev_001", jobConfig)(metrics)
  }
}
