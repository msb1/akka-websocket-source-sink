import java.io.{ByteArrayOutputStream, File}
import java.text.SimpleDateFormat
import java.util.{Calendar, UUID}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.Map
import scala.math._

case class Continuous(id: Int, name: String, cmax: Double, cmin: Double, distribution: String,
                      alarmLL: Double, alarmUL: Double, warnLL: Double, warnUL: Double,
                      posparam1: Double, posparam2: Double, negparam1: Double, negparam2: Double)

case class Discrete(id: Int, name: String, numlevels: Int, levels: String, posprobs: String, negprobs: String,
                    alarmlevels: String, warnlevels: String)

case class Equipment(errate: Double, filename: String, name: String, success: Double)

case class EpdConfig(equipment: Equipment, continuous: List[Continuous], discrete: List[Discrete])

case class EquipConfig(equipName: String, fileName: String, taktTime: Int, offsetDelay: Int,
                       consumerTopic: String, producerTopic: String )

case class MasterConfig(config: List[EquipConfig])

case class EpdData(uuid: String, currentTime: String, name: String, discretes: Map[String, Int], cons: Map[String, Double], result: Int)

case class EpdAnalytic(uuid: String, currentTime: String, name: String, analytic: Int)

object DataRecord {

  val logger: Logger = LoggerFactory.getLogger("DataRecord")
  private val timeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def readConfigFile(filePath: String, fileName: String): EpdConfig = {
    // read single equipment config file
      logger.info(s"Reading ${fileName} ...")
      val yamlFile = new File(filePath + fileName + ".yml")
      val objectMapper = new ObjectMapper(new YAMLFactory()) with ScalaObjectMapper
      objectMapper.registerModule(DefaultScalaModule)
      objectMapper.readValue[EpdConfig](yamlFile, classOf[EpdConfig])
  }

  def readConfigData(masterConfig: MasterConfig, filePath: String): Map[String, EpdConfig] = {
    // read multiple equipment config files (from master config)
    val epdConfig: Map[String, EpdConfig] = masterConfig.config.map(v =>
      (v.equipName, readConfigFile(filePath, v.fileName))
    ).toMap
    epdConfig.foreach({ println })
    epdConfig
  }

  def readMasterConfig(equipFileName: String): MasterConfig = {
    // read equip config master file
    logger.info(s"Reading $equipFileName ...")
    val yamlFile = new File(equipFileName)

    val objectMapper = new ObjectMapper(new YAMLFactory()) with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    val masterConfig: MasterConfig = objectMapper
      .readValue[MasterConfig](yamlFile, classOf[MasterConfig])
    println(masterConfig)
    masterConfig
  }

  def makeSimulatedRecord(config: EpdConfig): String = {
    val random = scala.util.Random

    // determine output (1 or 0) for data record
    val result = if(random.nextFloat() < config.equipment.success)  1 else 0

    // generate sensor (continuous) simulated data
    val cons: Map[String, Double] = config.continuous.map(c => {
      // use case match to determine simulator type
      val output = c.distribution match {
        // posparam1/negparam1 are max values for uni distribution; posparam2/negparam2 are min values
        case "uni" if result == 1 => random.nextDouble() * (c.posparam1 - c.posparam2) + c.posparam2
        case "uni" if result == 0 => random.nextDouble() * (c.negparam1 - c.negparam2) + c.negparam2
        // posparam1/negparam1 is mean of Normal distribution; posparam2/negparam2 is std dev
        case "norm" if result == 1 => random.nextGaussian() * c.posparam2 + c.posparam1
        case "norm" if result == 0 => random.nextGaussian() * c.negparam2 + c.negparam1
        // posparam1/negparam1 is mean of Log Normal distribution; posparam2/negparam2 is std dev
        case "log" if result == 1 =>
          val mu: Double = log(c.posparam1 * c.posparam1 / sqrt(c.posparam1 * c.posparam1 + c.posparam2 * c.posparam2))
          val std: Double = log(1.0 + c.posparam2 * c.posparam2 / c.posparam1 / c.posparam1)
          exp(random.nextGaussian() * std + mu)
        case "log" if result == 0 =>
          val mu: Double = log(c.negparam1 * c.negparam1 / sqrt(c.negparam1 * c.negparam1 + c.negparam2 * c.negparam2))
          val std: Double = log(1.0 + c.negparam2 * c.negparam2 / c.negparam1 / c.negparam1)
          exp(random.nextGaussian() * std + mu)
        // default case
        case _ => -1.0
      }
      if (output != -1.0) {
        (c.name, round(1000 * output) / 1000.0)
      }
      else {
        logger.warn(s"Improper simulator type for record: ${c.name}")
        (c.name, 0.0)
      }
    }).toMap

    // generate category endpoint simulated data
    val discretes = config.discrete.map(d => {
      val rv = random.nextDouble()
      val probs = if (result == 1) d.posprobs.split(',') else d.negprobs.split(',')
      val idx = probs.map(p => p.trim.toDouble > rv).indexOf(true)
      (d.name, idx)
    }).toMap

    // change result according to error rate
    val finalResult = if (random.nextDouble() < config.equipment.errate) {
      if (result == 1) 0
      else 1
    } else result

    val epd = EpdData(UUID.randomUUID().toString, timeFormat.format(Calendar.getInstance.getTime), config.equipment.name, discretes, cons, finalResult)

    // convert epdData record to Json string
    val out = new ByteArrayOutputStream()
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.writeValue(out, epd)
    // logger.info(out.toString)
    out.toString
  }

  def makeSimulatedAnalytic(config: EpdConfig) : String = {
    val random = scala.util.Random
    // determine output (1 or 0) for data record
    val result = if(random.nextFloat() < config.equipment.success)  1 else 0
    val epd = EpdAnalytic(UUID.randomUUID().toString, timeFormat.format(Calendar.getInstance.getTime), config.equipment.name, result)

    // convert epdData record to Json string
    val out = new ByteArrayOutputStream()
    val objectMapper = new ObjectMapper() with ScalaObjectMapper
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.writeValue(out, epd)
    // logger.info(out.toString)
    out.toString
  }
}

