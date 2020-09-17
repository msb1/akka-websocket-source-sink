import java.io.ByteArrayOutputStream

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.{DefaultScalaModule, ScalaObjectMapper}

object JsonUtil {
  val objectMapper = new ObjectMapper() with ScalaObjectMapper

  def toType[T](json: String)(implicit m: Manifest[T]): T = {
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.readValue[T](json)
  }

  def toString[T](obj: T): String = {
    val out = new ByteArrayOutputStream()
    objectMapper.registerModule(DefaultScalaModule)
    objectMapper.writeValue(out, obj)
    out.toString()
  }

}
