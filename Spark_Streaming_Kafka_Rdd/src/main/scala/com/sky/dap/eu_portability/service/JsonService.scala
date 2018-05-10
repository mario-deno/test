package com.sky.dap.eu_portability.service

import org.json4s.{NoTypeHints, jackson}
import org.json4s.JsonAST.{JField, JObject, JString}
import org.apache.log4j.{LogManager, Logger}

/**
  * Created by dap on 4/10/18.
  */
object JsonService {
  private val _log = LogManager.getLogger("KafkaService")

    def extractMessageFromJson[A: Manifest](message: String): Option[A] = {
      //try {
      _log.debug("Event --> " + message.toString)
      implicit val formats = jackson.Serialization.formats(NoTypeHints)
      val json = jackson.parseJson(message)
      val event: A = json.extract[A]
      Some(event)
  }

  def extractFieldFromJson(fieldNameIn : String, jsonMessage: String) : Option[String]= {
    implicit val formats = jackson.Serialization.formats(NoTypeHints)
    val jsonParsed = jackson.parseJson(jsonMessage)

    val result = for {
      JObject(jsonObject) <- jsonParsed
      JField(fieldName, JString(field)) <- jsonObject if fieldNameIn == fieldName
    } yield field
    result match {
      case Nil => None
      case v : List[String] => Some(v.head)
      case _ => None
    }

  }

}


