package com.sky.dap.eu_portability.service

import java.time.{ZoneOffset, ZonedDateTime}

import com.sky.dap.eu_portability.model.{Activity, IMS_Event, Vip}
import org.apache.log4j.LogManager

import scala.util.{Failure, Success, Try}




/**
  * Created by dap on 4/13/18.
  */
object ModelConverter {

  private val _log = LogManager.getLogger("ModelConverter")

  def IMSEvent2Vip(imsEvent: IMS_Event) : Option[Vip] = {


    val c = Try(BuildVIP(imsEvent))
    c match {
      case Success(v) => v
      case Failure(e) => {
        _log.error(e.getStackTrace.toString)
        None
      }
    }

  }

  def BuildVIP(imsEvent: IMS_Event): Option[Vip] = {

    val globalAccessStartDate = ZonedDateTime.now(ZoneOffset.UTC).toEpochSecond

    val changes: List[String] = imsEvent.data.changes
    val providerTerritory = imsEvent.providerTerritory.toUpperCase
    val globalAccessEndDateDummy: Long = 9999999999999L



    changes match {
      case x if x.contains("services/nowtv") && providerTerritory.toUpperCase.equals("GB") => {
        val globalAccessEndDate = if (imsEvent.data.profile.services.nowtv.get.globalaccess == null) globalAccessEndDateDummy else imsEvent.data.profile.services.nowtv.get.globalaccess.toLong
        Some(Vip(imsEvent.data.profile.profileid, imsEvent.providerTerritory.toLowerCase, imsEvent.provider.toLowerCase, globalAccessEndDate, globalAccessStartDate))
      }
      case x if x.contains("services/nowtv") && providerTerritory.toUpperCase.equals("IE") => {
        val globalAccessEndDate = if (imsEvent.data.profile.services.`nowtv-ie`.get.globalaccess == null) globalAccessEndDateDummy else imsEvent.data.profile.services.`nowtv-ie`.get.globalaccess.toLong
        Some(Vip(imsEvent.data.profile.profileid, imsEvent.providerTerritory.toLowerCase, imsEvent.provider.toLowerCase, globalAccessEndDate, globalAccessStartDate))
      }
      case x if x.contains("services/skygo") && providerTerritory.toUpperCase.equals("GB") => {
        val globalAccessEndDate = if (imsEvent.data.profile.services.skygo.get.globalaccess == null) globalAccessEndDateDummy else imsEvent.data.profile.services.skygo.get.globalaccess.toLong
        Some(Vip(imsEvent.data.profile.profileid, imsEvent.providerTerritory.toLowerCase, imsEvent.provider.toLowerCase, globalAccessEndDate, globalAccessStartDate))
      }
      case _ => None
    }
  }


  def IMSEvent2Activity(imsEvent: IMS_Event): Option[Activity] = {
    Some(Activity(imsEvent.data.profile.profileid, imsEvent.provider.toLowerCase, imsEvent.providerTerritory.toLowerCase))

  }

































  /*  try {
      changes match {
        case x if x.contains("services/nowtv") && providerTerritory.toUpperCase.equals("GB") => {
          val globalAccessEndDate = if (imsEvent.data.profile.services.nowtv.get.globalaccess == null) globalAccessEndDateDummy else imsEvent.data.profile.services.nowtv.get.globalaccess.toLong
          Some(Vip(imsEvent.data.profile.profileid, imsEvent.providerTerritory.toLowerCase, imsEvent.provider.toLowerCase, globalAccessEndDate, globalAccessStartDate))
        }
        case x if x.contains("services/nowtv") && providerTerritory.toUpperCase.equals("IE") => {
          val globalAccessEndDate = if (imsEvent.data.profile.services.`nowtv-ie`.get.globalaccess == null) globalAccessEndDateDummy else imsEvent.data.profile.services.nowtv.get.globalaccess.toLong
          Some(Vip(imsEvent.data.profile.profileid, imsEvent.providerTerritory.toLowerCase, imsEvent.provider.toLowerCase, globalAccessEndDate, globalAccessStartDate))
        }
        case x if x.contains("services/skygo") && providerTerritory.toUpperCase.equals("GB") => {
          val globalAccessEndDate = if (imsEvent.data.profile.services.skygo.get.globalaccess == null) globalAccessEndDateDummy else imsEvent.data.profile.services.skygo.get.globalaccess.toLong
          Some(Vip(imsEvent.data.profile.profileid, imsEvent.providerTerritory.toLowerCase, imsEvent.provider.toLowerCase, globalAccessEndDate, globalAccessStartDate))
        }
        case _ => None
      }
    }
    catch
    {
      case e: Exception => _log.error(e.getStackTrace.toString)
        None
    }
*/




    /*try {

      if (changes.contains("services/nowtv") && providerTerritory.toUpperCase.equals("GB")) {
        val globalAccessEndDate = if (imsEvent.data.profile.services.nowtv.get.globalaccess == null) globalAccessEndDateDummy else imsEvent.data.profile.services.nowtv.get.globalaccess.toLong
        return Some(Vip(imsEvent.data.profile.profileid, imsEvent.providerTerritory.toLowerCase, imsEvent.provider.toLowerCase, globalAccessEndDate, globalAccessStartDate))
      }
      if (changes.contains("services/nowtv") && providerTerritory.toUpperCase.equals("IE")) {
        val globalAccessEndDate = if (imsEvent.data.profile.services.`nowtv-ie`.get.globalaccess == null) globalAccessEndDateDummy else imsEvent.data.profile.services.nowtv.get.globalaccess.toLong
        return Some(Vip(imsEvent.data.profile.profileid, imsEvent.providerTerritory.toLowerCase, imsEvent.provider.toLowerCase, globalAccessEndDate, globalAccessStartDate))
      }
      if (changes.contains("services/skygo") && providerTerritory.toUpperCase.equals("GB")) {
        val globalAccessEndDate = if (imsEvent.data.profile.services.skygo.get.globalaccess == null) globalAccessEndDateDummy else imsEvent.data.profile.services.skygo.get.globalaccess.toLong
        return Some(Vip(imsEvent.data.profile.profileid, imsEvent.providerTerritory.toLowerCase, imsEvent.provider.toLowerCase, globalAccessEndDate, globalAccessStartDate))
      }
    }
    catch
      {
        case e: Exception => _log.error(e.getStackTrace.toString)
          None
      }

    None

  } */

}
