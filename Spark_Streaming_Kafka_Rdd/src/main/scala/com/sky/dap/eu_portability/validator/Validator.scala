package com.sky.dap.eu_portability.validator

import com.sky.dap.eu_portability.model._



object Validator {

  def validateGlobalAccessEvent(event: IMS_Event): (List[String],Boolean) = {


    //for a given message build a list of couple with all error message
    //____________________
    //not a string| false |
    //null        | true  |
    //not a number|false  |

    val errorList = List[(String,Boolean)](
      (validateString(event.provider)),
      (validateNumeric(event.data.profile.profileid)),
      (validateString(event.providerTerritory)),
      (validateGlobalAccessExpectedService(event.data.changes))
      //TODO: Controllare che l'evento arrivi con questa configurazione (servizio,country)
      /*nowtv,ie
      nowtv,gb
      sky,gb
      (ogni altra configurazione da un problema) */
    )

    val isValidResult  = true
    val errorListResult = List[String]()

    //return a copule with 1st element list of all error and 2nd element logic AND of each validation done before
    (errorList.map(z => z._1),errorList.forall(z => z._2))



  }

  def validateEuPortabilityEvent(event: IMS_Event): (List[String],Boolean) = {

    //for a given message build a list of couple with all error message
    //____________________
    //not a string| false |
    //null        | true  |
    //not a number|false  |

    val errorList = List[(String,Boolean)](
      (validateString(event.provider)),
      (validateNumeric(event.data.profile.profileid)),
      (validateString(event.providerTerritory)),
      (validateManualOverrideExpectedChanges(event.data.changes))
      //TODO: Controllare che l'evento arrivi con questa configurazione (servizio,country)
      /*nowtv,ie
      nowtv,gb
      sky,gb
      (ogni altra configurazione da un problema) */
    )

    val isValidResult  = true
    val errorListResult = List[String]()

    //return a couple with 1st element list of all error and 2nd element logic AND of each validation done before
    (errorList.map(z => z._1),errorList.forall(z => z._2))
  }


  //given a string return  (eventually an error message , if the string is a number)
  def validateNumeric(str: String): (String,Boolean) = { if (str.matches("[-+]?\\d+(\\.\\d+)?")) (null,true) else ("not a number",false) }
  //given a string return  (error message , if the string is a sequence of character)
  def validateString(str: String): (String,Boolean) = { if ( str.matches("([a-zA-Z])*")) (null,true) else ("not a string",false) }
  //given a string return  (error message , if the string is a sequence of number or null)
  def isGlobalAccessDate(str: String): (String,Boolean) = { if (str.matches("(\\d+)|(null)")) (null,true) else ("global access not valid: expected epoch time or null",false) }
  //given a List of string return  (error message , if the list contains a valid service)
  def validateGlobalAccessExpectedService(str: List[String]): (String,Boolean) = { if (str.contains("services/nowtv") || str.contains("services/skygo")) (null,true) else ("services not valid: expected nowtv|skygo",false) }
  def validateManualOverrideExpectedChanges(str: List[String]): (String,Boolean) = { if (str.contains("euportability")) (null,true) else ("changes not valid: expected euportability",false) }

}
