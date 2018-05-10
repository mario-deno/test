package com.sky.dap.eu_portability.data

import com.sky.dap.eu_portability.model.IMS_Event

/**
  * Created by dap on 4/30/18.
  */
object IMSEvents {


  def quotes(s: String, n: Int = 1) = ("\"" * n) + s + ("\"" * n)

  def convertToJson(mapJson: Map[String, String]) = {
    "{" + mapJson.map({ case (k, v) => quotes(k.toString) + ":" + quotes(v.toString) }).mkString(",") + "}"
  }

  def transformFromIMSToJson(imsEvent: IMS_Event) = {
    def mapJson = Map("" -> "") // TODO : Insert fields
    convertToJson(mapJson)
  }


  val eventFake: (String, String) = ("999", "{\"FakeEvent\":\"fake_event_body\"}")
  val eventNotInPerimeter: (String, String) = ("999", "{\"event\":\"NOT_IN_PERIMETER\"}")
  val eventInPerimeterAndEuPortabilityNotActive: (String, String) = ("999", "{\"event\":\"PROFILE_MODIFIED_EUP\", \"data\": {\"euportability\":\"false\"}}")
  val eventInPerimeter: (String, String) = ("999", "{\"event\":\"PROFILE_MODIFIED_EUP\", \"data\": {\"euportability\":\"true\"}}")

  // TODO : Update of event aligned to type


  val manualOverrideEventEuportabilityFalse =
    """{
             "activityTimestamp":"2018-02-15T16:29:52.039Z",
             "provider":"SKY",
             "event":"PROFILE_MODIFIED_EUP",
             "data":{
                "profile":{
                   "services":{

                   },
                   "mobilenumber":null,
                   "targetedoptin":true,
                   "targetedoptoutdate":null,
                   "email":"3iydhpyw@example.com",
                   "passwordrequireschange":false,
                   "targetedoptindate":1518712191987,
                   "hhuserauthorised":null,
                   "hhusertype":"primary",
                   "username":"TEST_RIGKFNC",
                   "skyoptin":true,
                   "dateofbirth":null,
                   "firstname":"Badger",
                   "nsprofileid":"084e06cc-478f-4ca2-81c1-56bceb1d0a55",
                   "hhid":"2297238189",
                   "euportability":false,
                   "emailchanged":1518712191987,
                   "contactemail":"3iydhpyw@example.com",
                   "lastname":"Mole",
                   "mobilenumberverified":null,
                   "displayname":null,
                   "securityquestionid":null,
                   "profileid":"20157337",
                   "mobilenumberchanged":null,
                   "emailverified":false,
                   "trackingid":"6+jOd1EFdY0BVH8qcVk23GPVWcPp4wOXtKI36oViANI=",
                   "title":"Mr",
                   "registrationdate":1518712191987,
                   "termsandconditionsaccepted":true
                },
                "changes":[
                   "euportability"
                ]
             },
             "id":"b5ea7dea-4afb-4ad1-bdf6-37ff9a6151a0",
             "providerTerritory":"GB",
             "origin":"oogway-intl",
             "profileid":"20157337",
             "originatingSystem":"IMS",
             "householdId":"20157337",
             "activityType":"PROFILE_MODIFIED"
          }"""


  val validManualOverrideEvent =
    """{
             "activityTimestamp":"2018-02-15T16:29:52.039Z",
             "provider":"SKY",
             "event":"PROFILE_MODIFIED_EUP",
             "data":{
                "profile":{
                   "services":{
          
                   },
                   "mobilenumber":null,
                   "targetedoptin":true,
                   "targetedoptoutdate":null,
                   "email":"3iydhpyw@example.com",
                   "passwordrequireschange":false,
                   "targetedoptindate":1518712191987,
                   "hhuserauthorised":null,
                   "hhusertype":"primary",
                   "username":"TEST_RIGKFNC",
                   "skyoptin":true,
                   "dateofbirth":null,
                   "firstname":"Badger",
                   "nsprofileid":"084e06cc-478f-4ca2-81c1-56bceb1d0a55",
                   "hhid":"2297238189",
                   "euportability":true,
                   "emailchanged":1518712191987,
                   "contactemail":"3iydhpyw@example.com",
                   "lastname":"Mole",
                   "mobilenumberverified":null,
                   "displayname":null,
                   "securityquestionid":null,
                   "profileid":"20157337",
                   "mobilenumberchanged":null,
                   "emailverified":false,
                   "trackingid":"6+jOd1EFdY0BVH8qcVk23GPVWcPp4wOXtKI36oViANI=",
                   "title":"Mr",
                   "registrationdate":1518712191987,
                   "termsandconditionsaccepted":true
                },
                "changes":[
                   "euportability"
                ]
             },
             "id":"b5ea7dea-4afb-4ad1-bdf6-37ff9a6151a0",
             "providerTerritory":"GB",
             "origin":"oogway-intl",
             "profileid":"20157337",
             "originatingSystem":"IMS",
             "householdId":"20157337",
             "activityType":"PROFILE_MODIFIED"
          }"""


  val validVIPEvent =
    """{
             "activityTimestamp":"2018-02-15T16:29:52.039Z",
             "provider":"SKY",
             "event":"PROFILE_MODIFIED_GLOBAL_ACCESS",
             "data":{
                "profile":{
                   "services":{

                   },
                   "mobilenumber":null,
                   "targetedoptin":true,
                   "targetedoptoutdate":null,
                   "email":"3iydhpyw@example.com",
                   "passwordrequireschange":false,
                   "targetedoptindate":1518712191987,
                   "hhuserauthorised":null,
                   "hhusertype":"primary",
                   "username":"TEST_RIGKFNC",
                   "skyoptin":true,
                   "dateofbirth":null,
                   "firstname":"Badger",
                   "nsprofileid":"084e06cc-478f-4ca2-81c1-56bceb1d0a55",
                   "hhid":"2297238189",
                   "euportability":true,
                   "emailchanged":1518712191987,
                   "contactemail":"3iydhpyw@example.com",
                   "lastname":"Mole",
                   "mobilenumberverified":null,
                   "displayname":null,
                   "securityquestionid":null,
                   "profileid":"20157337",
                   "mobilenumberchanged":null,
                   "emailverified":false,
                   "trackingid":"6+jOd1EFdY0BVH8qcVk23GPVWcPp4wOXtKI36oViANI=",
                   "title":"Mr",
                   "registrationdate":1518712191987,
                   "termsandconditionsaccepted":true
                },
                "changes":[
                   "euportability"
                ]
             },
             "id":"b5ea7dea-4afb-4ad1-bdf6-37ff9a6151a0",
             "providerTerritory":"GB",
             "origin":"oogway-intl",
             "profileid":"20157337",
             "originatingSystem":"IMS",
             "householdId":"20157337",
             "activityType":"PROFILE_MODIFIED"
          }"""


  val invalidManualOverrideEvent =
    """{
  "activityTimestamp":"2018-02-15T16:29:52.039Z",
  "provider":"S555KY321",
  "event":"PROFILE_MODIFIED_GLOBAL_ACCESS",
  "data":{
    "profile":{
    "services":{

  },
    "mobilenumber":null,
    "targetedoptin":true,
    "targetedoptoutdate":null,
    "email":"3iydhpyw@example.com",
    "passwordrequireschange":false,
    "targetedoptindate":1518712191987,
    "hhuserauthorised":null,
    "hhusertype":"primary",
    "username":"TEST_RIGKFNC",
    "skyoptin":true,
    "dateofbirth":null,
    "firstname":"Badger",
    "nsprofileid":"084e06cc-478f-4ca2-81c1-56bceb1d0a55",
    "hhid":"2297238189",
    "euportability":true,
    "emailchanged":1518712191987,
    "contactemail":"3iydhpyw@example.com",
    "lastname":"Mole",
    "mobilenumberverified":null,
    "displayname":null,
    "securityquestionid":null,
    "profileid":"201573aaa37",
    "mobilenumberchanged":null,
    "emailverified":false,
    "trackingid":"6+jOd1EFdY0BVH8qcVk23GPVWcPp4wOXtKI36oViANI=",
    "title":"Mr",
    "registrationdate":1518712191987,
    "termsandconditionsaccepted":true
  },
    "changes":[
    "euportabilityASD"
    ]
  },
  "id":"b5ea7dea-4afb-4ad1-bdf6-37ff9a6151a0",
  "providerTerritory":"GB74",
  "origin":"oogway-intl",
  "profileid":"20157337AAA",
  "originatingSystem":"IMS",
  "householdId":"20157337",
  "activityType":"PROFILE_MODIFIED"
}"""

}
