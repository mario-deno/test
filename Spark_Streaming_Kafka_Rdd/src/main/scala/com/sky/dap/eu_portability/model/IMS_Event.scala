package com.sky.dap.eu_portability.model

/**
  * Created by dap on 4/10/18.
  */

case class IMS_Event_NowTv
(
  globalaccess:String
)

case class IMS_Event_Services
(
  nowtv : Option[IMS_Event_NowTv],
  skygo : Option[IMS_Event_NowTv],
  `nowtv-ie` : Option[IMS_Event_NowTv]
)


case class IMS_Event_Profile
(
  services : IMS_Event_Services,
  email : String,
  username : String,
  profileid : String,
  euportability : Boolean

)

case class IMS_Event_Data
(
  profile : IMS_Event_Profile,
  changes: List[String]
)

case class IMS_Event
(
  provider: String,
  event:String,
  providerTerritory:String,
  data : IMS_Event_Data
)


