package com.sky.dap.eu_portability.model

case class Activity(
                     profile_id: String,
                     provider: String,
                     home_country: String
                   )
{

  override def toString: String =
    s"""
       | Profile_Id           --> $profile_id
       | Provider             --> $provider
       | Home Country         --> $home_country
       """.stripMargin

}