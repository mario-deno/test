package com.sky.dap.eu_portability.integration

import com.sky.dap.eu_portability.UnitSpec

/**
  * Created by dap on 4/11/18.
  */
class E2E_IdentityListenerTest extends  UnitSpec("EndToEndTest")  {
  Given("Send Correct Event")
    // Generate event method
  When("Search into Cassandra")
    // Search in Cassandra Method
  Then("Found a record")
    // Assertion Record Found


  Given("Send Wrong Event")
  // Generate event method no correct
  When("Search into Cassandra")
  // Search in Cassandra Method
  Then("Not Found a record")
  // Assertion No Record founded




  def logic(): Unit =
  {
    //load config
    //define perimeter
    //exclude not in perimeter
    //deserialize message (extraction with model)
    //exclude failures
    //validation filter
    //transformation from Event to VIP
    //insert into cassandra
  }

}
