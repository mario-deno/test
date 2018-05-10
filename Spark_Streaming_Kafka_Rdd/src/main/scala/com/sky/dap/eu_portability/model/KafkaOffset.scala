package com.sky.dap.eu_portability.model


case class KafkaOffset
(
  group_id: String,
  topic: String,
  partition: Int,
  next_offset:Long
)
