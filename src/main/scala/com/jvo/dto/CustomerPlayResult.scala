package com.jvo.dto

import java.sql.Timestamp
import java.util.UUID

case class CustomerPlayResult(id: String = UUID.randomUUID().toString,
                              customerId: String,
                              lossAmount: BigDecimal,
                              fromDateTime: Timestamp,
                              toDateTime: Timestamp)