package com.jvo.dto

import java.sql.Timestamp

case class GameRoundResult(individualTaxNumber: String,
                           gameRoundId: String,
                           providerId: String,
                           resultType: String,
                           resultDate: Timestamp,
                           amount: BigDecimal)
