package com.jvo.dto

import java.sql.Timestamp

case class WalletTransaction(providerId: String,
                             customerId: String,
                             gameRoundId: String,
                             transactionId: String,
                             transactionType: String,
                             transactionDate: Timestamp,
                             deviceType: String,
                             isFinal: Boolean,
                             amount: BigDecimal)
