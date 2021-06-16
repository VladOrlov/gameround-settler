package com.jvo.dto

case class CustomerWalletTransactions(individualTaxNumber: String, transactions: Seq[WalletTransaction])
