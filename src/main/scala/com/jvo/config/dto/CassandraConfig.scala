package com.jvo.config.dto

case class CassandraConfig(host: String,
                           keyspace: String,
                           transactionTable: String,
                           gameRoundResultTable: String,
                           bigLossEventTable: String)