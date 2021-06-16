package com.jvo.service

import com.datastax.oss.driver.api.core.cql.{ResultSet, Row}
import com.datastax.spark.connector.cql.CassandraConnector
import com.jvo.config.Constants._
import com.jvo.config.dto.CassandraConfig
import com.jvo.dto.{CustomerPlayResult, GameRoundResult, WalletTransaction}
import org.apache.spark.SparkConf

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime}
import scala.collection.mutable.ArrayBuffer
import scala.math.BigDecimal.RoundingMode

class DatabaseService(val connector: CassandraConnector,
                      val transactionTable: String,
                      val gameRoundResultTable: String,
                      val bigLossEventTable: String,
                      val keyspace: String) extends Serializable {

  def getCustomerGameRoundTransactions(individualTaxNumber: String, walletTransaction: WalletTransaction): Seq[GameRoundResult] = {
    val query = getSelectGameRoundTransactionsQuery(walletTransaction)
    val resultSet: ResultSet = connector.withSessionDo(session => session.execute(query))

    val result = ArrayBuffer[GameRoundResult]()
    resultSet.forEach { row =>

      result += GameRoundResult(
        individualTaxNumber = individualTaxNumber,
        gameRoundId = row.getObject(GameRoundIdColumn).asInstanceOf[String],
        providerId = row.getObject(ProviderIdColumn).asInstanceOf[String],
        resultType = getGameRoundResultType(row),
        resultDate = Timestamp.from(row.getObject(ResultDateColumn).asInstanceOf[Instant]),
        amount = BigDecimal.valueOf(row.getObject(GameRoundAmountColumn).asInstanceOf[Double]).setScale(2, RoundingMode.UP)
      )
    }

    result
  }

  private def getGameRoundResultType(row: Row) = {
    if (BigDecimal.valueOf(row.getObject(GameRoundAmountColumn).asInstanceOf[Double]) > 0)
      GameRoundWin
    else
      GameRoundLoss
  }

  private def getSelectGameRoundTransactionsQuery(walletTransaction: WalletTransaction): String = {
    s"""
       |SELECT
       |$ProviderIdColumn,
       |$CustomerIdColumn,
       |$GameRoundIdColumn,
       |MAX($TransactionDateColumn) as result_date,
       |SUM($AmountColumn) as game_round_amount
       |FROM $keyspace.$transactionTable
       |WHERE
       |$ProviderIdColumn = '${walletTransaction.providerId}'
       |AND $CustomerIdColumn = '${walletTransaction.customerId}'
       |AND $GameRoundIdColumn = '${walletTransaction.gameRoundId}'
       |GROUP BY
       |$ProviderIdColumn,
       |$CustomerIdColumn,
       |$GameRoundIdColumn
       |""".stripMargin
  }

  def getCustomerPlayResultForPeriod(customerId: String, cutOffDateTime: LocalDateTime): Option[CustomerPlayResult] = {

    val query = getCustomerPlayResultQuery(customerId, cutOffDateTime)
    val resultSet: ResultSet = connector.withSessionDo(session => session.execute(query))

    val result = ArrayBuffer[CustomerPlayResult]()
    resultSet.forEach { row =>

      result += CustomerPlayResult(
        customerId = row.getObject(CustomerIdColumn).asInstanceOf[String],
        lossAmount = BigDecimal.valueOf(row.getObject(LossAmountColumn).asInstanceOf[Double]).setScale(2, RoundingMode.UP),
        fromDateTime = Timestamp.from(row.getObject(FromDateTimeColumn).asInstanceOf[Instant]),
        toDateTime = Timestamp.from(row.getObject(ToDateTimeColumn).asInstanceOf[Instant])
      )
    }

    result.headOption
  }

  private def getCustomerPlayResultQuery(customerId: String, cutOffDateTime: LocalDateTime): String = {
    s"""
       |SELECT
       | $CustomerIdColumn,
       | SUM($AmountColumn) as $LossAmountColumn,
       | MIN(result_date) as $FromDateTimeColumn,
       | MAX(result_date) as $ToDateTimeColumn
       | FROM $keyspace.$gameRoundResultTable
       | WHERE $CustomerIdColumn = '$customerId' AND $ResultDateColumn > '${cutOffDateTime.format(TimestampCassandraQueryFormatter)}'
       | GROUP BY $CustomerIdColumn
       |""".stripMargin
  }

  def saveGameRoundResult(gameRoundResult: GameRoundResult): ResultSet = {
    val insertQuery = getInsertGameRoundResultQuery(gameRoundResult)
    connector.withSessionDo(session => session.execute(insertQuery))
  }

  private def getInsertGameRoundResultQuery(gameRoundResult: GameRoundResult): String = {
    s"""
       |INSERT INTO $keyspace.$gameRoundResultTable(
       | $IndividualTaxNumberColumn,
       | $ResultDateColumn,
       | $GameRoundIdColumn,
       | $ProviderIdColumn,
       | $ResultTypeColumn,
       | $AmountColumn)
       | VALUES (
       | '${gameRoundResult.individualTaxNumber}',
       | '${gameRoundResult.resultDate}',
       | '${gameRoundResult.gameRoundId}',
       | '${gameRoundResult.providerId}',
       | '${gameRoundResult.resultType}',
       | ${gameRoundResult.amount})
       |""".stripMargin
  }

  def saveBigLossEvent(customerPlayResult: CustomerPlayResult): ResultSet = {
    val insertQuery = getInsertBigLossEventQuery(customerPlayResult)
    connector.withSessionDo(session => session.execute(insertQuery))
  }

  private def getInsertBigLossEventQuery(customerPlayResult: CustomerPlayResult): String = {
    s"""
       |INSERT INTO $keyspace.$bigLossEventTable(
       | $CustomerIdColumn,
       | $FromDateTimeColumn,
       | $ToDateTimeColumn,
       | $EventIdColumn,
       | $LossAmountColumn)
       | VALUES (
       | '${customerPlayResult.customerId}',
       | '${customerPlayResult.fromDateTime}',
       | '${customerPlayResult.toDateTime}',
       | '${customerPlayResult.id}',
       | ${customerPlayResult.lossAmount.abs})
       """.stripMargin
  }

  def getCustomerLatestBigLossEventDate(customerId: String): Option[LocalDateTime] = {
    val query = getCustomerLatestBigLossEventQuery(customerId)
    val resultSet: ResultSet = connector.withSessionDo(session => session.execute(query))

    val result = ArrayBuffer[LocalDateTime]()
    resultSet.forEach { row =>
      result += Timestamp.from(row.getObject(LatestBigLossTimestamp).asInstanceOf[Instant]).toLocalDateTime
    }

    result.headOption
  }

  private def getCustomerLatestBigLossEventQuery(customerId: String) = {
    s""" SELECT
       | MAX($ToDateTimeColumn) as latest_big_loss_timestamp
       | FROM $keyspace.$bigLossEventTable
       | WHERE $CustomerIdColumn = '$customerId'
       | GROUP BY $CustomerIdColumn
       |""".stripMargin
  }
}

object DatabaseService {
  def apply(cassandraConfig: CassandraConfig, sparkConfig: SparkConf): DatabaseService =
    new DatabaseService(
      CassandraConnector(sparkConfig),
      cassandraConfig.transactionTable,
      cassandraConfig.gameRoundResultTable,
      cassandraConfig.bigLossEventTable,
      cassandraConfig.keyspace)
}