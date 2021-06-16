package com.jvo.utils

import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.jvo.dto.WalletTransaction
import org.apache.logging.log4j.LogManager

import scala.util.{Failure, Success, Try}

object JsonObjectMapper {

  private[this] final val log = LogManager.getLogger(this.getClass)

  private val mapper = new ObjectMapper() with ScalaObjectMapper
  mapper.registerModule(DefaultScalaModule)
  mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
  mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)


  def parseToWalletTransaction(sourceJson: String): WalletTransaction = {
    Try(mapper.readValue[WalletTransaction](sourceJson)) match {
      case Success(v) => v
      case Failure(e) => throw new RuntimeException(e)
    }
  }

  def parseToWalletTransactionList(sourceJson: String): Seq[WalletTransaction] = {
    Try(mapper.readValue[Seq[WalletTransaction]](sourceJson)) match {
      case Success(v) => v
      case Failure(e) => throw new RuntimeException(e)
    }
  }

  def convertToWalletTransactionsJson(walletBonusTransactions: Seq[WalletTransaction]): String = {
    Try(toJson(walletBonusTransactions)) match {
      case Success(v) => v
      case Failure(e) =>
        val errorMessage = s"Failed to convert wallet transactions list " +
          s"${walletBonusTransactions.toString} into json ${e.getMessage}"
        log.error(errorMessage)
        throw new Exception(errorMessage)
    }
  }

  def toJson(sourceObject: AnyRef): String = {
    mapper.writeValueAsString(sourceObject)
  }
}