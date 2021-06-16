package com.jvo.service

import com.jvo.config.Constants._
import com.jvo.dto.{CustomerWalletTransactions, LossCheckEvent}

class GameRoundSettler(databaseService: DatabaseService) extends Serializable {

  def processGameRoundFinalTransaction(customerWalletTransaction: CustomerWalletTransactions): Option[LossCheckEvent] = {
    val gameRoundResults = getGameRoundResultsForCustomerFinalTransactions(customerWalletTransaction)
    gameRoundResults.foreach(result => databaseService.saveGameRoundResult(result))

    gameRoundResults
      .find(_.resultType == GameRoundLoss)
      .map(gameRoundResult => LossCheckEvent(
        customerWalletTransaction.individualTaxNumber,
        gameRoundResult.resultDate.toLocalDateTime.format(EventDateFormatter))
      )
  }

  private def getGameRoundResultsForCustomerFinalTransactions(customerWalletTransaction: CustomerWalletTransactions) = {
    customerWalletTransaction.transactions
      .flatMap(walletTransaction => databaseService
        .getCustomerGameRoundTransactions(customerWalletTransaction.individualTaxNumber, walletTransaction))
  }

}

object GameRoundSettler {
  def apply(databaseService: DatabaseService): GameRoundSettler = new GameRoundSettler(databaseService)
}