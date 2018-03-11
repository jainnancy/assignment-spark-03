package edu.knoldus

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession, DataFrame}
import org.apache.spark.sql.functions._

object Matches extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  val log = Logger.getLogger(this.getClass)

  /**
    * Q-1. Read the input CSV file using Spark Session.
    */
  val sparkSession = SparkSession.builder()
    .master("local")
    .appName("assignment-spark-03")
    .getOrCreate()

  val allDataFrame: DataFrame = sparkSession.read.option("header", "true").csv("src/main/resources/D1.csv")
  allDataFrame.createOrReplaceTempView("matchDetails")
  println("Q-1. Read the input CSV file using Spark Session")
  allDataFrame.select("HomeTeam", "AwayTeam", "FTHG", "FTAG", "FTR").show()

  /**
    * Q2. Total number of match played by each team as HOME TEAM.
    */
  val asHomeTeam = sparkSession.sql("SELECT HomeTeam,count(1) AS HomeTeamMatchCount FROM matchDetails GROUP BY HomeTeam")
  asHomeTeam.distinct().createOrReplaceTempView("HomePlayed")
  println("Q2. Total number of match played by each team as HOME TEAM.")
  asHomeTeam.select("HomeTeam", "HomeTeamMatchCount").show()

  /**
    * Q3. Top 10 team with highest wining percentage.
    */
  val asAwayTeam = sparkSession.sql("SELECT AwayTeam,count(1) AS AwayTeamMatchCount FROM matchDetails GROUP BY AwayTeam")
  asAwayTeam.createOrReplaceTempView("AwayPlayed")

  val homeWins = sparkSession.sql("SELECT HomeTeam,count(1) As HomeWinsCount FROM matchDetails WHERE FTR = 'H' GROUP BY HomeTeam")
  homeWins.createOrReplaceTempView("HomeWins")
  //homeWins.select("HomeTeam", "HomeWinsCount")

  val awayWins = sparkSession.sql("SELECT AwayTeam,count(1) As AwayWinsCount FROM matchDetails WHERE FTR = 'A' GROUP BY AwayTeam")
  awayWins.createOrReplaceTempView("AwayWins")
  //awayWins.select("AwayTeam", "AwayWinsCount")

  val totalMatches = asHomeTeam.join(asAwayTeam, asHomeTeam("HomeTeamMatchCount") === asAwayTeam("AwayTeamMatchCount"))
  totalMatches.createOrReplaceTempView("TotalMatches")
  val matches = sparkSession.sql("SELECT HomeTeam, (HomeTeamMatchCount + AwayTeamMatchCount) as NumberOfMatches FROM TotalMatches")

  val totalWins = homeWins.join(awayWins, homeWins("HomeTeam") === awayWins("AwayTeam"))
  totalWins.createOrReplaceTempView("TotalWins")
  val wins = sparkSession.sql("SELECT HomeTeam AS Teams, (HomeWinsCount + AwayWinsCount) As NumberOfWins FROM TotalWins")

  matches.join(wins, matches("HomeTeam") === wins("Teams")).createOrReplaceTempView("FinalResult")


  val result = sparkSession.sql("SELECT HomeTeam AS Teams,((NumberOfWins/NumberOfMatches)*100) AS WinPercentage" +
    " From FinalResult ORDER BY WinPercentage DESC LIMIT 10")
  println("Q3. Top 10 team with highest wining percentage.")
  result.show()

  /**
    * Q-4. Convert the DataFrame created in Q1 to DataSet by using
    * only following fields.
    */

  import sparkSession.implicits._

  case class MatchesDb(HomeTeam: String, AwayTeam: String, FTHG: String, FTAG: String, FTR: String)

  val allDataSet: Dataset[MatchesDb] = allDataFrame.as[MatchesDb]

  /**
    * Q-5. Total number of match played by each team.
    */

  println("Q-5. Total number of match played by each team.")
  allDataSet.select($"HomeTeam")
    .union(allDataSet.select($"AwayTeam"))
    .groupBy($"HomeTeam")
    .count()
    .show()

  /**
    * Q-6. Top Ten team with highest wins.
    */
  val homeTeam = allDataSet.select("HomeTeam", "FTR").where("FTR = 'H'").groupBy("HomeTeam").count().withColumnRenamed("count", "HomeWins")
  val awayTeam = allDataSet.select("AwayTeam", "FTR").where("FTR = 'A'").groupBy("AwayTeam").count().withColumnRenamed("count", "AwayWins")
  val teams = homeTeam.join(awayTeam, homeTeam.col("HomeTeam") === awayTeam.col("AwayTeam"))
  val sum: (Int, Int) => Int = (HomeMatches: Int, TeamMatches: Int) => HomeMatches + TeamMatches

  val total = udf(sum)
  println("Q-6. Top Ten team with highest wins.")
  teams.withColumn ("TotalWins", total (col ("HomeWins"), col ("AwayWins"))).select ("HomeTeam", "TotalWins")
    .withColumnRenamed ("HomeTeam", "Team").sort (desc ("TotalWins")).limit (10).show ()

}
