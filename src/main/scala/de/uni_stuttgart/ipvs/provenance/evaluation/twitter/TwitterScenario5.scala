package de.uni_stuttgart.ipvs.provenance.evaluation.twitter

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class TwitterScenario5(spark: SparkSession, testConfiguration: TestConfiguration) extends TwitterScenario (spark, testConfiguration) {
  override def getName(): String = "T5"

  import spark.implicits._

  override def referenceScenario(): DataFrame = {
    val tw = loadTweets()
    var res = tw.select($"id", $"text", $"user.id".alias("user"), $"place.country".alias("country")) // Schema Alternative: user.location
    res = res.filter($"country".contains("Deutschland") || $"country".contains("Germany")) // Schema Alternative: user.location
    res = res.agg(count("id").alias("numOfTweets"))
//    res = res.agg(max($"numOfTweets").alias("maxCount"))
//    res.sort(desc("numOfTweets"))
    res
  }

  override def whyNotQuestion(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val count = twig.createNode("numOfTweets", 1, 1, "gtgtgtgt100")
    twig = twig.createEdge(root, count, false)
    twig.validate.get
  }
}
