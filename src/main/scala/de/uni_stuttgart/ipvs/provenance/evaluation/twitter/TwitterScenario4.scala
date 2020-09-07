package de.uni_stuttgart.ipvs.provenance.evaluation.twitter

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class TwitterScenario4(spark: SparkSession, testConfiguration: TestConfiguration) extends TwitterScenario (spark, testConfiguration) {
  override def getName(): String = "T4"

  import spark.implicits._

  override def referenceScenario(): DataFrame = {
    val tw = loadTweets()
    var res = tw.withColumn("hashtag", explode($"entities.hashtags"))
    res = res.select($"id", $"hashtag.text".alias("hashtagText"), $"text", $"user.id".alias("user"), $"place.country".alias("country")) // Schema Alternative: country -> location
//                          when($"lang".equalTo("en"),"1").otherwise("2").alias("langInt"))
    res = res.filter($"text".contains("War"))
    res = res.groupBy($"hashtagText").agg(count($"country").alias("numOfCountries")) // Schema Alternative: country -> location
    res = res.filter($"numOfCountries" > 0)
    res
  }

  override def whyNotQuestion(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val user = twig.createNode("hashtagText", 1, 1, "Warcraft")
    twig = twig.createEdge(root, user, false)
    twig.validate.get
  }
}
