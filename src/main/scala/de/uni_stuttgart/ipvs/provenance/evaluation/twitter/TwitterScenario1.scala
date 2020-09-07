package de.uni_stuttgart.ipvs.provenance.evaluation.twitter

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class TwitterScenario1 (spark: SparkSession, testConfiguration: TestConfiguration) extends TwitterScenario (spark, testConfiguration) {
  override def getName(): String = "T1"

  import spark.implicits._

  override def referenceScenario(): DataFrame = {
    val df = loadTweets()
//    df.filter($"text".contains("BTS"))
    var res = df.withColumn("hashtag", explode($"entities.hashtags")) // Schema Alternative: hashtags -> media
    res = res.select($"id", $"user.name".alias("name"), $"hashtag.text".alias("hashtagText"), $"text") // Schema Alternative: text -> description
    res = res.filter($"text".contains("history"))
//    res.filter($"hashtagText".contains("Lincoln"))
    res
  }

  override def whyNotQuestion(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val text = twig.createNode("hashtagText", 1, 1, "Lincoln")
    twig = twig.createEdge(root, text, false)
    twig.validate.get
  }
}
