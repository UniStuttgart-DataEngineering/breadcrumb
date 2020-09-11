package de.uni_stuttgart.ipvs.provenance.evaluation.twitter

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class TwitterScenario1 (spark: SparkSession, testConfiguration: TestConfiguration) extends TwitterScenario (spark, testConfiguration) {
  override def getName(): String = "T1"

  import spark.implicits._

  override def referenceScenario(): DataFrame = {
    val tw = loadTweets()
    var res = tw.withColumn("hashtag", explode($"entities.media")) // SA: hashtags -> media
    res = res.filter($"text".contains("Lebron") || $"hashtag.description".contains("Lebron")) // SA: hashtag.text -> hashtag.description
    res = res.select($"id_str", $"entities.media".alias("media"))
    res
  }

  override def whyNotQuestion(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val id = twig.createNode("id_str", 1, 1, "1027605897680510976")
    val media = twig.createNode("media", 1, 1, "")
    val element = twig.createNode("element", 1, 1, "")
    val url = twig.createNode("url", 1, 1, "containshttp")
    twig = twig.createEdge(root, id, false)
    twig = twig.createEdge(root, media, false)
    twig = twig.createEdge(media, element, false)
    twig = twig.createEdge(element, url, false)
    twig.validate.get
  }
}
