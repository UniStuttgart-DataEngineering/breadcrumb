package de.uni_stuttgart.ipvs.provenance.evaluation.twitter

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class TwitterScenario3(spark: SparkSession, testConfiguration: TestConfiguration) extends TwitterScenario (spark, testConfiguration) {
  override def getName(): String = "T3"

  import spark.implicits._

  override def referenceScenario(): DataFrame = {
    val tw = loadTweets()
    val media = tw.withColumn("medias", explode($"entities.media")) // Schema Alternative: media -> urls
    val mentioned = media.withColumn("mentioned_user", explode($"entities.user_mentions"))
//    mentioned.select($"id", $"created_at", $"mentioned_user", $"entities.media", $"entities.urls").filter($"mentioned_user.name".contains("YouTube"))
    val extracted_mentioned_users = mentioned.select($"created_at", $"text", $"id".alias("tid"),
      $"mentioned_user.id".alias("id"), $"mentioned_user.id_str".alias("id_str"),
      $"mentioned_user.name".alias("name"), $"mentioned_user.screen_name".alias("screen_name"),
      $"medias.url".alias("murl"))
    val restructured_users = extracted_mentioned_users.select(
      struct($"id", $"id_str", $"name", $"screen_name").alias("user_mentioned"),
      struct($"created_at", $"text", $"tid").alias("tweet"),
      $"murl")
    val res = restructured_users.groupBy($"user_mentioned").agg(count($"tweet").alias("numOfTweets"), collect_list($"murl").alias("numOfUrls"))
//    res.filter($"user_mentioned.screen_name".contains("YouTube"))
    res
  }

  override def whyNotQuestion(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val user = twig.createNode("user_mentioned", 1, 1, "")
    val name = twig.createNode("screen_name", 1, 1, "YouTube")
    val count = twig.createNode("numOfTweets", 1, 1, "gtgtgtgt100")
    twig = twig.createEdge(root, user, false)
    twig = twig.createEdge(user, name, false)
    twig = twig.createEdge(root, count, false)
    twig.validate.get
  }
}
