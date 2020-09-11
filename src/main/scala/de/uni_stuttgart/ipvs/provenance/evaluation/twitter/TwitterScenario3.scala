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
    val mentioned = tw.withColumn("mentioned_user", explode($"entities.user_mentions"))
    val media = mentioned.withColumn("medias", explode($"entities.media")) // Schema Alternative: media -> urls
    val extracted_mentioned_users = media.select($"id".alias("tid"), $"created_at", $"text",
      $"mentioned_user.id".alias("uid"), $"mentioned_user.name".alias("name"), $"mentioned_user.screen_name".alias("screen_name"),
//      $"entities.urls".alias("media"))
      $"medias.url".alias("murl"))
    val extracted_mentioned_users_with_media = extracted_mentioned_users.filter($"murl".contains("http"))
    val restructured_users = extracted_mentioned_users_with_media.select(
      struct($"uid", $"name", $"screen_name").alias("user_mentioned"),
      struct($"created_at", $"text", $"tid").alias("tweet"))
//      $"murl")
    var res = restructured_users.groupBy($"user_mentioned").agg(count($"tweet").alias("numOfTweets"))
////      , collect_list($"murl").alias("numOfUrls"))
//    var res = extracted_mentioned_users.filter($"screen_name".contains("YouTube"))
    res = res.sort(desc("numOfTweets"))
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
