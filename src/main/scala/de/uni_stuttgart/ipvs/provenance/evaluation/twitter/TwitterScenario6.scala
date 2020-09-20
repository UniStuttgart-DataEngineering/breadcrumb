package de.uni_stuttgart.ipvs.provenance.evaluation.twitter

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.functions.{avg, countDistinct, explode, size, count, sum, lit}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

class TwitterScenario6(spark: SparkSession, testConfiguration: TestConfiguration) extends TwitterScenario (spark, testConfiguration) {
  override def getName(): String = "T3"

  import spark.implicits._

  override def referenceScenario(): DataFrame = {
    val tw = loadTweets()

    var res = tw.select($"user.id".alias("uid"), $"user.name".alias("name"),
      $"entities.hashtags".alias("hashtag"), $"entities.media".alias("medias")) //SA: entities.media -> extended_entities.media
    res = res.withColumn("moreMedia", explode($"medias"))
    res = res.withColumn("hashtags", explode($"hashtag"))
    res = res.filter($"hashtags.text".contains("red"))
    res = res.groupBy($"uid", $"name").agg(countDistinct($"moreMedia").alias("numOfMedia"))
    res = res.filter($"numOfMedia" > 2)
//    res.filter($"name".contains("Coca cola"))
    res

//     TODO: flatten on extended_tweet.entities.hashtags + filter on hashtag.text
//    val extended = tw.withColumn("media_size", size($"extended_entities.media") cast(LongType))
//    var filtered = flattened.filter($"media_size" > 1)
//    filtered
//    f.select($"user.name", $"user.screen_name", $"hashtag.text")
//    filtered = filtered.filter($"hashtag.text".contains("NewZealand"))
//    val mentioned = filtered.withColumn("mentioned_user", explode($"entities.user_mentions"))
//      .select($"mentioned_user.id".alias("uid"), $"mentioned_user.name".alias("name"), $"media_size")
//    val users = filtered.select($"user.id".alias("uid"), $"user.name".alias("name"), $"media_size")//.alias("media_mentions"))
//          .filter($"media_size" === 1)
//        .filter($"user.name".contains("Social Media Marketing"))
//    users
//    var res = users.groupBy($"uid", $"name").agg(avg($"media_size").alias("media_mentions"))
//    res
  }

  override def whyNotQuestion(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val name = twig.createNode("name", 1, 1, "Coca cola")
    val count = twig.createNode("numOfMedia", 1, 1, "gtgtgtgt1")
    twig = twig.createEdge(root, name, false)
    twig = twig.createEdge(root, count, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    val saSize = testConfiguration.schemaAlternativeSize
    createAlternatives(primaryTree, saSize)

    for (i <- 0 until saSize by 2) {
      replace1(primaryTree.alternatives(i).rootNode)
    }

    primaryTree
  }

  def replace1(node: SchemaNode): Unit ={
    if (node.name == "media" &&
      node.parent.name == "entities" &&
      node.parent.parent.name == "root") {
      node.parent.name = "extended_entities"
      node.parent.modified = true
      return
    }
    for (child <- node.children){
      replace1(child)
    }
  }

}
