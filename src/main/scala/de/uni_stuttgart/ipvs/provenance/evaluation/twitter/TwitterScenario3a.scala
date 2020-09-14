package de.uni_stuttgart.ipvs.provenance.evaluation.twitter

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.functions.{avg, countDistinct, explode, size }
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SparkSession}

class TwitterScenario3a(spark: SparkSession, testConfiguration: TestConfiguration) extends TwitterScenario (spark, testConfiguration) {
  override def getName(): String = "T3a"

  import spark.implicits._

  override def referenceScenario(): DataFrame = {
    val tw = loadTweets()
    val extended = tw.withColumn("media_size", size($"entities.media") cast(LongType))
    val filtered = extended.filter($"media_size" > 0)
    //val mentioned = filtered.withColumn("mentioned_user", explode($"entities.user_mentions"))
    //  .select($"mentioned_user.id".alias("uid"), $"mentioned_user.name".alias("name"), $"media_size")
    val users = filtered.select($"user.id".alias("uid"), $"user.name".alias("name"), $"media_size")
    val res = users.groupBy($"uid", $"name").agg(avg($"media_size").alias("media_mentions"))
    res
  }

  override def whyNotQuestion(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val name = twig.createNode("name", 1, 1, "Vanessa Tuqueque")
    val count = twig.createNode("media_mentions", 1, 1, "gtgtgtgt1")
    twig = twig.createEdge(root, name, false)
    twig = twig.createEdge(root, count, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    createAlternatives(primaryTree, 1)
    replace1(primaryTree.alternatives(0).rootNode)
    primaryTree
  }

  def replace1(node: SchemaNode): Unit ={
    if (node.name == "media" &&
      node.parent.name == "entities" &&
      node.parent.parent.name == "root") {
      node.parent.name = "extended_entities"
      return
    }
    for (child <- node.children){
      replace1(child)
    }
  }

}
