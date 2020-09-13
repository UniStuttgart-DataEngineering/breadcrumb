package de.uni_stuttgart.ipvs.provenance.evaluation.twitter

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class TwitterScenario1 (spark: SparkSession, testConfiguration: TestConfiguration) extends TwitterScenario (spark, testConfiguration) {
  override def getName(): String = "T1"

  import spark.implicits._

  override def referenceScenario(): DataFrame = {
    val tw = loadTweets()
    var res = tw.withColumn("hashtag", explode($"entities.hashtags")) // SA: hashtags -> media
    res = res.filter($"text".contains("Lebron") || $"hashtag.text".contains("Lebron")) // SA: hashtag.text -> hashtag.description
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

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    createAlternatives(primaryTree, 1)
    replace1(primaryTree.alternatives(0).rootNode)
//    replace2(primaryTree.alternatives(1).rootNode)
//    replace3(primaryTree.alternatives(2).rootNode)
    primaryTree
  }

  def replace1(node: SchemaNode): Unit ={
    if (node.name == "hashtags" && node.parent.name == "entities") {
      node.name = "media"
      return
    }
    for (child <- node.children){
      replace1(child)
    }
  }

  def replace2(node: SchemaNode): Unit ={
    if (node.name == "text" && node.parent.name == "hashtags") {
      node.name = "description"
      return
    }
    for (child <- node.children){
      replace2(child)
    }
  }

  var node1 = ""
  var node2 = ""

  def replace3(node: SchemaNode): Unit ={
    if (node.name == "hashtags") {
      node.name = "media"
      node1 = node.name
    }
    if (node.name == "text" && node.parent.name == node1) {
      node.name = "description"
      node2 = node.name
    }
    if (node1 == "media" && node2 == "description")
      return

    for (child <- node.children){
      replace3(child)
    }
  }

}
