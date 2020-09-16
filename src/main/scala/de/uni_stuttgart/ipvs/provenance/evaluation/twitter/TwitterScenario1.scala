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
    var res = tw.withColumn("medias", explode($"entities.media")) // SA: media -> urls
//    res = res.filter($"text".contains("Lebron") || $"hashtag.text".contains("Lebron")) // SA: hashtag.text -> hashtag.description
    res = res.filter($"text".contains("Lebron"))
    res = res.select($"id_str", $"medias.url".alias("media_url"))
    res
  }

  override def whyNotQuestion(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val id = twig.createNode("id_str", 1, 1, "1027612080084414464")
//    val media = twig.createNode("media", 1, 1, "")
//    val element = twig.createNode("element", 1, 1, "")
    val url = twig.createNode("media_url", 1, 1, "containshttp")
    twig = twig.createEdge(root, id, false)
//    twig = twig.createEdge(root, media, false)
//    twig = twig.createEdge(media, element, false)
    twig = twig.createEdge(root, url, false)
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
      node.name = "urls"
      return
    }
    for (child <- node.children){
      replace1(child)
    }
  }

//  def replace2(node: SchemaNode): Unit ={
//    if (node.name == "text" && node.parent.name == "hashtags") {
//      node.name = "description"
//      return
//    }
//    for (child <- node.children){
//      replace2(child)
//    }
//  }
//
//  var node1 = ""
//  var node2 = ""
//
//  def replace3(node: SchemaNode): Unit ={
//    if (node.name == "hashtags") {
//      node.name = "media"
//      node1 = node.name
//    }
//    if (node.name == "text" && node.parent.name == node1) {
//      node.name = "description"
//      node2 = node.name
//    }
//    if (node1 == "media" && node2 == "description")
//      return
//
//    for (child <- node.children){
//      replace3(child)
//    }
//  }

}
