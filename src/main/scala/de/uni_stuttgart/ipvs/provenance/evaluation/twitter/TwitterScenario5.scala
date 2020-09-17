package de.uni_stuttgart.ipvs.provenance.evaluation.twitter

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class TwitterScenario5(spark: SparkSession, testConfiguration: TestConfiguration) extends TwitterScenario (spark, testConfiguration) {
  override def getName(): String = "T5"

  import spark.implicits._

  override def referenceScenario(): DataFrame = {
    val tw = loadTweets()
    //TODO: compatibles are not correctly marked
//    var res = tw.select($"id", $"user.location".alias("location"), $"place.name".alias("pname"))
//    var res = tw.withColumn("pname", $"place.name")
//    res = res.withColumn("hashtag", explode($"entities.hashtags"))
//    res = res.withColumn("medias", explode($"entities.media")) // SA: entities.media -> entities.urls
//    res = res.select($"pname", $"hashtag.text".alias("hashtagText"), $"medias.url".alias("uurl"), $"user.location".alias("location")) // SA: user.location -> place.full_name
//    res = res.filter($"location".contains("CA"))
//    res = res.groupBy($"pname", $"hashtagText").agg(collect_list($"uurl").alias("listOfUrls"))
////    res = res.filter($"pname".contains("Los Angeles"))

    var res = tw.withColumn("pname", $"place.name")
    res = res.filter($"user.location".contains("CA")) // SA: user.location -> place.full_name
    res = res.groupBy($"pname").agg(collect_list($"id_str").alias("listOfTweets"))
//    res = res.filter($"pname".contains("San Diego"))
    res
  }

  override def whyNotQuestion(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
//    val city = twig.createNode("pname", 1, 1, "containsLos Angeles")
//    val element = twig.createNode("hashtagText", 1, 1, "immixeleven")
//    val list2 = twig.createNode("listOfUrls", 1, 1, "")
//    val element2 = twig.createNode("element", 1, 1, "containshttps")
//    twig = twig.createEdge(root, city, false)
//    twig = twig.createEdge(root, element, false)
//    twig = twig.createEdge(root, list2, false)
//    twig = twig.createEdge(list2, element2, false)
    val name = twig.createNode("pname", 1, 1, "containsSan Diego")
    val list = twig.createNode("listOfTweets", 1, 1, "")
    val element = twig.createNode("element", 1, 1, "contains1027608724662185984")
    twig = twig.createEdge(root, name, false)
    twig = twig.createEdge(root, list, false)
    twig = twig.createEdge(list, element, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    val saSize = testConfiguration.schemaAlternativeSize
    createAlternatives(primaryTree, saSize)

    for (i <- 0 until saSize by 2) {
      replace2(primaryTree.alternatives(i).rootNode)
    }

//    for (i <- 0 until saSize) {
//      if (math.abs(i % 6) == 0) {
//        replace1(primaryTree.alternatives(i).rootNode)
//      }
//      if (math.abs(i % 6) == 1) {
//        replace2(primaryTree.alternatives(i).rootNode)
//      }
//      if (math.abs(i % 6) == 2) {
//        replace3(primaryTree.alternatives(i).rootNode)
//      }
//    }

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

  def replace2(node: SchemaNode): Unit ={
    if (node.name == "user" &&
        node.parent.name == "root") {
      node.name = "place"
      node.getChild("location").get.name = "full_name"
      return
    }
    for (child <- node.children){
      replace2(child)
    }
  }

  var node1 = ""
  var node2 = ""

  def replace3(node: SchemaNode): Unit ={
    if (node.name == "media" &&
      node.parent.name == "entities" &&
      node.parent.parent.name == "root") {
      node.name = "urls"
      node1 = node.name
    }
    if (node.name == "user" &&
      node.parent.name == "root") {
      node.name = "place"
      node.getChild("location").get.name = "full_name"
      node2 = node.name
    }
    if (node1 == "urls" && node2 == "place")
      return

    for (child <- node.children){
      replace3(child)
    }

  }

}
