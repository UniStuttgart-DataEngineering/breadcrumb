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
    var res = tw.select($"id", $"place.country".alias("pcountry")) // SA: user.location
//    res = res.filter($"pcountry".contains("Deutschland") || $"pcountry".contains("Germany"))
    res = res.filter($"pcountry".contains("Germany"))
    res = res.agg(count("id").alias("numOfTweets"))
    res
  }

  override def whyNotQuestion(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val count = twig.createNode("numOfTweets", 1, 1, "gtgtgtgt100")
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
    if (node.name == "place" && node.parent.name == "root") {
      node.name = "user"
      node.getChild("country").get.name = "location"
      return
    }
    for (child <- node.children){
      replace1(child)
    }
  }

}
