package de.uni_stuttgart.ipvs.provenance.evaluation.dblp

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class DBLPScenario5(spark: SparkSession, testConfiguration: TestConfiguration) extends DBLPScenario (spark, testConfiguration) {
  override def getName: String = "D5"

  import spark.implicits._

  override def referenceScenario: DataFrame = {
    val www = loadWWW()
    val www_author = www.withColumn("wauthor", explode($"author"))
    val www_url = www_author.withColumn("wurl", explode($"url")) // SA: url -> note
    val www_selected = www_url.select($"wauthor._VALUE".alias("name"), $"wurl._VALUE".alias("url"))
    var res = www_selected.groupBy($"name").agg(collect_list($"url").alias("listOfUrl"))
//    res = res.filter($"name".contains("Sinziana Mazilu"))
    res
  }

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val name = twig.createNode("name", 1, 1, "containsSinziana Mazilu")
//    val list = twig.createNode("listOfUrl", 1, 1000000, "")
//    val elem = twig.createNode("element", 1, 1, "containshttps://orcid.org/0000-0001-8552-0934")
    twig = twig.createEdge(root, name, false)
//    twig = twig.createEdge(root, list, false)
//    twig = twig.createEdge(list, elem, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    createAlternatives(primaryTree, 1)
    replace1(primaryTree.alternatives(0).rootNode)
    primaryTree
  }

  def replace1(node: SchemaNode): Unit ={
    if (node.name == "url" && node.parent.name == "root") {
      node.name = "note"
      return
    }
    for (child <- node.children){
      replace1(child)
    }
  }

}
