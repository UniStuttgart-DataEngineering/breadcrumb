package de.uni_stuttgart.ipvs.provenance.evaluation.dblp

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class DBLPScenario10(spark: SparkSession, testConfiguration: TestConfiguration) extends DBLPScenario (spark, testConfiguration) {
  override def getName: String = "D5"

  import spark.implicits._

  override def referenceScenario: DataFrame = {
    val inproceedings = loadInproceedings()
    val www = loadWWW()

    val www_author = www.withColumn("wauthor", explode($"author"))
//    val www_url = www_author.withColumn("wurl", explode($"url"))
//    val www_selected = www_author.select($"wauthor._VALUE".alias("wauthorName"), $"wurl._VALUE".alias("url"), $"title".alias("wtitle"))
    val www_selected = www_author.select($"wauthor._VALUE".alias("wauthorName"), $"url".alias("wurl"), $"title".alias("wtitle"))
    val inproceedings_flattened = inproceedings.withColumn("iauthor", explode($"author"))
    val inproceedings_selected = inproceedings_flattened.select($"iauthor._VALUE".alias("iauthorName"), $"title._VALUE".alias("ititle"), $"ee")
    var joined = www_selected.join(inproceedings_selected, $"wauthorName" === $"iauthorName")
    joined = joined.withColumn("url", explode($"wurl")).withColumnRenamed("iauthorName", "name")
    val res = joined.groupBy($"name").agg(collect_list($"url").alias("listOfUrl"))
//      agg(collect_list($"ititle").alias("listOfIn"), collect_list($"wtitle").alias("listOfW"), collect_list($"url").alias("listOfUrl"))
//    val res = joined.filter($"iauthorName".contains("A. Allam"))
    res
  }

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val text = twig.createNode("name", 1, 1, "containsA. Allam")
//    val url = twig.createNode("listOfUrl", 1, 1, "")
//    val element = twig.createNode("element", 1, 1, "")
    twig = twig.createEdge(root, text, false)
//    twig = twig.createEdge(root, url, false)
//    twig = twig.createEdge(url, element, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    createAlternatives(primaryTree, 1)
    replace1(primaryTree.alternatives(0).rootNode)
    primaryTree
  }

  def replace1(node: SchemaNode): Unit ={
    if (node.name == "url") {
      node.name = "ee"
      return
    }
    for (child <- node.children){
      replace1(child)
    }
  }

}
