package de.uni_stuttgart.ipvs.provenance.evaluation.dblp

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.functions.{collect_list, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DBLPScenario1(spark: SparkSession, testConfiguration: TestConfiguration) extends DBLPScenario (spark, testConfiguration) {
  override def getName: String = "D1"

  import spark.implicits._

  override def referenceScenario: DataFrame = {
    val proceedings = loadProceedings()
    val inproceedings = loadInproceedings()
    var inproceedings_flattened = inproceedings.withColumn("crf", explode($"crossref"))
    inproceedings_flattened = inproceedings_flattened.withColumn("iauthor", explode($"author"))
    val inproceedings_selected = inproceedings_flattened.select($"crf", $"iauthor._VALUE".alias("author"), $"title._VALUE".alias("ititle"))
    val proceedings_selected = proceedings.select($"_key", $"title".alias("proceeding")) // SA: title -> booktitle
    val proceedings_with_inproceedings = proceedings_selected.join(inproceedings_selected, $"_key" === $"crf")
    val sigmod = proceedings_with_inproceedings.filter($"proceeding".contains("SIGMOD"))
    val res = sigmod.select($"author", $"ititle", $"proceeding")
    res
  }

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
//    val name = twig.createNode("author", 1, 1, "containsSridhar Ramaswamy")
//    val text = twig.createNode("title", 1, 1, "containsSelectivity Estimation in Spatial Databases")
    val text = twig.createNode("ititle", 1, 1, "containsScalable algorithms for scholarly figure mining and semantics")
//    twig = twig.createEdge(root, name, false)
    twig = twig.createEdge(root, text, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    createAlternatives(primaryTree, 1)
    replaceTitle(primaryTree.alternatives(0).rootNode)
    primaryTree
  }

  def replaceTitle(node: SchemaNode): Unit ={
    // to avoid "title._VALUE"
    if (node.name == "title" && node.children.isEmpty) {
      node.name = "booktitle"
      return
    }
    for (child <- node.children){
      replaceTitle(child)
    }
  }
}
