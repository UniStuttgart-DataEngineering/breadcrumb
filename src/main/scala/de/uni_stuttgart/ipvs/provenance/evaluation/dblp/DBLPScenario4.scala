package de.uni_stuttgart.ipvs.provenance.evaluation.dblp

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.functions.{collect_list, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DBLPScenario4(spark: SparkSession, testConfiguration: TestConfiguration) extends DBLPScenario (spark, testConfiguration) {
  override def getName: String = "D4"

  import spark.implicits._

  override def referenceScenario: DataFrame = {
    val proceedings = loadProceedings()
    val inproceedings = loadInproceedings()

    var inproceedings_flattened = inproceedings.withColumn("crf", explode($"crossref"))
    inproceedings_flattened = inproceedings_flattened.withColumn("iauthor", explode($"author"))
    val inproceedings_selected = inproceedings_flattened.select($"crf", $"iauthor._VALUE".alias("ipauthor"), $"title._VALUE".alias("ititle"))
    val proceedings_springer = proceedings.filter($"publisher._VALUE".contains("ACM")) // SA: series._VALUE
    val proceedings_tenyears = proceedings_springer.filter($"year".contains("2015")) // SA: _mdate
    val proceedings_selected = proceedings_tenyears.select($"_key", $"year")
    val proceedings_with_inproceedings = proceedings_selected.join(inproceedings_selected, $"_key" === $"crf")
    val proceedings_with_inproceedingsRenamed = proceedings_with_inproceedings.withColumnRenamed("ipauthor", "author")
    val res = proceedings_with_inproceedingsRenamed.groupBy($"author").agg(collect_list($"ititle").alias("ititleList"))
    res
  }

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val text = twig.createNode("author", 1, 1, "containsGeorge V. Tsoulos")
//    val title = twig.createNode("ititleList", 1, 1, "")
//    val child1 = twig.createNode("element", 1, 1, "Metrics for Measuring the Performance of the Mixed Workload CH-benCHmark.")
    twig = twig.createEdge(root, text, false)
//    twig = twig.createEdge(root, title, false)
//    twig = twig.createEdge(title, child1, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    val saSize = testConfiguration.schemaAlternativeSize
    createAlternatives(primaryTree, saSize)

    for (i <- 0 until saSize) {
      if (math.abs(i % 6) == 0) {
        replace1(primaryTree.alternatives(i).rootNode)
      }
      if (math.abs(i % 6) == 1) {
        replace2(primaryTree.alternatives(i).rootNode)
      }
      if (math.abs(i % 6) == 2) {
        replace3(primaryTree.alternatives(i).rootNode)
      }
    }

    primaryTree
  }

  def replace1(node: SchemaNode): Unit ={
    if (node.name == "publisher") {
      node.name = "series"
      return
    }
    for (child <- node.children){
      replace1(child)
    }
  }

  def replace2(node: SchemaNode): Unit ={
    if (node.name == "year") {
      node.name = "_mdate"
      return
    }
    for (child <- node.children){
      replace2(child)
    }
  }

  var node1 = ""
  var node2 = ""

  def replace3(node: SchemaNode): Unit ={
    if (node.name == "publisher") {
      node.name = "series"
      node1 = node.name
    }
    if (node.name == "year") {
      node.name = "_mdate"
      node2 = node.name
    }
    if (node1 == "series" && node2 == "_mdate")
      return

    for (child <- node.children){
      replace3(child)
    }
  }

}
