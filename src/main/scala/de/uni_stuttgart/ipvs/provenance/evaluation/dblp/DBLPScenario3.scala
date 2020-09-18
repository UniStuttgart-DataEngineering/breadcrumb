package de.uni_stuttgart.ipvs.provenance.evaluation.dblp

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class DBLPScenario3(spark: SparkSession, testConfiguration: TestConfiguration) extends DBLPScenario (spark, testConfiguration) {
  override def getName: String = "D3"

  import spark.implicits._

  override def referenceScenario: DataFrame = {
    val inproceedings = loadInproceedings()
    val inproceedings_select = inproceedings.select($"booktitle".alias("btitle"), $"year".alias("iyear"),
          struct($"author", $"title._VALUE".alias("ititle")).alias("authorPaperPair")) // SA: editor
    val res = inproceedings_select.groupBy($"btitle", $"iyear").agg(collect_list($"authorPaperPair").alias("listOfAuthorPapers"))
//    val inproceedings_flattened = inproceedings.withColumn("iauthor", explode($"author")) // SA: editor
//    val inproceedings_select = inproceedings_flattened.select($"booktitle", $"year",
//          struct($"iauthor._VALUE".alias("author"), $"title._VALUE".alias("ititle")).alias("authorPaperPair"))
//    val res = inproceedings_select.groupBy($"booktitle", $"year").agg(collect_list($"authorPaperPair").alias("listOfAuthorPapers"))
    res
  }

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val booktitle = twig.createNode("btitle", 1, 1, "containsHICSS")
    val year = twig.createNode("iyear", 1, 1, "contains2006")
    val list = twig.createNode("listOfAuthorPapers", 1, 1, "")
    val element = twig.createNode("element", 1, 1, "")
    val author = twig.createNode("author", 1, 1, "")
    val element2 = twig.createNode("element", 1, 1, "")
    val name = twig.createNode("_VALUE", 1, 1, "containsGail Corbitt")
    val title = twig.createNode("ititle", 1,1, "containsMinitrack Introduction")
    twig = twig.createEdge(root, booktitle, false)
    twig = twig.createEdge(root, year, false)
    twig = twig.createEdge(root, list, false)
    twig = twig.createEdge(list, element, false)
    twig = twig.createEdge(element, author, false)
    twig = twig.createEdge(author, element2, false)
    twig = twig.createEdge(element2, name, false)
    twig = twig.createEdge(element, title, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    val saSize = testConfiguration.schemaAlternativeSize
    createAlternatives(primaryTree, saSize)

    for (i <- 0 until saSize by 2) {
      replaceTitle(primaryTree.alternatives(i).rootNode)
    }

    primaryTree
  }

  def replaceTitle(node: SchemaNode): Unit ={
    if (node.name == "author") {
      node.name = "editor"
      node.modified = true
      return
    }
    for (child <- node.children){
      replaceTitle(child)
    }
  }

}
