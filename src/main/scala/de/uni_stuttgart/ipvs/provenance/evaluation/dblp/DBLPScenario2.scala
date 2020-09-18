package de.uni_stuttgart.ipvs.provenance.evaluation.dblp

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class DBLPScenario2(spark: SparkSession, testConfiguration: TestConfiguration) extends DBLPScenario (spark, testConfiguration) {
  override def getName: String = "D2"

  import spark.implicits._

  override def referenceScenario: DataFrame = {
    val article = loadArticle()
    val article_flattened = article.withColumn("aauthor", explode($"author"))
    val article_select = article_flattened.select($"aauthor._VALUE".alias("author"), $"title._bibtex".alias("title")) // SchemaAlternative: "title._VALUE"
//    val article_notnull = article_select.filter($"title".isNotNull || $"title" != "null" || $"title" != "")
    val article_filter = article_select.filter(!$"author".contains("Dey")) // SchemaAlternative: "ee._VALUE"
    val res = article_filter.groupBy($"author").agg(count($"title").alias("cnt"))
    article_filter
    res
  }

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val author = twig.createNode("author", 1, 1, "containsSudeepa Roy")
    val count = twig.createNode("cnt", 1, 1, "gtgtgtgt5")
    twig = twig.createEdge(root, author, false)
    twig = twig.createEdge(root, count, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    val saSize = testConfiguration.schemaAlternativeSize
    createAlternatives(primaryTree, saSize)

    for (i <- 0 until saSize by 2) {
      replaceBibtex(primaryTree.alternatives(i).rootNode)
    }

    primaryTree
  }

  def replaceBibtex(node: SchemaNode): Unit ={
    if (node.name == "_bibtex") {
      node.name = "_VALUE"
      node.modified = true
      return
    }
    for (child <- node.children){
      replaceBibtex(child)
    }
  }

}
