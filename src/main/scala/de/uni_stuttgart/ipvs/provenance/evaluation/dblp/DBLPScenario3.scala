package de.uni_stuttgart.ipvs.provenance.evaluation.dblp

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class DBLPScenario3(spark: SparkSession, testConfiguration: TestConfiguration) extends DBLPScenario (spark, testConfiguration) {
  override def getName: String = "D3"

  import spark.implicits._

  override def referenceScenario: DataFrame = {
    val article = loadArticle()
    val article_flattened = article.withColumn("aauthor", explode($"author"))
    val article_select = article_flattened.select($"aauthor._VALUE".alias("author"), $"journal".alias("title"))
    val res = article_select.groupBy($"author").agg(countDistinct($"title").alias("cnt"))
    res
  }

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val author = twig.createNode("author", 1, 1, "containsSudeepa Roy")
    val count = twig.createNode("cnt", 1, 1, "ltltltlt17")
    twig = twig.createEdge(root, author, false)
    twig = twig.createEdge(root, count, false)
    twig.validate.get
  }
}
