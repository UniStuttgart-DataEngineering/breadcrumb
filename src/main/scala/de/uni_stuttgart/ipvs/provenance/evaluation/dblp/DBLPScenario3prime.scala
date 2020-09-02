package de.uni_stuttgart.ipvs.provenance.evaluation.dblp

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.functions.{explode, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DBLPScenario3prime(spark: SparkSession, testConfiguration: TestConfiguration) extends DBLPScenario (spark, testConfiguration) {
  override def getName: String = "D3prime"

  import spark.implicits._

  override def referenceScenario: DataFrame = {
    val article = loadArticle()
    val article_flattened = article.withColumn("aauthor", explode($"author"))
    val article_select = article_flattened.select($"aauthor._VALUE".alias("author"), $"title._VALUE".alias("title"))
    val article_agg = article_select.groupBy($"author").agg(countDistinct($"title").alias("cnt"))
    val article_filter = article_agg.filter($"cnt" > 10)
    val res = article_filter.select($"author")
    article_agg
  }

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val author = twig.createNode("author", 1, 1, "containsSudeepa Roy Dey")
//    val count = twig.createNode("cnt", 1, 1, "4")
    twig = twig.createEdge(root, author, false)
//    twig = twig.createEdge(root, count, false)
    twig.validate.get
  }
}
