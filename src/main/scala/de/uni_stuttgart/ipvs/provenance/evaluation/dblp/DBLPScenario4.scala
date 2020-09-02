package de.uni_stuttgart.ipvs.provenance.evaluation.dblp

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.{DataFrame, SparkSession}

class DBLPScenario4(spark: SparkSession, testConfiguration: TestConfiguration) extends DBLPScenario (spark, testConfiguration) {
  override def getName: String = "D4"

  import spark.implicits._

  override def referenceScenario: DataFrame = {
    val inproceedings = loadInproceedings()
    val proceedings = loadProceedings()
    val article = loadArticle()

    val inproceedings_flattened = inproceedings.withColumn("iauthor", explode($"author"))
    val inproceedings_filter = inproceedings_flattened.filter($"iauthor._VALUE".contains("Oliver Kennedy"))
//    val inproceedings_flattened2 = inproceedings_filter.withColumn("crf", explode($"crossref"))
//    val inproceedings_select = inproceedings_flattened2.select($"crf".alias("ikey"), $"title._VALUE".alias("title"))
    val inproceedings_select = inproceedings_filter.select($"_key".alias("ikey"), $"title._VALUE".alias("title"))
    val proceedings_select = proceedings.select($"_key".alias("pkey"), $"series._VALUE".alias("series_title"))
    val inproceedings_proceedings = inproceedings_select.join(proceedings_select, $"ikey" === $"pkey")
    val inproceedings_proceedings_select = inproceedings_proceedings.select($"title", $"series_title")
    val article_select = article.select($"title._VALUE".alias("title"), $"journal".alias("series_title"))
    val res = inproceedings_proceedings_select.union(article_select)
    res
  }

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val title = twig.createNode("title", 1, 1, "containsDBToaster: Agile Views for a Dynamic Data Management System.")
//    val series = twig.createNode("series_title", 1, 1, "containsCIDR 2011, Fifth Biennial Conference on Innovative Data Systems Research")
//    val series = twig.createNode("series_title", 1, 1, "CIDR 2011, Fifth Biennial Conference on Innovative Data Systems Research, Asilomar, CA, USA, January 9-12, 2011, Online Proceedings")
    twig = twig.createEdge(root, title, false)
//    twig = twig.createEdge(root, series, false)
    twig.validate.get
  }
}
