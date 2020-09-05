package de.uni_stuttgart.ipvs.provenance.evaluation.dblp

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.functions.{collect_list, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DBLPScenario9(spark: SparkSession, testConfiguration: TestConfiguration) extends DBLPScenario (spark, testConfiguration) {
  override def getName: String = "D9"

  import spark.implicits._

  override def referenceScenario: DataFrame = {
    val proceedings = loadProceedings()
    val inproceedings = loadInproceedings()
    val articles = loadArticle()

    val inproceedings_flattened = inproceedings.withColumn("crf", explode($"crossref"))
    val inproceedings_select = inproceedings_flattened.select($"author", $"title", $"crf")
    val proceedings_select = proceedings.select($"_key", $"series".alias("series_title"))
    val all_proceedings = inproceedings_select.join(proceedings_select, proceedings_select("_key") === inproceedings_select("crf"))
    val all_proceedings_select = all_proceedings.select($"author", $"title", $"series_title")
    val articles_select = articles.select($"author", $"title", $"journal".alias("series_title"))
    val res = all_proceedings_select.union(articles_select)
    res
  }

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val text = twig.createNode("series_title", 1, 1, "31st IEEE International Conference on Data Engineering, ICDE 2015, Seoul, South Korea, April 13-17, 2015")
    twig = twig.createEdge(root, text, false)
    twig.validate.get
  }
}
