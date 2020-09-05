package de.uni_stuttgart.ipvs.provenance.evaluation.dblp

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.functions.{collect_list, explode, count}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DBLPScenario8(spark: SparkSession, testConfiguration: TestConfiguration) extends DBLPScenario (spark, testConfiguration) {
  override def getName: String = "D8"

  import spark.implicits._

  override def referenceScenario: DataFrame = {
    val proceedings = loadProceedings()
    val inproceedings = loadInproceedings()
    val article = loadArticle()

    var inproceedings_flattened = inproceedings.withColumn("crf", explode($"crossref"))
    inproceedings_flattened = inproceedings_flattened.withColumn("iauthor", explode($"author"))
    val inproceedings_selected = inproceedings_flattened.select($"_key".alias("ikey"), $"iauthor._VALUE".alias("allauthors"), $"title._VALUE".alias("alltitles"))
    val proceedings_springer = proceedings.filter($"publisher._VALUE" === "Springer")
    val proceedings_select = proceedings_springer.select($"_key".alias("pkey"), $"title".alias("series_title"), $"year")
    val proceedings_with_inproceedings = proceedings_select.join(inproceedings_selected, $"ikey" === $"pkey")
    val all_in_proceedings = proceedings_with_inproceedings.select($"allauthors", $"alltitles", $"series_title", $"year")
    val article_flattened = article.withColumn("aauthor", explode($"author"))
    val article_sigmod = article_flattened.filter($"journal".contains("SIGMOD"))
    val article_select = article_sigmod.select($"aauthor._VALUE".alias("allauthors"), $"title._VALUE".alias("alltitles"), $"journal".alias("series_title"), $"year")
    val all = all_in_proceedings.union(article_select)
    val all_twentyfifteen = all.filter($"year" > 2015)
    val res = all_twentyfifteen.groupBy($"allauthors").agg(count($"series_title").alias("cnt"))
    res
  }

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val text = twig.createNode("allauthors", 1, 1, "containsThomas Neumann")
    twig = twig.createEdge(root, text, false)
    twig.validate.get
  }
}
