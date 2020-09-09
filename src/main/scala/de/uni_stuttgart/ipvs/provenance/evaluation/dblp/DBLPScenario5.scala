package de.uni_stuttgart.ipvs.provenance.evaluation.dblp

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class DBLPScenario5(spark: SparkSession, testConfiguration: TestConfiguration) extends DBLPScenario (spark, testConfiguration) {
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
    joined = joined.withColumn("url", explode($"wurl"))
    val res = joined.groupBy($"iauthorName").agg(collect_list($"url").alias("listOfUrl"))
//      agg(collect_list($"ititle").alias("listOfIn"), collect_list($"wtitle").alias("listOfW"), collect_list($"url").alias("listOfUrl"))
//    val res = joined.filter($"iauthorName".contains("A. Allam"))
    res
  }

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val text = twig.createNode("iauthorName", 1, 1, "containsA. Allam")
//    val url = twig.createNode("listOfUrl", 1, 1, "")
//    val element = twig.createNode("element", 1, 1, "")
    twig = twig.createEdge(root, text, false)
//    twig = twig.createEdge(root, url, false)
//    twig = twig.createEdge(url, element, false)
    twig.validate.get
  }
}
