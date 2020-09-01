package de.uni_stuttgart.ipvs.provenance.evaluation.dblp

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.functions.{collect_list, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DBLPScenario2(spark: SparkSession, testConfiguration: TestConfiguration) extends DBLPScenario (spark, testConfiguration) {
  override def getName: String = "D2"

  import spark.implicits._

  override def referenceScenario: DataFrame = {
    val inproceedings = loadInproceedings()
    val inproceedings_flattened = inproceedings.withColumn("iauthor", explode($"author"))
    val inproceedings_filter1 = inproceedings_flattened.filter($"iauthor._VALUE".contains("Adriane Chapman"))
    val inproceedings_filter2 = inproceedings_filter1.filter($"booktitle".contains("VLDB"))
    val res = inproceedings_filter2.select($"iauthor._VALUE".alias("author"), $"title._VALUE".alias("title"), $"booktitle")
    res
  }

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val text = twig.createNode("title", 1, 1, "containsWhy not?")
    twig = twig.createEdge(root, text, false)
    twig.validate.get
  }
}
