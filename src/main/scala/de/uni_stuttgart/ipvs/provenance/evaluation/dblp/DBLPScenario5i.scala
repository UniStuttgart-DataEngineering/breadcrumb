package de.uni_stuttgart.ipvs.provenance.evaluation.dblp

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.functions.{collect_list, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DBLPScenario5i(spark: SparkSession, testConfiguration: TestConfiguration) extends DBLPScenario (spark, testConfiguration) {
  override def getName: String = "D2i"

  import spark.implicits._

  override def referenceScenario: DataFrame = {

    val inproceedings = loadInproceedings()

    var inproceedings_flattened = inproceedings.withColumn("crf", explode($"crossref"))
    inproceedings_flattened = inproceedings_flattened.withColumn("iauthor", explode($"author"))
    val inproceedings_selected = inproceedings_flattened.select($"crf", $"iauthor._VALUE".alias("iauthor"), $"title._VALUE".alias("ititle"))
    inproceedings_selected.groupBy($"iauthor").agg(collect_list($"ititle").alias("ititleList"))

  }

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val text = twig.createNode("iauthor", 1, 1, "containsMelanie Herschel")
    twig = twig.createEdge(root, text, false)
    twig.validate.get
  }
}
