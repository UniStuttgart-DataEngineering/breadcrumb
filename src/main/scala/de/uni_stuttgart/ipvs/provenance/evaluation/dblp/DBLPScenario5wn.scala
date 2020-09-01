package de.uni_stuttgart.ipvs.provenance.evaluation.dblp

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.functions.{collect_list, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DBLPScenario5wn(spark: SparkSession, testConfiguration: TestConfiguration) extends DBLPScenario (spark, testConfiguration) {
  override def getName: String = "D2wn"

  import spark.implicits._

  override def referenceScenario: DataFrame = {
    val proceedings = loadProceedings()
    val inproceedings = loadInproceedings()

    var inproceedings_flattened = inproceedings.withColumn("crf", explode($"crossref"))
    inproceedings_flattened = inproceedings_flattened.withColumn("iauthor", explode($"author"))
    val inproceedings_selected = inproceedings_flattened.select($"crf", $"iauthor._VALUE".alias("iauthor"), $"title._VALUE".alias("ititle"))
    val proceedings_springer = proceedings.filter($"publisher._VALUE" === "Springer") // RP
    val proceedings_tenyears = proceedings_springer.filter($"year" > 2016) // Potential RP?
    val proceedings_selected = proceedings_tenyears.select($"_key", $"publisher._VALUE".alias("ppublisher"), $"year")
    proceedings_selected.join(inproceedings_selected, $"_key" === $"crf")

  }

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val text = twig.createNode("iauthor", 1, 1, "containsMelanie Herschel")
    val year = twig.createNode("year", 1, 1, "2016")
    val publisher = twig.createNode("ppublisher", 1, 1, "containsACM")
    twig = twig.createEdge(root, year, false)
    twig = twig.createEdge(root, publisher)
    twig = twig.createEdge(root, text, false)
    twig.validate.get
  }
}
