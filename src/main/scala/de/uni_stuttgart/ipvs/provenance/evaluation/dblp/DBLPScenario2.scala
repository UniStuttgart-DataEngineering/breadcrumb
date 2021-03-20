package de.uni_stuttgart.ipvs.provenance.evaluation.dblp

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.functions.{collect_list, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DBLPScenario2(spark: SparkSession, testConfiguration: TestConfiguration) extends DBLPScenario (spark, testConfiguration) {
  override def getName: String = "D2"

  import spark.implicits._

  override def referenceScenario: DataFrame = {
    val proceedings = loadProceedings()
    val inproceedings = loadInproceedings()

    var inproceedings_flattened = inproceedings.withColumn("crf", explode($"crossref"))
    inproceedings_flattened = inproceedings_flattened.withColumn("iauthor", explode($"author"))
    val inproceedings_selected = inproceedings_flattened.select($"crf", $"iauthor._VALUE".alias("iauthor"), $"title._VALUE".alias("ititle"))
    val proceedings_springer = proceedings.filter($"publisher._VALUE" === "Springer") // RP
    val proceedings_tenyears = proceedings_springer.filter($"year" > 2010) // Potential RP?
    val proceedings_selected = proceedings_tenyears.select($"_key", $"publisher._VALUE".alias("ppublisher"), $"year")
    val proceedings_with_inproceedings = proceedings_selected.join(inproceedings_selected, $"_key" === $"crf")
    val res = proceedings_with_inproceedings.groupBy($"iauthor").agg(collect_list($"ititle").alias("ititleList"))

//    inproceedings.show(false)
//    inproceedings_selected.show(false)
//
//    proceedings.show(false)
//    proceedings_selected.show(false)

    res
  }

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val text = twig.createNode("iauthor", 1, 1, "Melanie Herschel")
    twig = twig.createEdge(root, text, false)
    twig.validate.get
  }
}
