package de.uni_stuttgart.ipvs.provenance.evaluation.dblp

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.functions.{collect_list, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

class DBLPScenario5(spark: SparkSession, testConfiguration: TestConfiguration) extends DBLPScenario (spark, testConfiguration) {
  override def getName: String = "D5"

  import spark.implicits._

  override def referenceScenario: DataFrame = {
    val proceedings = loadProceedings()
    val inproceedings = loadInproceedings()

    var inproceedings_flattened = inproceedings.withColumn("crf", explode($"crossref"))
    inproceedings_flattened = inproceedings_flattened.withColumn("iauthor", explode($"author"))
    val inproceedings_selected = inproceedings_flattened.select($"crf", $"iauthor._VALUE".alias("ipauthor"), $"title._VALUE".alias("ititle"))
    val proceedings_springer = proceedings.filter($"publisher._VALUE" === "Springer")
    val proceedings_fiveyears = proceedings_springer.filter($"year" > 2015)
    val proceedings_selected = proceedings_fiveyears.select($"_key", $"publisher._VALUE".alias("ppublisher"), $"year")
    val proceedings_with_inproceedings = proceedings_selected.join(inproceedings_selected, $"_key" === $"crf")
    val res = proceedings_with_inproceedings.groupBy($"ipauthor").agg(collect_list($"ititle").alias("ititleList"))
//    res.printSchema()
//    res.explain(true)
    res
  }

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val text = twig.createNode("ipauthor", 1, 1, "containsThomas Neumann")
//    val title = twig.createNode("ititleList", 1, 1, "")
//    val child1 = twig.createNode("element", 1, 1, "Metrics for Measuring the Performance of the Mixed Workload CH-benCHmark.")
    twig = twig.createEdge(root, text, false)
//    twig = twig.createEdge(root, title, false)
//    twig = twig.createEdge(title, child1, false)
    twig.validate.get
  }
}
