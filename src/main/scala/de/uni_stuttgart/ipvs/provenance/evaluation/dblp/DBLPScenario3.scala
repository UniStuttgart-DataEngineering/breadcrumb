package de.uni_stuttgart.ipvs.provenance.evaluation.dblp

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class DBLPScenario3(spark: SparkSession, testConfiguration: TestConfiguration) extends DBLPScenario (spark, testConfiguration) {
  override def getName: String = "D3"

  import spark.implicits._

  override def referenceScenario: DataFrame = {
    val inproceedings = loadInproceedings()
    val inproceedings_flattened = inproceedings.withColumn("cref", explode($"crossref")) // Schema Alternative: author
    val inproceedings_nineties = inproceedings_flattened.filter($"year" > 1990 && $"year" < 2001)
    val res = inproceedings_nineties.groupBy($"year").agg(countDistinct($"title._VALUE").alias("cnt"))
    res
  }

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val text = twig.createNode("year", 1, 1, "1992")
    val count = twig.createNode("cnt", 1, 1, "ltltltlt12474")
    twig = twig.createEdge(root, text, false)
    twig = twig.createEdge(root, count, false)
    twig.validate.get
  }
}
