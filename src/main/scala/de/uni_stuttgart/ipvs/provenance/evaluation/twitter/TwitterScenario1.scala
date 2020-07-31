package de.uni_stuttgart.ipvs.provenance.evaluation.twitter

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.evaluation.dblp.DBLPScenario
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.{DataFrame, SparkSession}

class TwitterScenario1 (spark: SparkSession, testConfiguration: TestConfiguration) extends TwitterScenario (spark, testConfiguration) {
  override def getName(): String = "T1"

  import spark.implicits._

  override def referenceScenario(): DataFrame = {
    val df = loadTweets()
    df.filter($"text".contains("BTS"))
  }

  override def whyNotQuestion(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val text = twig.createNode("text")
    twig = twig.createEdge(root, text, false)
    twig.validate.get
  }
}
