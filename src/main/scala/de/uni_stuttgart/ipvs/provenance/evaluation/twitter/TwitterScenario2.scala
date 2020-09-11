package de.uni_stuttgart.ipvs.provenance.evaluation.twitter

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class TwitterScenario2(spark: SparkSession, testConfiguration: TestConfiguration) extends TwitterScenario (spark, testConfiguration) {
  override def getName(): String = "T2"

  import spark.implicits._

  override def referenceScenario(): DataFrame = {
    val tw = loadTweets()
    val tw_select = tw.select($"user.name".alias("name"), $"user.location".alias("loc"), $"text", $"place.country".alias("country"))
    val tw_bts = tw_select.filter($"text".contains("BTS"))
    var res = tw_bts.filter($"country".contains("United States")) // SA: loc
    res = res.groupBy($"loc").agg(collect_list($"name").alias("listOfNames"))
    res
  }

  override def whyNotQuestion(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
//    val country = twig.createNode("loc", 1, 1, "United States")
    val list = twig.createNode("listOfNames", 1, 1, "")
    val element = twig.createNode("element", 1, 1, "containsCindy")
//    twig = twig.createEdge(root, country, false)
    twig = twig.createEdge(root, list, false)
    twig = twig.createEdge(list, element, false)
    twig.validate.get
  }
}
