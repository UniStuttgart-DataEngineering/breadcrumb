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

    val www_flattened = www.withColumn("u_author", explode($"author"))
    val www_selected = www_flattened.select($"_key".alias("wkey"), $"u_author".alias("wauthor"), $"u_author._VALUE".alias("wname"))
    val inproceedings_flattened = inproceedings.withColumn("iauthor", explode($"author"))
    val inproceedings_selected = inproceedings_flattened.select($"iauthor._VALUE".alias("iname"), $"_key", $"booktitle", $"title", $"iauthor", $"author")
    var joined = www_selected.join(inproceedings_selected, $"wname" === $"iname")
    joined = joined.withColumn("co_author", explode($"author"))
//    val joined2 = joined.filter($"booktitle".isNull)
//    val res1 = joined.groupBy($"wauthor").agg(count($"_key").alias("cnt"))
//    val res2 = joined2.groupBy($"wauthor").agg(count($"_key").alias("cntNull"))
//    var res = res1.join(res2, Seq("wauthor"))
//    res = res.filter($"cntNull" === $"cnt" && $"cnt" > 10)
    val res = joined.groupBy($"wauthor").agg(collect_list($"co_author").alias("co_authors"), collect_list($"booktitle").alias("venue"), collect_list($"title").alias("titles"))
    res
  }

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val text = twig.createNode("wauthor", 1, 1, "containsNicolas Nagel")
    val venue = twig.createNode("venue", 1, 1, "")
    val element = twig.createNode("element", 1, 1, "isscc")
    twig = twig.createEdge(root, text, false)
    twig = twig.createEdge(root, venue, false)
    twig = twig.createEdge(venue, element, false)
    twig.validate.get
  }
}
