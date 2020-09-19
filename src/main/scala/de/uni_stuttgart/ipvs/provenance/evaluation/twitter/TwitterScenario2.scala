package de.uni_stuttgart.ipvs.provenance.evaluation.twitter

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class TwitterScenario2(spark: SparkSession, testConfiguration: TestConfiguration) extends TwitterScenario (spark, testConfiguration) {
  override def getName(): String = "T2"

  import spark.implicits._

  override def referenceScenario(): DataFrame = {
    val tw = loadTweets()
//    val tw_select = tw.select($"user.name".alias("name"), $"user.location".alias("loc"), $"text",
//      size($"entities.hashtags").alias("sizeOfHashtags"), $"place.country".alias("pcountry")) // SA: place.country -> user.location
//    var tw_select = tw.withColumn("sizeOfHashtags", size($"entities.hashtags"))
//    tw_select = tw_select.withColumn("uLocation", $"user.location")
//    tw_select = tw_select.withColumn("uName", $"user.name")
    val tw_select = tw.select(size($"entities.hashtags").alias("sizeOfHashtags"), $"user.location".alias("uLocation"), //$"user.created_at",
        $"user.name".alias("uName"), $"text", $"place.country".alias("country"), $"user.lang".alias("lang"),
        $"user.followers_count".alias("cntFollowers"), $"created_at".alias("time")) // SA: place.country -> user.location
//    val year = tw_select.withColumn("createdYear", $"created_at".substr(length($"created_at")-4, length($"created_at")))
    val tw_bts = tw_select.filter($"text".contains("BTS"))
    var res = tw_bts.filter($"country".contains("United States"))
    res = res.groupBy($"time", $"uLocation", $"lang", $"sizeOfHashtags", $"cntFollowers").agg(collect_list($"uName").alias("listOfNames"))
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

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    val saSize = testConfiguration.schemaAlternativeSize
    createAlternatives(primaryTree, saSize)

    for (i <- 0 until saSize by 2) {
      replace1(primaryTree.alternatives(i).rootNode)
    }

    primaryTree
  }

  def replace1(node: SchemaNode): Unit ={
    if (node.name == "place" && node.parent.name == "root") {
      node.name = "user"
      node.modified = true
      val child = node.getChild("country").get
      child.name = "location"
      child.modified = true
      return
    }
    for (child <- node.children){
      replace1(child)
    }
  }

}
