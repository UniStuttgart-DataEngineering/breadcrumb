package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.functions.{count, explode, explode_outer}
import org.apache.spark.sql.{DataFrame, SparkSession}

class TPCHScenario213(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {


  import spark.implicits._

//  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//  val increase = udf { (x: Double, y: Double) => x * (1 + y) }
//  val special = udf { (x: String) => x.matches(".*special.*requests.*") }

/*
Original result:
+-------+--------+
|c_count|custdist|
+-------+--------+
|0      |50005   |
|9      |6641    |
|10     |6532    |
|11     |6014    |
|8      |5937    |
|12     |5639    |
|13     |5024    |
|19     |4793    |
|7      |4687    |
|17     |4587    |
+-------+--------+

Explanations:
+--------------+---------------+-----------+
|pickyOperators|compatibleCount|alternative|
+--------------+---------------+-----------+
|[0005]        |1              |000015     |
+--------------+---------------+-----------+
*/


  def unmodifiedNestedReferenceScenario: DataFrame = {
    var nestedCust = loadNestedCustomer() // TODO: non-existed null values are loaded
//    println(nestedCust.count())
//    println(nestedCust.filter($"c_custkey".isNotNull).count())
//    nestedCust = nestedCust.filter($"c_custkey".isNotNull)

    val flattenOrd = nestedCust.withColumn("order", explode_outer($"c_orders"))
    val ordComment = flattenOrd.filter(!$"order.o_comment".like("%special%requests%") || $"order.o_comment".isNull) // necessary to include customers with no records
    val countOrd = ordComment.groupBy($"c_custkey").agg(count($"order").as("c_count"))
    val res = countOrd.groupBy($"c_count").agg(count($"c_custkey").as("custdist"))
    res.sort($"custdist".desc, $"c_count".desc)

//    res.filter($"c_count" === 0)
//    flattenOrd.printSchema()
//    projectCols.filter($"c_custkey" === 474)
//    nestedCust.filter(size($"c_orders") === 0)
//    res
  }

  def nestedScenarioWithFlatten: DataFrame = {
    var nestedCust = loadNestedCustomer() // TODO: non-existed null values are loaded
//    nestedCust = nestedCust.filter($"c_custkey".isNotNull)

    val flattenOrd = nestedCust.withColumn("order", explode($"c_orders")) // explode -> explode_outer
    val ordComment = flattenOrd.filter(!$"order.o_comment".like("%special%requests%"))
//    val ordComment = flattenOrd.filter(!$"order.o_comment".contains("special") || !$"order.o_comment".contains("requests"))
//    val addId = flattenOrd.withColumn("id", $"order.o_comment".contains("special") && $"order.o_comment".contains("requests"))
//    val filter1 = addId.filter($"id" === false)
    val countOrd = ordComment.groupBy($"c_custkey").agg(count($"order").as("c_count"))
    val res = countOrd.groupBy($"c_count").agg(count($"c_custkey").as("custdist"))
//    res.sort($"custdist".desc, $"c_count".desc)
//    res.filter($"c_count" === 0)
    res
  }

  def nestedScenarioWithFlattenWithSmall: DataFrame = {
    val nestedCust = loadNestedCustomer001()

    val flattenOrd = nestedCust.withColumn("order", explode($"c_orders")) // explode -> explode_outer
    val ordComment = flattenOrd.filter(!$"order.o_comment".like("%special%requests%"))
    val countOrd = ordComment.groupBy($"c_custkey").agg(count($"order").as("c_count"))
    val res = countOrd.groupBy($"c_count").agg(count($"c_custkey").as("custdist"))
    //    res.sort($"custdist".desc, $"c_count".desc)
    //    res.filter($"c_count" === 0)
    res
  }

  override def referenceScenario: DataFrame = {
//    return unmodifiedNestedReferenceScenario
    return nestedScenarioWithFlatten
//    return nestedScenarioWithFlattenWithSmall
  }

  override def getName(): String = "TPCH213"

  override def whyNotQuestion: Twig =   {
    var twig = new Twig()
    val root = twig.createNode("root")
    val key = twig.createNode("c_count", 1, 1, "0")
//    val key = twig.createNode("c_custkey", 1, 1, "474")
    twig = twig.createEdge(root, key, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree =  {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
//    val saSize = testConfiguration.schemaAlternativeSize
//    createAlternatives(primaryTree, saSize)
//
//    for (i <- 0 until saSize) {
//      replaceDate(primaryTree.alternatives(i).rootNode)
//    }

    primaryTree
  }

//  def replaceDate(node: SchemaNode): Unit ={
//    if (node.name == "l_commitdate" && node.children.isEmpty) {
//      node.name = "l_shipdate"
//      node.modified = true
//      return
//    }
//    for (child <- node.children){
//      replaceDate(child)
//    }
//  }

}
