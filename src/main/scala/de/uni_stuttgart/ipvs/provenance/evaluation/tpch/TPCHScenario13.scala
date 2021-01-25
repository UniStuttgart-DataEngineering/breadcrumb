package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.functions.{sum, udf, count}
import org.apache.spark.sql.{DataFrame, SparkSession}

class TPCHScenario13(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {


  import spark.implicits._

//  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//  val increase = udf { (x: Double, y: Double) => x * (1 + y) }
  val special = udf { (x: String) => x.matches(".*special.*requests.*") }

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
|[0003]        |1              |000017     |
+--------------+---------------+-----------+
*/


  def unmodifiedReferenceScenario: DataFrame = {
    val customer = loadCustomer()
    val order = loadOrder()

    val ordComment = order.filter(!$"o_comment".like("%special%requests%"))
//    val ordComment = order.filter(!$"o_comment".contains("special") || !$"o_comment".contains("requests"))
    val joinCustOrd = customer.join(ordComment, $"c_custkey" === $"o_custkey", "left_outer")
    val countOrd = joinCustOrd.groupBy($"c_custkey").agg(count($"o_orderkey").as("c_count"))
    val res = countOrd.groupBy($"c_count").agg(count($"c_custkey").as("custdist"))
//    res.sort($"custdist".desc, $"c_count".desc)
//    joinCustOrd.filter($"o_orderkey".isNull)
//    res.filter($"c_count" === 9)
    res
//    joinCustOrd.filter($"c_custkey" === 474)
  }

  def test: DataFrame = {
    val customer = loadCustomer()
    val order = loadOrder()

    var res = customer.join(order, $"c_custkey" === order("o_custkey")
      && !special(order("o_comment")), "left_outer")
    res = res.filter($"c_custkey" === 474)
//      .groupBy($"o_custkey")
//      .agg(count($"o_orderkey").as("c_count"))
//      .filter($"o_custkey" === 474)
//      .groupBy($"c_count")
//      .agg(count($"o_custkey").as("custdist"))
//      .sort($"custdist".desc, $"c_count".desc)
    res
  }

  def flatScenarioModified: DataFrame = {
    val customer = loadCustomer()
    val order = loadOrder()

    val ordComment = order.filter(!$"o_comment".like("%special%requests%")) // oid=5
    val joinCustOrd = customer.join(ordComment, $"c_custkey" === $"o_custkey") // LEFT_OUTER_JOIN -> NATURAL_JOIN
    val countOrd = joinCustOrd.groupBy($"c_custkey").agg(count($"o_orderkey").as("c_count"))
    val res = countOrd.groupBy($"c_count").agg(count($"c_custkey").as("custdist"))
//    res.sort($"custdist".desc, $"c_count".desc)
//    res.filter($"c_count" === 9)
//    countOrd
    res
  }

  def flatScenarioModifiedWithSmall: DataFrame = {
    val customer = loadCustomer()
    val order = loadOrder001()

    val ordComment = order.filter(!$"o_comment".like("%special%requests%")) // oid=5
    val joinCustOrd = customer.join(ordComment, $"c_custkey" === $"o_custkey") // LEFT_OUTER_JOIN -> NATURAL_JOIN // oid = 3
    val countOrd = joinCustOrd.groupBy($"c_custkey").agg(count($"o_orderkey").as("c_count"))
    val res = countOrd.groupBy($"c_count").agg(count($"c_custkey").as("custdist"))
    //    res.sort($"custdist".desc, $"c_count".desc)
    //    res.filter($"c_count" === 9)
    //    countOrd
    res
  }

  override def referenceScenario: DataFrame = {
//    return unmodifiedReferenceScenario
//    return test
    return flatScenarioModified
//    return flatScenarioModifiedWithSmall
  }

  override def getName(): String = "TPCH13"

  override def whyNotQuestion: Twig =   {
    var twig = new Twig()
    val root = twig.createNode("root")
//    val key = twig.createNode("c_count", 1, 1, "9")
//    val value = twig.createNode("custdist", 1, 1, "gtgtgtgt6600")
//    val key = twig.createNode("c_custkey", 1, 1, "474")
    val key = twig.createNode("c_count", 1, 1, "0")
    twig = twig.createEdge(root, key, false)
//    twig = twig.createEdge(root, value, false)
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
