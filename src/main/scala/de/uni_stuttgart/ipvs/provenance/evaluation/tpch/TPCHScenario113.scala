package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.functions.{count, explode, explode_outer, size}
import org.apache.spark.sql.{DataFrame, SparkSession}

class TPCHScenario113(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {


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

over sample:

Explanations:
+--------------+---------------+-----------+
|pickyOperators|compatibleCount|alternative|
+--------------+---------------+-----------+
|[0003]        |1              |000017     |
+--------------+---------------+-----------+
*/


  def unmodifiedNestedReferenceScenario: DataFrame = {
    val customer = loadCustomer()
    val nestedOrder = loadNestedOrders()

    val ordComment = nestedOrder.filter(!$"o_comment".like("%special%requests%"))
    val joinCustOrd = customer.join(ordComment, $"c_custkey" === $"o_custkey", "left_outer")
    val countOrd = joinCustOrd.groupBy($"c_custkey").agg(count($"o_orderkey").as("c_count"))
    val res = countOrd.groupBy($"c_count").agg(count($"c_custkey").as("custdist"))
//    res.sort($"custdist".desc, $"c_count".desc)
//    res.filter($"c_count" === 0)
    res
//    joinCustOrd.filter($"c_custkey" === 474)
  }

  def nestedScenarioWithOuterJoinToJoin: DataFrame = {
    val customer = loadCustomer()
    val nestedOrder = loadNestedOrders()

    val ordComment = nestedOrder.filter(!$"o_comment".like("%special%requests%"))
    val joinCustOrd = customer.join(ordComment, $"c_custkey" === $"o_custkey")  // LEFT_OUTER_JOIN -> NATURAL_JOIN
    val countOrd = joinCustOrd.groupBy($"c_custkey").agg(count($"o_orderkey").as("c_count"))
    val res = countOrd.groupBy($"c_count").agg(count($"c_custkey").as("custdist"))
//    res.sort($"custdist".desc, $"c_count".desc)
//    res.filter($"c_count" === 0)
    res
  }

  def nestedScenarioWithOuterJoinToJoinWithSmall: DataFrame = {
    val customer = loadCustomer()
    val nestedOrder = loadNestedOrders001()

    val ordComment = nestedOrder.filter(!$"o_comment".like("%special%requests%"))
    val joinCustOrd = customer.join(ordComment, $"c_custkey" === $"o_custkey")  // LEFT_OUTER_JOIN -> NATURAL_JOIN
    val countOrd = joinCustOrd.groupBy($"c_custkey").agg(count($"o_orderkey").as("c_count"))
    val res = countOrd.groupBy($"c_count").agg(count($"c_custkey").as("custdist"))
    //    res.sort($"custdist".desc, $"c_count".desc)
    //    res.filter($"c_count" === 0)
    res
  }

  override def referenceScenario: DataFrame = {
//    return unmodifiedNestedReferenceScenario
    return nestedScenarioWithOuterJoinToJoin
//    return nestedScenarioWithOuterJoinToJoinWithSmall
  }

  override def getName(): String = "TPCH113"

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
