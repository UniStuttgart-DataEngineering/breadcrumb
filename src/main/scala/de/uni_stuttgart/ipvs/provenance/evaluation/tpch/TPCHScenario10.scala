package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class TPCHScenario10(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {


  import spark.implicits._

//  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//  val increase = udf { (x: Double, y: Double) => x * (1 + y) }

/*
Original result:
+---------+------------------+---------+---------------+------+----------+----------------------------------------------------------------+-----------------+
|c_custkey|c_name            |c_acctbal|c_phone        |n_name|c_address |c_comment                                                       |revenue          |
+---------+------------------+---------+---------------+------+----------+----------------------------------------------------------------+-----------------+
|57040    |Customer#000057040|632.87   |22-895-641-3466|JAPAN |Eioyzjf4pp|sits. slyly regular requests sleep alongside of the regular inst|734235.2455000001|
+---------+------------------+---------+---------------+------+----------+----------------------------------------------------------------+-----------------+

Rewrite without SA:

Rewrite with SA + MSR:
*/

  override def referenceScenario: DataFrame = {
    val lineitem = loadLineItem()
    val orders = loadOrder()
    val customer = loadCustomer()
    val nation = loadNation()
    val nestedCustomer = loadCustomer() //TODO: create nestedCustomer using nestedOrders and nation that are nested into customer table

//    // Original query
//    val filterLine = lineitem.filter($"l_returnflag" === "R")
//    val filterOrd = orders.filter($"o_orderdate".between("1993-10-01", "1993-12-31"))
//    val joinCustOrd = customer.join(filterOrd, $"c_custkey" === $"o_custkey")
//    val joinOrdLine = joinCustOrd.join(filterLine, $"o_orderkey" === $"l_orderkey")
//    val joinNation = joinOrdLine.join(nation, $"c_nationkey" === $"n_nationkey")
//    var res = joinNation.groupBy($"c_custkey",  $"c_name", $"c_acctbal", $"c_phone", $"n_name", $"c_address", $"c_comment")
//          .agg(sum(expr("l_extendedprice * (1 - l_discount)")).alias("revenue"))

    // New query
    // TODO: find another SA (can we have flatten in the explanation?)
    val flattenOrd = nestedCustomer.withColumn("order", explode($"c_orders"))
    val flattenLine = flattenOrd.withColumn("lineitem", explode($"o_lineitem"))
    val flattenCustNa = flattenLine.withColumn("cust_nation", explode($"c_nation"))
    val projectCust = flattenCustNa.select($"c_custkey", $"c_name", $"lineitem.l_extendedprice".alias("l_extendedprice"),
        $"lineitem.l_discount".alias("l_discount"), $"c_acctbal", $"cust_nation.n_name".alias("cust_nationname"),
        $"c_address", $"c_phone", $"c_comment", $"order.o_orderdate".alias("o_orderdate"),
        $"lineitem.l_linestatus".alias("l_returnflag")) // SA: l_linestatus -> l_returnflag
    val filterOrd = projectCust.filter($"o_orderdate".between("1993-10-01", "1993-12-31"))
    val filterFlag = filterOrd.filter($"l_returnflag" === "R")
    var res = filterFlag.groupBy($"c_custkey",  $"c_name", $"c_acctbal", $"c_phone", $"n_name", $"c_address", $"c_comment")
        .agg(sum(expr("l_extendedprice * (1 - l_discount)")).alias("revenue"))

    res
  }

  override def getName(): String = "TPCH10"

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val custkey = twig.createNode("c_custkey", 1, 1, "57040")
    val nation = twig.createNode("n_name", 1, 1, "JAPAN")
    val revenue = twig.createNode("revenue", 1, 1, "") //TODO: add condition
    twig = twig.createEdge(root, custkey, false)
    twig = twig.createEdge(root, nation, false)
    twig = twig.createEdge(root, revenue, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    val nestedOrders = input.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].location.rootPaths.head.toUri.toString.contains("nestedorders")
    if (!nestedOrders) {
      val saSize = testConfiguration.schemaAlternativeSize
      createAlternatives(primaryTree, saSize)

      for (i <- 0 until saSize by 2) {
        replaceDate(primaryTree.alternatives(i).rootNode)
      }
    }
    primaryTree
  }

  def replaceDate(node: SchemaNode): Unit ={
    if (node.name == "l_linestatus" && node.children.isEmpty) {
      node.name = "l_returnflag"
      node.modified = true
      return
    }
    for (child <- node.children){
      replaceDate(child)
    }
  }

}
