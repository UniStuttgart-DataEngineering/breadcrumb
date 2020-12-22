package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions.{count, explode, expr, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

class TPCHScenario05(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {


  import spark.implicits._

//  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//  val increase = udf { (x: Double, y: Double) => x * (1 + y) }

/*
Original result:
+---------+--------------------+
|n_name   |revenue             |
+---------+--------------------+
|JAPAN    |4.5410175695400015E7|
|CHINA    |5.372449425659998E7 |
|INDIA    |5.203551200020002E7 |
|VIETNAM  |5.5295086996700004E7|
|INDONESIA|5.550204116970004E7 |
+---------+--------------------+

Rewrite without SA:


Rewrite with SA + MSR:

*/

  override def referenceScenario: DataFrame = {
//    // Original query
//    val customer = loadCustomer()
//    val orders = loadOrder()
//    val lineitem = loadLineItem()
//    val supplier = loadSupplier()
//    val nation = loadNation()
//    val region = loadRegion()
//
//    val joinCustOrd = customer.join(orders, $"c_custkey" === $"o_custkey")
//    val joinOrdLine = joinCustOrd.join(lineitem, $"o_orderkey" === $"l_orderkey")
//    val joinSuppLine = joinOrdLine.join(supplier, $"l_suppkey" === $"s_suppkey" && $"c_nationkey" === $"s_nationkey")
//    val joinSuppNa = joinSuppLine.join(nation, $"s_nationkey" === $"n_nationkey")
//    val filterReg = region.filter($"r_name" === "ASIA")
//    val joinRegion = joinSuppNa.join(filterReg, $"n_regionkey" === $"r_regionkey")
//    val filterOrdDate = joinRegion.filter($"o_orderdate".between("1994-01-01", "1994-12-31"))
//    var res = filterOrdDate.groupBy($"n_name").agg(sum(expr("l_extendedprice * (1 - l_discount)")).alias("revenue"))
////        .sort($"n_name")


    // New query
//    val nestedOrders = loadNestedOrders()
    val nestedCustomer = loadCustomer() //TODO: create nestedCustomer using customer and nestedOrders
    val supplier = loadSupplier()
    val nation = loadNation()
    val region = loadRegion()

    val flattenOrders = nestedCustomer.withColumn("order", explode($"c_orders")) //TODO: no SA
    val filterOrderDate = flattenOrders.filter($"order.o_orderdate".between("1994-01-01", "1994-12-31"))
    val flattenLineitems = filterOrderDate.withColumn("lineitem", explode($"o_lineitems"))

    val joinCustSupp = flattenLineitems.join(supplier, $"c_nationkey" === $"s_nationkey")
    val joinSuppNa = joinCustSupp.join(nation, $"s_nationkey" === $"n_nationkey")

    val filterReg = region.filter($"r_name" === "ASIA")
    val joinNaReg = joinSuppNa.join(filterReg, $"n_regionkey" === $"r_regionkey")
    var res = joinNaReg.groupBy($"cust_nation.n_name").agg(sum(expr("l_extendedprice * (1 - l_discount)")).alias("revenue"))

    res
  }

  override def getName(): String = "TPCH05"

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val priority = twig.createNode("n_name", 1, 1, "VIETNAM")
    val count = twig.createNode("revenue", 1, 1, "") //TODO: add condition
    twig = twig.createEdge(root, priority, false)
    twig = twig.createEdge(root, count, false)
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

  //TODO: update the alternative
  def replaceDate(node: SchemaNode): Unit ={
    if (node.name == "l_shipdate" && node.children.isEmpty) {
      node.name = "l_commitdate"
      node.modified = true
      return
    }
    for (child <- node.children){
      replaceDate(child)
    }
  }
}
