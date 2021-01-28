package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class TPCHScenario110(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {


  import spark.implicits._

//  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//  val increase = udf { (x: Double, y: Double) => x * (1 + y) }

/*
Original result:
+---------+------------------+---------+---------------+------+-------------+--------------------------------------------------------------------+---------+
|c_custkey|c_name            |c_acctbal|c_phone        |n_name|c_address    |c_comment                                                           |revenue  |
+---------+------------------+---------+---------------+------+-------------+--------------------------------------------------------------------+---------+
|61402    |Customer#000061402|8285.02  |22-693-385-6828|JAPAN |axsukwH3j7bjp|usly alongside of the final accounts. carefully even packages boost;|5781.8499|
+---------+------------------+---------+---------------+------+-------------+--------------------------------------------------------------------+---------+

Result over mq:
+---------+------------------+---------+---------------+------+-------------+--------------------------------------------------------------------+-----------+
|c_custkey|c_name            |c_acctbal|c_phone        |n_name|c_address    |c_comment                                                           |revenue    |
+---------+------------------+---------+---------------+------+-------------+--------------------------------------------------------------------+-----------+
|61402    |Customer#000061402|8285.02  |22-693-385-6828|JAPAN |axsukwH3j7bjp|usly alongside of the final accounts. carefully even packages boost;|560945.1556|
+---------+------------------+---------+---------------+------+-------------+--------------------------------------------------------------------+-----------+

Explanations over sample:
+------------------+---------------+-----------+
|pickyOperators    |compatibleCount|alternative|
+------------------+---------------+-----------+
|[0002]            |1              |000034     |
|[0002, 0006]      |1              |000034     |
|[0002, 0006, 0007]|1              |000034     |
+------------------+---------------+-----------+
0034: l_discount
*/

  def unmodifiedNestedReferenceScenario: DataFrame = {
    val customer = loadCustomer()
    val nestedOrder = loadNestedOrders()
    val nation = loadNation()

    val flattenLineItem = nestedOrder.withColumn("lineitem", explode($"o_lineitems"))
    val filterFlag = flattenLineItem.filter($"lineitem.l_returnflag" === "R")
    val orderDate = filterFlag.filter($"o_orderdate".between("1993-10-01", "1993-12-31"))
    val joinCustomer = customer.join(orderDate, $"c_custkey" === $"o_custkey")
    val joinNation = joinCustomer.join(nation, $"c_nationkey" === $"n_nationkey")
//    val projectExpr = joinNation.select($"c_custkey", $"c_name", $"c_acctbal", $"c_address", $"c_phone", $"c_comment",
//      $"n_name", $"o_orderdate", ($"lineitem.l_extendedprice" * (lit(1.0) - $"lineitem.l_discount")).alias("disc_price"))
    val projectExpr = joinNation.withColumn("disc_price", ($"lineitem.l_extendedprice" * (lit(1.0) - $"lineitem.l_discount")))
    var res = projectExpr.groupBy($"c_custkey",  $"c_name", $"c_acctbal", $"c_phone", $"n_name", $"c_address", $"c_comment")
      .agg(sum($"disc_price").alias("revenue"))
    res.filter($"c_custkey" === 61402)
//    res
  }

  def nestedScenarioWithLinestatusToReturnflagWithSmall: DataFrame = {
    val customer = loadCustomer()
    val nestedOrder = loadNestedOrders001()
    val nation = loadNation()

    val flattenLineItem = nestedOrder.withColumn("lineitem", explode($"o_lineitems"))
    val filterFlag = flattenLineItem.filter($"lineitem.l_returnflag" === "N") // oid = 7
    val orderDate = filterFlag.filter($"o_orderdate".between("1997-10-01", "1997-12-31")) // oid = 6
    val joinCustomer = customer.join(orderDate, $"c_custkey" === $"o_custkey")
    val joinNation = joinCustomer.join(nation, $"c_nationkey" === $"n_nationkey")
//    val projectExpr = joinNation.select($"c_custkey", $"c_name", $"c_acctbal", $"c_address", $"c_phone", $"c_comment",
//      $"n_name", $"o_orderdate", ($"lineitem.l_extendedprice" * (lit(1.0) - $"lineitem.l_tax")).alias("disc_price")) // SA: l_tax -> l_discount
    val projectExpr = joinNation.withColumn("disc_price", ($"lineitem.l_extendedprice" * (lit(1.0) - $"lineitem.l_tax"))) // SA: l_tax -> l_discount
    var res = projectExpr.groupBy($"c_custkey",  $"c_name", $"c_acctbal", $"c_phone", $"n_name", $"c_address", $"c_comment")
      .agg(sum($"disc_price").alias("revenue"))
//    res.filter($"c_custkey" === 61402)
    res
  }

  def nestedScenarioWithLinestatusToReturnflag: DataFrame = {
    val customer = loadCustomer()
    val nestedOrder = loadNestedOrders()
    val nation = loadNation()

    val flattenLineItem = nestedOrder.withColumn("lineitem", explode($"o_lineitems"))
    val filterFlag = flattenLineItem.filter($"lineitem.l_returnflag" === "N") // oid = 7
    val orderDate = filterFlag.filter($"o_orderdate".between("1997-10-01", "1997-12-31")) // oid = 6
    val joinCustomer = customer.join(orderDate, $"c_custkey" === $"o_custkey")
    val joinNation = joinCustomer.join(nation, $"c_nationkey" === $"n_nationkey")
//    val projectExpr = joinNation.select($"c_custkey", $"c_name", $"c_acctbal", $"c_address", $"c_phone", $"c_comment",
//      $"n_name", $"o_orderdate", ($"lineitem.l_extendedprice" * (lit(1.0) - $"lineitem.l_tax")).alias("disc_price")) // SA: l_tax -> l_discount
    val projectExpr = joinNation.withColumn("disc_price", ($"lineitem.l_extendedprice" * (lit(1.0) - $"lineitem.l_tax"))) // SA: l_tax -> l_discount
    var res = projectExpr.groupBy($"c_custkey",  $"c_name", $"c_acctbal", $"c_phone", $"n_name", $"c_address", $"c_comment")
      .agg(sum($"disc_price").alias("revenue"))
//    res.filter($"c_custkey" === 61402)
    res
  }

  override def referenceScenario: DataFrame = {
//    return unmodifiedNestedReferenceScenario
    return nestedScenarioWithLinestatusToReturnflagWithSmall
//    return nestedScenarioWithLinestatusToReturnflag
  }

  override def getName(): String = "TPCH110"

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    //    val custkey = twig.createNode("c_custkey", 1, 1, "57040") // outdated
    //    val revenue = twig.createNode("revenue", 1, 1, "ltltltlt740000") // outdated
    val custkey = twig.createNode("c_custkey", 1, 1, "61402") // for both full and sample data
    val revenue = twig.createNode("revenue", 1, 1, "ltltltlt560000")
    twig = twig.createEdge(root, custkey, false)
    twig = twig.createEdge(root, revenue, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    val nesteOrder = input.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].location.rootPaths.head.toUri.toString.contains("nestedOrders")

    if(nesteOrder) {
      //    val saSize = testConfiguration.schemaAlternativeSize
      val saSize = 1
      createAlternatives(primaryTree, saSize)

      for (i <- 0 until saSize) {
        replaceDate(primaryTree.alternatives(i).rootNode)
      }
    }

    primaryTree
  }

  def replaceDate(node: SchemaNode): Unit ={
    if (node.name == "l_tax" && node.children.isEmpty) {
      node.name = "l_discount"
      node.modified = true
      return
    }
    for (child <- node.children){
      replaceDate(child)
    }
  }

}
