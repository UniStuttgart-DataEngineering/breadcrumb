package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions.{count, explode, lit, sum}
import org.apache.spark.sql.{DataFrame, SparkSession}

class TPCHScenario103(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {


  import spark.implicits._

//  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//  val increase = udf { (x: Double, y: Double) => x * (1 + y) }

/*
Original result for a specific orderkey:
+----------+-----------+--------------+-----------------+
|l_orderkey|o_orderdate|o_shippriority|revenue          |
+----------+-----------+--------------+-----------------+
|4986467   |1994-12-21 |0             |7309.665599999999|
+----------+-----------+--------------+-----------------+

Explanations (over sample):
+------------------+---------------+-----------+
|pickyOperators    |compatibleCount|alternative|
+------------------+---------------+-----------+
|[0006]            |1              |000037     |
|[0002, 0006]      |1              |000042     |
|[0002, 0006, 0007]|1              |000046     |
|[0006]            |1              |000038     |
|[0002, 0006]      |1              |000041     |
|[0006, 0007]      |1              |000045     |
|[0006, 0007]      |1              |000044     |
|[0006]            |1              |000039     |
|[0002, 0006, 0007]|1              |000048     |
|[0006, 0007]      |1              |000043     |
+------------------+---------------+-----------+

0007: Flatten
TODO: this should not be part of the explanation since there are no empty lists of o_lineitems for o_orderkey === 4986467
*/


  def unmodifiedNestedReferenceScenario: DataFrame = {
    val customer = loadCustomer()
    val nestedOrders = loadNestedOrders()

    val filterMktSeg = customer.filter($"c_mktsegment" === "BUILDING")
    val filterOrdDate = nestedOrders.filter($"o_orderdate" < "1995-03-15")
    val flattenLineItem = filterOrdDate.withColumn("lineitem", explode($"o_lineitems"))
    val filterShipDate = flattenLineItem.filter($"lineitem.l_shipdate" > "1995-03-15")
    val joinCustOrd = filterMktSeg.join(filterShipDate, $"c_custkey" === $"o_custkey")
//    val projectExpr = joinCustOrd.withColumn("disc_price", ($"lineitem.l_extendedprice" * (lit(1.0) - $"lineitem.l_discount")))
    val projectExpr = joinCustOrd.select($"lineitem.l_orderkey".alias("l_orderkey"), $"o_orderdate", $"o_shippriority",
      ($"lineitem.l_extendedprice" * (lit(1.0) - $"lineitem.l_discount")).alias("disc_price"))
    val res = projectExpr.groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority").agg(sum($"disc_price").alias("revenue"))

//    val filterMktSeg = customer.filter($"c_mktsegment" === "BUILDING")
//    val flattenLineItem = nestedOrders.withColumn("lineitem", explode($"o_lineitems"))
//    val projectExpr = flattenLineItem.select($"lineitem.l_orderkey".alias("l_orderkey"), $"o_orderdate", $"o_shippriority", $"o_custkey",
//      $"lineitem.l_commitdate".alias("l_commitdate"), ($"lineitem.l_extendedprice" * (lit(1.0) - $"lineitem.l_discount")).alias("disc_price"))
//    val filterOrdDate = projectExpr.filter($"o_orderdate" < "1995-03-15")
//    val filterShipDate = filterOrdDate.filter($"lineitem.l_shipdate" > "1995-03-15")
//    val joinCustOrd = filterMktSeg.join(filterShipDate, $"c_custkey" === $"o_custkey")
//    val res = joinCustOrd.groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority").agg(sum($"disc_price").alias("revenue"))

    res.filter($"l_orderkey" === 4986467 || $"l_orderkey" == 1468993)
//    res
  }

  def nestedScenarioWithCommitToShipDate: DataFrame = {
    val customer = loadCustomer()
    val nestedOrders = loadNestedOrders()

    val filterMktSeg = customer.filter($"c_mktsegment" === "BUILDING")
    val filterOrdDate = nestedOrders.filter($"o_orderdate" < "1995-03-15")
    val flattenLineItem = filterOrdDate.withColumn("lineitem", explode($"o_lineitems"))
    val filterShipDate = flattenLineItem.filter($"lineitem.l_commitdate" > "1995-03-15") // SA: l_commitdate -> l_shipdate
    val joinCustOrd = filterMktSeg.join(filterShipDate, $"c_custkey" === $"o_custkey")
//    val projectExpr = joinCustOrd.withColumn("disc_price", ($"lineitem.l_extendedprice" * (lit(1.0) - $"lineitem.l_discount")))
    val projectExpr = joinCustOrd.select($"lineitem.l_orderkey".alias("l_orderkey"), $"o_orderdate", $"o_shippriority",
      ($"lineitem.l_extendedprice" * (lit(1.0) - $"lineitem.l_discount")).alias("disc_price"))
    val res = projectExpr.groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority").agg(sum($"disc_price").alias("revenue"))
//    res.filter($"l_orderkey" === 4986467 || $"l_orderkey" == 1468993)
    res
  }

  def nestedScenarioWithCommitToShipDateWithSmall: DataFrame = {
    val customer = loadCustomer()
    val nestedOrders = loadNestedOrders001()

    val filterMktSeg = customer.filter($"c_mktsegment" === "BUILDING")
    val filterOrdDate = nestedOrders.filter($"o_orderdate" < "1995-03-15") // oid = 9
    val flattenLineItem = filterOrdDate.withColumn("lineitem", explode($"o_lineitems")) // oid = 7
    val filterShipDate = flattenLineItem.filter($"lineitem.l_commitdate" > "1995-03-15") // SA: l_commitdate -> l_shipdate // oid = 6
    val joinCustOrd = filterMktSeg.join(filterShipDate, $"c_custkey" === $"o_custkey")
    //    val projectExpr = joinCustOrd.withColumn("disc_price", ($"lineitem.l_extendedprice" * (lit(1.0) - $"lineitem.l_discount")))
    val projectExpr = joinCustOrd.select($"lineitem.l_orderkey".alias("l_orderkey"), $"o_orderdate", $"o_shippriority",
      ($"lineitem.l_extendedprice" * (lit(1.0) - $"lineitem.l_discount")).alias("disc_price"))
    val res = projectExpr.groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority").agg(sum($"disc_price").alias("revenue"))

//    val filterMktSeg = customer.filter($"c_mktsegment" === "BUILDING")
//    val flattenLineItem = nestedOrders.withColumn("lineitem", explode($"o_lineitems"))
//    val projectExpr = flattenLineItem.select($"lineitem.l_orderkey".alias("l_orderkey"), $"o_orderdate", $"o_shippriority", $"o_custkey",
//      $"lineitem.l_commitdate".alias("l_commitdate"), ($"lineitem.l_extendedprice" * (lit(1.0) - $"lineitem.l_discount")).alias("disc_price"))
//    val filterOrdDate = projectExpr.filter($"o_orderdate" < "1995-03-15")
//    val filterShipDate = filterOrdDate.filter($"lineitem.l_commitdate" > "1995-03-15") // SA: l_commitdate -> l_shipdate
//    val joinCustOrd = filterMktSeg.join(filterShipDate, $"c_custkey" === $"o_custkey")
//    val res = joinCustOrd.groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority").agg(sum($"disc_price").alias("revenue"))

//    res.filter($"l_orderkey" === 4986467 || $"l_orderkey" == 1468993)
    res
  }

  def orderKey: DataFrame = {
    val customer = loadCustomer()
    val nestedOrder = loadNestedOrders()

    //    val filterMktSeg = customer.filter($"c_mktsegment" === "BUILDING")
    //    val filterOrdDate = orders.filter($"o_orderdate" < "1995-03-15")
    //    val filterShipDate = lineitem.filter($"l_commitdate" > "1995-03-15")
//    val joinCustOrd = customer.join(orders, $"c_custkey" === $"o_custkey")
    val res = nestedOrder.filter($"o_orderkey" === 4986467)
    res
  }

  override def referenceScenario: DataFrame = {
//    return unmodifiedNestedReferenceScenario
    return nestedScenarioWithCommitToShipDate
//    return nestedScenarioWithCommitToShipDateWithSmall
//    return orderKey
  }

  override def getName(): String = "TPCH103"

  override def whyNotQuestion: Twig =   {
    var twig = new Twig()
    val root = twig.createNode("root")
    val key = twig.createNode("l_orderkey", 1, 1, "4986467")
//    val rev = twig.createNode("revenue", 1, 1, "ltltltlt200000")
    twig = twig.createEdge(root, key, false)
//    twig = twig.createEdge(root, rev, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree =  {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    val nestedOrder = input.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].location.rootPaths.head.toUri.toString.contains("nestedOrders")

    if(nestedOrder) {
      getNestedAlternatives1(primaryTree, Seq("o_shippriority", "o_orderpriority"), Seq("l_discount", "l_tax"), Seq("l_commitdate", "l_shipdate", "l_receiptdate"))
      //NestedOrdersAlternatives.createAlternativesWithOrdersWith1Permutations(primaryTree,
      //  Seq("o_shippriority", "o_orderpriority"), Seq("l_discount", "l_tax"), Seq("l_commitdate", "l_shipdate", "l_receiptdate"))

//      val saSize = testConfiguration.schemaAlternativeSize
//      createAlternatives(primaryTree, saSize)
//
//      for (i <- 0 until saSize) {
//        replaceDate(primaryTree.alternatives(i).rootNode)
//      }
    }

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
