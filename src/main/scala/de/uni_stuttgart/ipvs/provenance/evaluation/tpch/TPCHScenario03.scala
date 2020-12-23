package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions.{count, explode, sum, udf, expr}
import org.apache.spark.sql.{DataFrame, SparkSession}

class TPCHScenario03(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {


  import spark.implicits._

//  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//  val increase = udf { (x: Double, y: Double) => x * (1 + y) }

/*
Original result limited 1:
+----------+-----------+--------------+----------+
|l_orderkey|o_orderdate|o_shippriority|revenue   |
+----------+-----------+--------------+----------+
|1821349   |1995-02-19 |0             |67014.5082|
+----------+-----------+--------------+----------+

Rewrite without SA:
java.lang.NullPointerException
	at de.uni_stuttgart.ipvs.provenance.nested_why_not.ProvenanceContext$.associateIds(ProvenanceContext.scala:19)
	at de.uni_stuttgart.ipvs.provenance.nested_why_not.ProvenanceContext$.mergeContext(ProvenanceContext.scala:42)
	at de.uni_stuttgart.ipvs.provenance.transformations.JoinRewrite.rewrite(JoinRewrite.scala:176)
	at de.uni_stuttgart.ipvs.provenance.transformations.ProjectRewrite.rewrite(ProjectRewrite.scala:71)
	at de.uni_stuttgart.ipvs.provenance.transformations.AggregateRewrite.rewrite(AggregateRewrite.scala:34)
	at de.uni_stuttgart.ipvs.provenance.nested_why_not.WhyNotProvenance$.internalRewrite(WhyNotProvenance.scala:40)
	at de.uni_stuttgart.ipvs.provenance.nested_why_not.WhyNotProvenance$$anonfun$dataFrameAndProvenanceContext$default$3$1.apply(WhyNotProvenance.scala:48)
	at de.uni_stuttgart.ipvs.provenance.nested_why_not.WhyNotProvenance$$anonfun$dataFrameAndProvenanceContext$default$3$1.apply(WhyNotProvenance.scala:48)
	at de.uni_stuttgart.ipvs.provenance.nested_why_not.WhyNotProvenance$.dataFrameAndProvenanceContext(WhyNotProvenance.scala:50)
	at de.uni_stuttgart.ipvs.provenance.nested_why_not.WhyNotProvenance$.rewrite(WhyNotProvenance.scala:68)

Rewrite with SA + MSR:
ERROR: org.apache.spark.sql.catalyst.expressions.Multiply cannot be cast to org.apache.spark.sql.catalyst.expressions.NamedExpression
*/

  override def referenceScenario: DataFrame = {
    val customer = loadCustomer()
    //    val res = customer.select($"c_mktsegment").distinct()
    val nestedOrders = loadNestedOrders()
    val orders = loadOrder()
    val lineitem = loadLineItem()

//    // Original query
//    val filterMktSeg = customer.filter($"c_mktsegment" === "HOUSEHOLD")
//    val filterOrdDate = orders.filter($"o_orderdate" < "1995-03-28")
//    val filterShipDate = lineitem.filter($"l_shipdate" > "1995-03-28")
//
//    val joinCustOrd = filterMktSeg.join(filterOrdDate, $"c_custkey" === $"o_custkey")
//    val joinOrdLine = joinCustOrd.join(filterShipDate, $"o_orderkey" === $"l_orderkey")
//    var res = joinOrdLine.groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority")
//            .agg(sum(expr("l_extendedprice * (1 - l_discount)")).alias("revenue"))

    // New query
    val flatten_no = nestedOrders.withColumn("lineitem", explode($"o_lineitems"))
    val filterOrd = flatten_no.filter($"o_orderdate" < "1995-03-28")
    val filterLine = filterOrd.filter($"lineitem.l_commitdate" > "1995-03-28") // SA: l_commitdate -> l_shipdate
    val filterMktSeg = customer.filter($"c_mktsegment" === "HOUSEHOLD")
    val joinCust = filterMktSeg.join(filterLine, $"c_custkey" === $"o_custkey")
    val projectJoinCust = joinCust.select($"lineitem.l_orderkey".alias("l_orderkey"), $"o_orderdate", $"o_shippriority",
      $"lineitem.l_extendedprice".alias("l_extendedprice"), $"lineitem.l_discount".alias("l_discount"))
    var res = projectJoinCust.groupBy($"l_orderkey", $"o_orderdate", $"o_shippriority")
      .agg(sum(expr("l_extendedprice * (1 - l_discount)")).alias("revenue"))
  //      .agg(sum(decrease($"lineitem.l_extendedprice", $"lineitem.l_discount")).alias("revenue"))
//        .sort($"revenue", $"o_orderdate")

    res
  }

  override def getName(): String = "TPCH03"

  override def whyNotQuestion: Twig =   {
    var twig = new Twig()
    val root = twig.createNode("root")
    val key = twig.createNode("o_orderkey", 1, 1, "1821349")
    val rev = twig.createNode("revenue", 1, 1, "ltltltlt100000")
    twig = twig.createEdge(root, key, false)
    twig = twig.createEdge(root, rev, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree =  {
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
    if (node.name == "l_commitdate" && node.children.isEmpty) {
      node.name = "l_shipdate"
      node.modified = true
      return
    }
    for (child <- node.children){
      replaceDate(child)
    }
  }
}
