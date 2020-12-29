package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, count, lit, sum, udf, explode, expr}

class TPCHScenario01(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {

  import spark.implicits._

  //This query reports the amount of business that was billed, shipped, and returned.
  //Alternatives: 1) l_tax -> l_discount, l_discount -> l_tax
  //              2) shipdate -> commitdate -> receiptdate
  //WN-Question: Why is a value not bigger than xyz
  //Assumed possible answers: filter, aggregation (through alternative), combination of them? --> to be discussed, can be pruned??????
  //Compared to other solutions: aggregation

  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
  val increase = udf { (x: Double, y: Double) => x * (1 + y) }

  def referenceScenarioOriginal: DataFrame = {
     loadLineItem()
       .filter($"l_shipdate" <= "1998-09-02")
       .groupBy($"l_returnflag", $"l_linestatus")
       .agg(sum($"l_quantity"), sum($"l_extendedprice"),
         sum(decrease($"l_extendedprice", $"l_discount")),
         sum(increase(decrease($"l_extendedprice", $"l_discount"), $"l_tax")),
         avg($"l_quantity"), avg($"l_extendedprice"), avg($"l_discount"), count($"l_quantity"))
       .sort($"l_returnflag", $"l_linestatus")
  }

//  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//  val increase = udf { (x: Double, y: Double) => x * (1 + y) }

/*
Original result:
+------------+------------+---------+-------------------+-------------------+-------------------+------------------+------------------+-------------------+-----------+
|l_returnflag|l_linestatus|sum_qty  |sum_base_price     |sum_disc_price     |sum_charge         |avg_qty           |avg_price         |avg_disc           |count_order|
+------------+------------+---------+-------------------+-------------------+-------------------+------------------+------------------+-------------------+-----------+
|N           |O           |5511363.0|8.260892168619934E9|7.848121363027013E9|8.162567467750073E9|25.490786735118636|38207.724751953814|0.04997761435639147|216210     |
+------------+------------+---------+-------------------+-------------------+-------------------+------------------+------------------+-------------------+-----------+

Rewrite without SA:
java.lang.NullPointerException
	at de.uni_stuttgart.ipvs.provenance.nested_why_not.WhyNotProvenance$.dataFrameAndProvenanceContext(WhyNotProvenance.scala:62)
	at de.uni_stuttgart.ipvs.provenance.nested_why_not.WhyNotProvenance$.rewrite(WhyNotProvenance.scala:68)

Rewrite with SA + MSR:
ERROR: org.apache.spark.sql.catalyst.expressions.Multiply cannot be cast to org.apache.spark.sql.catalyst.expressions.NamedExpression
*/

  def nestedScenario: DataFrame = {
//    // Original query
//    val lineitem = loadLineItem()
//    val filterShipDate = lineitem.filter($"l_shipdate".between("1998-06-03", "1998-08-31"))
//    val res = filterShipDate.groupBy($"l_returnflag", $"l_linestatus")
//            .agg(sum($"l_quantity").alias("sum_qty"), sum($"l_extendedprice").alias("sum_base_price"),
//            sum(expr("l_extendedprice * (1 - l_discount)")).alias("sum_disc_price"),
//            sum(expr("l_extendedprice * (1 - l_discount) * (1 + l_tax)")).alias("sum_charge"),
//            avg($"l_quantity").alias("avg_qty"), avg($"l_extendedprice").alias("avg_price"),
//            avg($"l_discount").alias("avg_disc"), count($"l_orderkey").alias("count_order"))
////          .sort($"l_returnflag", $"l_linestatus")

    // New query
    val nestedOrders = loadNestedOrders()
    val flattened_no = nestedOrders.withColumn("lineitem", explode($"o_lineitems"))
    val project_fno = flattened_no.select($"lineitem.l_returnflag".alias("returnflag"), $"lineitem.l_linestatus".alias("linestatus"),
                      $"lineitem.l_quantity".alias("quantity"), $"lineitem.l_extendedprice".alias("extendedprice"),
                      $"lineitem.l_discount".alias("discount"), $"lineitem.l_tax".alias("tax"),
                      $"lineitem.l_commitdate".alias("shipdate"), $"lineitem.l_orderkey".alias("orderkey")) // SA: l_commitdate -> l_shipdate
    val filter_pfno = project_fno.filter($"shipdate".between("1998-06-03", "1998-08-31"))
    val res = filter_pfno.groupBy($"returnflag", $"linestatus")
        .agg(sum($"quantity").alias("sum_qty"), sum($"extendedprice").alias("sum_base_price"),
//        sum(decrease($"l_extendedprice", $"l_discount")).alias("sum_disc_price"),
//        sum(increase(decrease($"l_extendedprice", $"l_discount"), $"l_tax")).alias("sum_charge"),
        sum(expr("extendedprice * (1 - discount)")).alias("sum_disc_price"),
        sum(expr("extendedprice * (1 - discount) * (1 + tax)")).alias("sum_charge"),
        avg($"quantity").alias("avg_qty"), avg($"extendedprice").alias("avg_price"),
        avg($"discount").alias("avg_disc"), count($"orderkey").alias("count_order"))
//      .sort($"returnflag", $"linestatus")

    res
  }

  override def referenceScenario: DataFrame = {
    //return minimalScenario
    loadLineItem()
    // loadNestedOrders().withColumn("l_lineitem", explode($"o_lineitem"))
      .filter($"l_shipdate" <= "1998-09-02")
      .withColumn("disc_price", ($"l_extendedprice"*(lit(1.0)-$"l_discount")))
      .withColumn("charge", $"l_extendedprice"*(lit(1.0)-$"l_discount")*(lit(1.0)-$"l_tax"))
      .groupBy($"l_returnflag", $"l_linestatus")
      .agg(
        sum($"l_quantity").as("SUM_QTY"),
        sum($"l_extendedprice").as("SUM_BASE_PRICE"),
        sum($"disc_price").as("SUM_DISC_PRICE"),
        sum($"charge").as("SUM_CHARGE"),
        avg($"l_quantity").as("AVG_QTY"),
        avg($"l_extendedprice").as("AVG_PRICE"),
        avg($"l_discount").as("AVG_DISC"),
        count($"l_quantity").as("COUNT_ORDER")
      )
      //.sort($"l_returnflag", $"l_linestatus")
  }

  def minimalScenario: DataFrame = {
    loadLineItem()
      .withColumn("disc_price", ($"l_extendedprice"*(lit(1.0)-$"l_discount")))
      .groupBy($"l_returnflag", $"l_linestatus")
      .agg(
        sum($"l_quantity").as("SUM_QTY")
       ,sum($"disc_price").as("SUM_DISC_PRICE")
      )
  }

  override def getName(): String = "TPCH01"

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val avg_disc = twig.createNode("SUM_QTY", 1, 1, "ltltltlt0.09")
    twig = twig.createEdge(root, avg_disc, false)
    twig.validate.get
  }

   def whyNotQuestion1: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val rf = twig.createNode("returnflag", 1, 1, "N")
    val ls = twig.createNode("linestatus", 1, 1, "O")
    val co = twig.createNode("count_order", 1, 1, "ltltltlt220000")
    twig = twig.createEdge(root, rf, false)
    twig = twig.createEdge(root, ls, false)
    twig = twig.createEdge(root, co, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    //return primaryTree
    val nestedOrders = input.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].location.rootPaths.head.toUri.toString.contains("nestedorders")
    if (!nestedOrders) {
      val saSize = testConfiguration.schemaAlternativeSize
      createAlternatives(primaryTree, saSize)

      for (i <- 0 until saSize) {
        replaceDiscount1(primaryTree.alternatives(i).rootNode)
        replaceTax(primaryTree.alternatives(i).rootNode)
        replaceDiscount2(primaryTree.alternatives(i).rootNode)
      }
      for (i <- 0 until saSize by 2) {
        replaceDate(primaryTree.alternatives(i).rootNode)
      }
    }
    primaryTree
  }

  def replaceDiscount1(node: SchemaNode): Unit ={
    if (node.name == "l_discount" && node.children.isEmpty) {
      node.name = "l_discount_tmp"
      node.modified = true
      return
    }
    for (child <- node.children){
        replaceDiscount1(child)
    }
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

  def replaceDiscount2(node: SchemaNode): Unit ={
    if (node.name == "l_discount_tmp" && node.children.isEmpty) {
      node.name = "l_tax"
      node.modified = true
      return
    }
    for (child <- node.children){
      replaceDiscount2(child)
    }
  }

  def replaceTax(node: SchemaNode): Unit ={
    if (node.name == "l_tax" && node.children.isEmpty) {
      node.name = "l_discount"
      node.modified = true
      return
    }
    for (child <- node.children){
      replaceTax(child)
    }
  }

}
