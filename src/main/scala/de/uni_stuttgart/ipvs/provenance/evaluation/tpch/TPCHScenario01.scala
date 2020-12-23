package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{avg, count, lit, sum, udf, explode}

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

  override def referenceScenario: DataFrame = {
    return minimalScenario
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
        sum($"l_quantity").as("SUM_QTY"),
        sum($"disc_price").as("SUM_DISC_PRICE"))
  }

  override def getName(): String = "TPCH01"

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val avg_disc = twig.createNode("SUM_DISC_PRICE", 1, 1, "ltltltlt0.09")
    twig = twig.createEdge(root, avg_disc, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    val nestedOrders = input.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].location.rootPaths.head.toUri.toString.contains("nestedorders")
    if (!nestedOrders) {
      val saSize = testConfiguration.schemaAlternativeSize
      createAlternatives(primaryTree, saSize)

      for (i <- 0 until saSize) {
        replaceDiscount1(primaryTree.alternatives(i).rootNode)
        replaceTax(primaryTree.alternatives(i).rootNode)
        replaceDiscount2(primaryTree.alternatives(i).rootNode)
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
