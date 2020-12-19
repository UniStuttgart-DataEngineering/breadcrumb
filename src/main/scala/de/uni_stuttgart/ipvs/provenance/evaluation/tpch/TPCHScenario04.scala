package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions.{avg, count, explode, max, min, sum, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

class TPCHScenario04(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {


  import spark.implicits._

//  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//  val increase = udf { (x: Double, y: Double) => x * (1 + y) }

  override def referenceScenario: DataFrame = {
    val nestedOrders = loadNestedOrders()
//    var res = nestedOrders.agg(min($"o_orderdate"), max($"o_orderdate"))
    val flattened_no = nestedOrders.withColumn("lineitem", explode($"o_lineitems"))
//    val filter1_no = flattened_no.filter($"o_orderdate" >= "1997-01-01")
//    val filter2_no = filter1_no.filter($"o_orderdate" < "1997-04-01")
    val filter1_no = flattened_no.filter($"o_orderdate".between("1997-01-01", "1997-03-31"))
    val filter3_no = filter1_no.filter($"lineitem.l_commitdate" < $"lineitem.l_receiptdate") // SA: receiptdate -> shipdate
    var res = filter3_no.groupBy($"o_orderpriority").agg(count($"o_orderkey").alias("order_count"))
//    res = res.sort($"o_orderpriority")
    res
  }

  override def getName(): String = "TPCH04"

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val priority = twig.createNode("o_orderpriority", 1, 1, "1-URGENT")
    val count = twig.createNode("order_count", 1, 1, "ltltltlt25000")
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
        replaceCommitDate(primaryTree.alternatives(i).rootNode)
      }
    }
    primaryTree
  }

  def replaceCommitDate(node: SchemaNode): Unit ={
    if (node.name == "l_commitdate" && node.children.isEmpty) {
      node.name = "l_shipdate"
      node.modified = true
      return
    }
    for (child <- node.children){
      replaceCommitDate(child)
    }
  }
}
