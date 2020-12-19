package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions.{count, explode, sum, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

class TPCHScenario03(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {


  import spark.implicits._

  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//  val increase = udf { (x: Double, y: Double) => x * (1 + y) }

  override def referenceScenario: DataFrame = {
    val customer = loadCustomer()
//    val res = customer.select($"c_mktsegment").distinct()
    val nestedOrders = loadNestedOrders()
    val flatten_no = nestedOrders.withColumn("lineitem", explode($"o_lineitems"))
    val filter1_no = flatten_no.filter($"o_orderdate" < "1995-03-28")
    val filter2_no = filter1_no.filter($"lineitem.l_shipdate" > "1995-03-28") // SA: shipdate -> commitdate
    val filter3_cust = customer.filter($"c_mktsegment" === "HOUSEHOLD")
    val join_cust_no = filter3_cust.join(filter2_no, $"c_custkey" === $"o_custkey")
    var res = join_cust_no.groupBy($"o_orderkey", $"o_orderdate", $"o_shippriority")
      .agg(sum(decrease($"lineitem.l_extendedprice", $"lineitem.l_discount")).alias("revenue"))
//    res = res.sort($"revenue", $"o_orderdate")
    res
  }

  override def getName(): String = "TPCH03"

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val key = twig.createNode("o_orderkey", 1, 1, "2007047")
    val rev = twig.createNode("revenue", 1, 1, "ltltltlt150000")
    twig = twig.createEdge(root, key, false)
    twig = twig.createEdge(root, rev, false)
    twig.validate.get
  }

  override def computeAlternatives(backtracedWhyNotQuestion: SchemaSubsetTree, input: LeafNode): PrimarySchemaSubsetTree = {
    val primaryTree = super.computeAlternatives(backtracedWhyNotQuestion, input)
    val nestedOrders = input.asInstanceOf[LogicalRelation].relation.asInstanceOf[HadoopFsRelation].location.rootPaths.head.toUri.toString.contains("nestedorders")
    if (!nestedOrders) {
      val saSize = testConfiguration.schemaAlternativeSize
      createAlternatives(primaryTree, saSize)

      for (i <- 0 until saSize by 2) {
        replaceShipDate(primaryTree.alternatives(i).rootNode)
      }
    }
    primaryTree
  }

  def replaceShipDate(node: SchemaNode): Unit ={
    if (node.name == "l_shipdate" && node.children.isEmpty) {
      node.name = "l_commitdate"
      node.modified = true
      return
    }
    for (child <- node.children){
      replaceShipDate(child)
    }
  }
}
