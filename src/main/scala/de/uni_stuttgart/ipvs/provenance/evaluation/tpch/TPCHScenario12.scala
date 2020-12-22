package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions.{count, explode, sum, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

class TPCHScenario12(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {


  import spark.implicits._

//  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//  val increase = udf { (x: Double, y: Double) => x * (1 + y) }

  override def referenceScenario: DataFrame = {
    val nestedOrders = loadNestedOrders()
    val flattened_no = nestedOrders.withColumn("lineitem", explode($"o_lineitems"))
    val filter_shipmode = flattened_no.filter($"lineitem.l_shipmode" ===  "AIR" || $"lineitem.l_shipmode" ===  "TRUCK")
    val filter_comm_receipt = filter_shipmode.filter($"lineitem.l_commitdate" < $"lineitem.l_receiptdate") //SA: l_receiptdate <-> l_commitdate
    val filter_ship_comm = filter_comm_receipt.filter($"lineitem.l_shipdate" < $"lineitem.l_commitdate") // l_commitdate -> l_receiptdate
    val filter_receipt = filter_ship_comm.filter($"lineitem.l_receiptdate".between("1995-01-01", "1995-12-31"))
    val res = filter_receipt.groupBy($"lineitem.l_shipmode")
      .agg(sum(when($"o_orderpriority" === "1-URGENT" || $"o_orderpriority" === "2-HIGH", 1).otherwise(0)).alias("high_line_count"),
        sum(when($"o_orderpriority" =!= "1-URGENT" && $"o_orderpriority" =!= "2-HIGH", 1).otherwise(0)).alias("low_line_count"))
//    var res = flattened_no.select($"lineitem.l_shipmode").distinct()
    res
  }

  override def getName(): String = "TPCH12"

  override def whyNotQuestion: Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val priority = twig.createNode("l_shipmode", 1, 1, "AIR")
    val count = twig.createNode("high_line_count", 1, 1, "ltltltlt10000")
    twig = twig.createEdge(root, priority, false)
    twig = twig.createEdge(root, count, false)
    twig.validate.get
  }
}
