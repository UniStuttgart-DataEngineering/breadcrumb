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
    val filter_comm_receipt = filter_shipmode.filter($"lineitem.l_commitdate" < $"lineitem.l_receiptdate")
    val filter_ship_comm = filter_comm_receipt.filter($"lineitem.l_shipdate" < $"lineitem.l_commitdate")
    val filter_receipt = filter_ship_comm.filter($"lineitem.l_commitdate".between("1994-01-01", "1994-12-31"))
    val res = filter_receipt.groupBy($"lineitem.l_shipmode")
      .agg(sum(when($"o_orderpriority" === "1-URGENT" || $"o_orderpriority" === "2-HIGH", 1).otherwise(0)).alias("high_line_count"),
        sum(when($"o_orderpriority" =!= "1-URGENT" && $"o_orderpriority" =!= "2-HIGH", 1).otherwise(0)).alias("low_line_count"))
//    var res = flattened_no.select($"lineitem.l_shipmode").distinct()
    res
  }

  override def getName(): String = "TPCH12"

  override def whyNotQuestion(): Twig = null
}
