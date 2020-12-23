package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.{PrimarySchemaSubsetTree, SchemaNode, SchemaSubsetTree}
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class TPCHScenario07(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {


  import spark.implicits._

//  val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
//  val increase = udf { (x: Double, y: Double) => x * (1 + y) }

/*
Original result:
+-----------+-----------+------+-------------------+
|supp_nation|cust_nation|l_year|revenue            |
+-----------+-----------+------+-------------------+
|FRANCE     |GERMANY    |1996  |5.463308330760002E7|
|GERMANY    |FRANCE     |1996  |5.252054902240002E7|
|GERMANY    |FRANCE     |1995  |5.253174666970002E7|
|FRANCE     |GERMANY    |1995  |5.463973273360001E7|
+-----------+-----------+------+-------------------+

Rewrite without SA:

Rewrite with SA + MSR:
*/

  override def referenceScenario: DataFrame = {
    val supplier = loadSupplier()
    val lineitem = loadLineItem()
    val orders = loadOrder()
    val customer = loadCustomer()
    val nation = loadNation()
    val nestedOrders = loadNestedOrders()
    val nestedCustomer = loadCustomer() //TODO: create nestedCustomer using nestedOrders and nation that are nested into customer table
    val nestedSupplier = loadSupplier() //TODO: create nestedSupplier using nation nested into supplier

//    // Original query
//    val filterLine = lineitem.filter($"l_shipdate".between("1995-01-01", "1996-12-31"))
//    val joinSuppLine = filterLine.join(supplier, $"s_suppkey" === $"l_suppkey")
//    val joinOrdLine = joinSuppLine.join(orders, $"l_orderkey" === $"o_orderkey")
//    val joinCustOrd = joinOrdLine.join(customer, $"o_custkey" === $"c_custkey")
//    val joinSuppNa = joinCustOrd.join(nation, $"s_nationkey" === $"n_nationkey")
//    val nation2 = nation.select($"n_nationkey".alias("n_nationkey2"), $"n_name".alias("n_name2"),
//            $"n_regionkey".alias("n_regionkey2"), $"n_comment".alias("n_comment2"))
//    val joinCustNa = joinSuppNa.join(nation2, $"c_nationkey" === $"n_nationkey2")
//    val filterNation = joinCustNa.filter(($"n_name" === "FRANCE" && $"n_name2" === "GERMANY") || ($"n_name" === "GERMANY" && $"n_name2" === "FRANCE"))
//    var res = filterNation.groupBy($"n_name".alias("supp_nation"), $"n_name2".alias("cust_nation"), $"l_shipdate".substr(1,4).alias("l_year"))
//        .agg(sum(expr("l_extendedprice * (1 - l_discount)")).alias("revenue"))

    // New query
    // TODO: find SA (can we have flatten in the explanation?)
    val flattenOrd = nestedCustomer.withColumn("order", explode($"c_orders"))
    val flattenLine = flattenOrd.withColumn("lineitem", explode($"o_lineitem"))
    val flattenCustNa = flattenLine.withColumn("cust_nation", explode($"c_nation"))
    val projectCust = flattenCustNa.select($"cust_nation.n_name".alias("cust_nationname"), $"cust_nation.n_nationkey".alias("cust_nationkey"),
          $"lineitem.l_shipdate".alias("l_shipdate"), $"lineitem.l_extendedprice".alias("l_extendedprice"),
          $"lineitem.l_discount".alias("l_discount"), $"lineitem.l_suppkey".alias("l_suppkey"))
    val filterCust = projectCust.filter($"l_shipdate".between("1995-01-01", "1996-12-31"))
    val flattenSuppNa  = nestedSupplier.withColumn("supp_nation", explode($"s_nation"))
    val projectSupp = flattenSuppNa.select($"s_suppkey", $"supp_nation.n_nationkey".alias("supp_nationkey"), $"supp_nation.n_name".alias("supp_nationname"))
    val joinCustSupp = filterCust.join(projectSupp, $"s_suppkey" === $"l_suppkey")
    var res = joinCustSupp.groupBy($"supp_nationname", $"cust_nationname", $"l_shipdate".substr(1,4).alias("l_year"))
          .agg(sum(expr("l_extendedprice * (1 - l_discount)")).alias("revenue"))

    res
  }

  override def getName(): String = "TPCH07"

  override def whyNotQuestion: Twig = null

}
