package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.{TestConfiguration, TestScenario}
import de.uni_stuttgart.ipvs.provenance.schema_alternatives.PrimarySchemaSubsetTree
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Lineitem(
                     l_orderkey: Long,
                     l_partkey: Long,
                     l_suppkey: Long,
                     l_linenumber: Long,
                     l_quantity: Double,
                     l_extendedprice: Double,
                     l_discount: Double,
                     l_tax: Double,
                     l_returnflag: String,
                     l_linestatus: String,
                     l_shipdate: String,
                     l_commitdate: String,
                     l_receiptdate: String,
                     l_shipinstruct: String,
                     l_shipmode: String,
                     l_comment: String)

case class Customer(
                     c_custkey: Long,
                     c_name: String,
                     c_address: String,
                     c_nationkey: Long,
                     c_phone: String,
                     c_acctbal: Double,
                     c_mktsegment: String,
                     c_comment: String)

case class Nation(
                   n_nationkey: Long,
                   n_name: String,
                   n_regionkey: Long,
                   n_comment: String)

case class Order(
                  o_orderkey: Long,
                  o_custkey: Long,
                  o_orderstatus: String,
                  o_totalprice: Double,
                  o_orderdate: String,
                  o_orderpriority: String,
                  o_clerk: String,
                  o_shippriority: String,
                  o_comment: String)

case class Part(
                 p_partkey: Long,
                 p_name: String,
                 p_mfgr: String,
                 p_brand: String,
                 p_type: String,
                 p_size: Long,
                 p_container: String,
                 p_retailprice: Double,
                 p_comment: String)

case class Partsupp(
                     ps_partkey: Long,
                     ps_suppkey: Long,
                     ps_availqty: Long,
                     ps_supplycost: Double,
                     ps_comment: String)

case class Region(
                   r_regionkey: Long,
                   r_name: String,
                   r_comment: String)

case class Supplier(
                     s_suppkey: Long,
                     s_name: String,
                     s_address: String,
                     s_nationkey: Long,
                     s_phone: String,
                     s_acctbal: Double,
                     s_comment: String)


abstract class TPCHScenario(spark: SparkSession, testConfiguration: TestConfiguration) extends TestScenario(spark, testConfiguration) {

  import spark.implicits._

  lazy val customerSchema = Seq(Customer(0L, "", "", 0L, "", 0.0, "", "")).toDF().schema

  def loadCustomer(): DataFrame = {
    val completePath = testConfiguration.pathToData + testConfiguration.getZeros() +"/customer*.tbl*"
    spark.read.schema(customerSchema).option("delimiter", "|").csv(completePath)
  }

  lazy val lineitemSchema = Seq(Lineitem(0L, 0L, 0L, 0L, 0.0, 0.0, 0.0, 0.0, "", "", "", "", "", "", "", "")).toDF().schema

  def loadLineItem(): DataFrame = {
    val completePath = testConfiguration.pathToData + testConfiguration.getZeros() +"/lineitem.tbl*"
    spark.read.schema(lineitemSchema).option("header", false).option("delimiter", "|").csv(completePath)
  }

  lazy val nationSchema = Seq(Nation(0L, "", 0L, "")).toDF().schema

  def loadNation(): DataFrame = {
    val completePath = testConfiguration.pathToData + testConfiguration.getZeros() +"/nation.tbl*"
    spark.read.schema(nationSchema).option("header", false).option("delimiter", "|").csv(completePath)
  }

  lazy val orderSchema = Seq(Order(0L, 0L, "", 0.0, "", "", "", 0L, "")).toDF().schema

  def loadOrder(): DataFrame = {
    val completePath = testConfiguration.pathToData + testConfiguration.getZeros() +"/orders.tbl*"
    spark.read.schema(orderSchema).option("header", false).option("delimiter", "|").csv(completePath)
  }

  lazy val partSchema = Seq(Part(0L, "", "", "", "", 0L, "", 0.0, "")).toDF().schema

  def loadPart(): DataFrame = {
    val completePath = testConfiguration.pathToData + testConfiguration.getZeros() +"/part.tbl*"
    spark.read.schema(partSchema).option("header", false).option("delimiter", "|").csv(completePath)
  }

  lazy val partsuppSchema = Seq(Partsupp(0L, 0L, 0L, 0.0, "")).toDF().schema

  def loadPartSupp(): DataFrame = {
    val completePath = testConfiguration.pathToData + testConfiguration.getZeros() +"/partsupp.tbl*"
    spark.read.schema(partsuppSchema).option("header", false).option("delimiter", "|").csv(completePath)
  }

  lazy val regionSchema = Seq(Region(0L, "", "")).toDF().schema

  def loadRegion(): DataFrame = {
    val completePath = testConfiguration.pathToData + testConfiguration.getZeros() +"/region.tbl*"
    spark.read.schema(regionSchema).option("header", false).option("delimiter", "|").csv(completePath)
  }

  lazy val supplierSchema = Seq(Supplier(0L, "", "", 0L, "", 0.0, "")).toDF().schema

  def loadSupplier(): DataFrame = {
    val completePath = testConfiguration.pathToData + testConfiguration.getZeros() +"/supplier.tbl*"
    spark.read.schema(supplierSchema).option("header", false).option("delimiter", "|").csv(completePath)
  }

  lazy val nestedLineItemList = ArrayType(lineitemSchema, true)
  lazy val nestedOrdersSchema = orderSchema.add("o_lineitems", nestedLineItemList)

  def loadNestedOrders(): DataFrame = {
    val completePath = testConfiguration.pathToData + testConfiguration.getZeros() +"/nestedOrders.json*"
    spark.read.schema(nestedOrdersSchema).json(completePath)
  }

  def loadNestedOrders001(): DataFrame = {
    val completePath = testConfiguration.pathToData + testConfiguration.getZeros() +"/nestedOrders001.json*"
    spark.read.schema(nestedOrdersSchema).json(completePath)
  }

  lazy val nestedOrdersList = ArrayType(nestedOrdersSchema, true)
  lazy val nestedCustomerSchema = customerSchema.add("c_orders", nestedOrdersList)

  def loadNestedCustomer(): DataFrame = {
    val completePath = testConfiguration.pathToData + testConfiguration.getZeros() +"/nestedCustomers.json*"
    spark.read.schema(nestedCustomerSchema).json(completePath)
  }

  def loadNestedCustomer001(): DataFrame = {
    val completePath = testConfiguration.pathToData + testConfiguration.getZeros() +"/nestedCustomers001.json*"
    spark.read.schema(nestedCustomerSchema).json(completePath)
  }

  def getLineItem001Schema() : StructType = {
    val completePath = testConfiguration.pathToData + testConfiguration.getZeros() +"/lineitem001.json*"
    val l = spark.read.json(completePath)
    l.schema
  }

  lazy val lineitem001Schema = getLineItem001Schema()

  def loadLineItem001(): DataFrame = {
    val completePath = testConfiguration.pathToData + testConfiguration.getZeros() +"/lineitem001.json*"
    spark.read.schema(lineitem001Schema).json(completePath)
  }

  def getOrders001Schema() : StructType = {
    val completePath = testConfiguration.pathToData + testConfiguration.getZeros() +"/orders001.json*"
    val l = spark.read.json(completePath)
    l.schema
  }

  lazy val orders001Schema = getOrders001Schema()

  def loadOrder001(): DataFrame = {
    val completePath = testConfiguration.pathToData + testConfiguration.getZeros() +"/orders001.json*"
    spark.read.schema(orders001Schema).json(completePath)
  }

  def getLineItemAlternatives1(primaryTree: PrimarySchemaSubsetTree, taxDiscount: Seq[String], date: Seq[String]): PrimarySchemaSubsetTree = {
    testConfiguration.schemaAlternativeSize match {
      case alt if ((alt & 1) > 0 && (alt & 2) > 0) => {
        LineItemAlternatives().createAlternativesWith1Permutations(primaryTree, taxDiscount, date)
      }
      case alt if ((alt & 1) > 0) => {
        LineItemAlternatives().createAlternativesWith1Permutations(primaryTree, taxDiscount)
      }
      case alt if ((alt & 2) > 0) => {
        LineItemAlternatives().createAlternativesWith1Permutations(primaryTree, date)
      }
      case _ => primaryTree
    }
  }

  def getLineItemAlternatives2(primaryTree: PrimarySchemaSubsetTree, taxDiscount: Seq[String], date: Seq[String]): PrimarySchemaSubsetTree = {
    testConfiguration.schemaAlternativeSize match {
      case alt if ((alt & 1) > 0 && (alt & 2) > 0) => {
        LineItemAlternatives().createAlternativesWith2Permutations(primaryTree, taxDiscount, date)
      }
      case alt if ((alt & 1) > 0) => {
        LineItemAlternatives().createAlternativesWith1Permutations(primaryTree, taxDiscount)
      }
      case alt if ((alt & 2) > 0) => {
        LineItemAlternatives().createAlternativesWith2Permutations(primaryTree, date)
      }
      case _ => primaryTree
    }
  }

  def getLineItemAlternatives3(primaryTree: PrimarySchemaSubsetTree, taxDiscount: Seq[String], date: Seq[String]): PrimarySchemaSubsetTree = {
    testConfiguration.schemaAlternativeSize match {
      case alt if ((alt & 1) > 0 && (alt & 2) > 0) => {
        LineItemAlternatives().createAlternativesWith3Permutations(primaryTree, taxDiscount, date)
      }
      case alt if ((alt & 1) > 0) => {
        LineItemAlternatives().createAlternativesWith1Permutations(primaryTree, taxDiscount)
      }
      case alt if ((alt & 2) > 0) => {
        LineItemAlternatives().createAlternativesWith3Permutations(primaryTree, date)
      }
      case _ => primaryTree
    }
  }

  def getNestedAlternatives1(primaryTree: PrimarySchemaSubsetTree, orderPriority: Seq[String], taxDiscount: Seq[String], date: Seq[String]): PrimarySchemaSubsetTree = {
    testConfiguration.schemaAlternativeSize match {
      case alt if ((alt & 1) > 0 && (alt & 2) > 0 && (alt & 4) > 0) => {
        NestedOrdersAlternatives.createAlternativesWithOrdersWith1Permutations(primaryTree, orderPriority, taxDiscount, date)
      }
      case alt if ((alt & 1) > 0 && (alt & 2) > 0) => {
        LineItemAlternatives().createAlternativesWith1Permutations(primaryTree, taxDiscount, date)
      }
      case alt if ((alt & 1) > 0 && (alt & 4) > 0) => {
        NestedOrdersAlternatives.create1AlternativesWithOrderAlternatives(primaryTree, orderPriority, taxDiscount)
      }
      case alt if ((alt & 2) > 0 && (alt & 4) > 0) => {
        NestedOrdersAlternatives.create1AlternativesWithOrderAlternatives(primaryTree, orderPriority, date)
      }
      case alt if ((alt & 1) > 0) => {
        LineItemAlternatives().createAlternativesWith1Permutations(primaryTree, taxDiscount)
      }
      case alt if ((alt & 2) > 0) => {
        LineItemAlternatives().createAlternativesWith1Permutations(primaryTree, date)
      }
      case alt if ((alt & 4) > 0) => {
        OrdersAlternatives.createAllAlternatives(primaryTree)
      }
      case _ => primaryTree
    }
  }

  def getNestedAlternatives2(primaryTree: PrimarySchemaSubsetTree, orderPriority: Seq[String], taxDiscount: Seq[String], date: Seq[String]): PrimarySchemaSubsetTree = {
    testConfiguration.schemaAlternativeSize match {
      case alt if ((alt & 1) > 0 && (alt & 2) > 0 && (alt & 4) > 0) => {
        NestedOrdersAlternatives.createAlternativesWithOrdersWith2Permutations(primaryTree, orderPriority, taxDiscount, date)
      }
      case alt if ((alt & 1) > 0 && (alt & 2) > 0) => {
        LineItemAlternatives().createAlternativesWith2Permutations(primaryTree, taxDiscount, date)
      }
      case alt if ((alt & 1) > 0 && (alt & 4) > 0) => {
        NestedOrdersAlternatives.create1AlternativesWithOrderAlternatives(primaryTree, orderPriority, taxDiscount)
      }
      case alt if ((alt & 2) > 0 && (alt & 4) > 0) => {
        NestedOrdersAlternatives.create2AlternativesWithOrderAlternatives(primaryTree, orderPriority, date)
      }
      case alt if ((alt & 1) > 0) => {
        LineItemAlternatives().createAlternativesWith1Permutations(primaryTree, taxDiscount)
      }
      case alt if ((alt & 2) > 0) => {
        LineItemAlternatives().createAlternativesWith2Permutations(primaryTree, date)
      }
      case alt if ((alt & 4) > 0) => {
        OrdersAlternatives.createAllAlternatives(primaryTree)
      }
      case _ => primaryTree
    }
  }

  def getNestedOrderAlternatives2(primaryTree: PrimarySchemaSubsetTree, orderPriority: Seq[String], date: Seq[String]): PrimarySchemaSubsetTree = {
    testConfiguration.schemaAlternativeSize match {
      case alt if ((alt & 2) > 0 && (alt & 4) > 0) => {
        NestedOrdersAlternatives.create2AlternativesWithOrderAlternatives(primaryTree, orderPriority, date)
      }
      case alt if ((alt & 2) > 0) => {
        LineItemAlternatives().createAlternativesWith2Permutations(primaryTree, date)
      }
      case alt if ((alt & 4) > 0) => {
        OrdersAlternatives.createAllAlternatives(primaryTree)
      }
      case _ => primaryTree
    }
  }

}
