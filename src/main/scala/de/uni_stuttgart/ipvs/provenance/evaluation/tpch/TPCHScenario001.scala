package de.uni_stuttgart.ipvs.provenance.evaluation.tpch

import de.uni_stuttgart.ipvs.provenance.evaluation.TestConfiguration
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.functions.{collect_list, struct, col, explode}
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.StructType

class TPCHScenario001(spark: SparkSession, testConfiguration: TestConfiguration) extends TPCHScenario(spark, testConfiguration) {
  override def getName(): String = "Sample"
  import spark.implicits._

  override def whyNotQuestion(): Twig = null

  override def referenceScenario(): DataFrame = {
      val factor = 1000
//    val lineitem = loadLineItem()
//    val cntLineItem = lineitem.count()
//    val sampleSizeLI = cntLineItem / factor
//
//    val sample = lineitem.sample(true, 1D * sampleSizeLI / cntLineItem)
//
//    // write to json
//    sample.coalesce(1).write.mode(SaveMode.Overwrite).json(testConfiguration.pathToData + "/lineitemSmall")

//    val orders = loadOrder()
//    val cntOrders = orders.count()
//    val sampleSizeNO = cntOrders / factor
//
//    val sample = orders.sample(true, 1D * sampleSizeNO / cntOrders)
//
//    // write to json
//    sample.coalesce(1).write.mode(SaveMode.Overwrite).json(testConfiguration.pathToData + "/ordersSmall")

//    val nestedOrders = loadNestedOrders()
//    val cntNestedOrders = nestedOrders.count()
//    val sampleSizeNO = cntNestedOrders / factor
//
//    val sample = nestedOrders.sample(true, 1D * sampleSizeNO / cntNestedOrders)
//
//    // write to json
//    sample.coalesce(1).write.mode(SaveMode.Overwrite).json(testConfiguration.pathToData + "/nestedOrdersSmall")

    val nestedCustomer = loadNestedCustomer()
//    val cntNestedCust = nestedCustomer.count()
//    val sampleSizeNO = cntNestedCust / factor

    var sample = nestedCustomer
    sample.printSchema()
//    var sample = nestedCustomer.filter($"c_custkey".isNotNull)
//    sample = sample.sample(true, 1D * sampleSizeNO / cntNestedCust)

//    // write to json
//    sample.coalesce(1).write.mode(SaveMode.Overwrite).json(testConfiguration.pathToData + "/nestedCustomerSmall")

//
//    // Generating small data with keeping co-relation
//    def flattenSchema(schema: StructType, prefix: String = null) : Array[Column] = {
//      schema.fields.flatMap(f => {
//        val colName = if (prefix == null) f.name else (prefix + "." + f.name)
//
//        f.dataType match {
//          case st: StructType => flattenSchema(st, colName)
//          case _ => Array(col(colName))
//        }
//      })
//    }
//
//    val nestedCustomer = loadNestedCustomer001()
//
//    // create small customer
//    var sample = nestedCustomer.drop($"c_orders")
//    sample.coalesce(1).write.mode(SaveMode.Overwrite).json(testConfiguration.pathToData + "/customerSmall")


//    // create small nestedOrders
//    var sample = nestedCustomer.withColumn("order", explode($"c_orders"))
//    sample = sample.select($"order")
//    sample = sample.select(flattenSchema(sample.schema):_*)
//    sample.coalesce(1).write.mode(SaveMode.Overwrite).json(testConfiguration.pathToData + "/nestedOrderSmall")


//    // create small orders
//    var sample = nestedCustomer.withColumn("order", explode($"c_orders"))
//    sample = sample.select($"order")
////    sample = sample.drop("order.o_lineitems")
//    sample = sample.select(flattenSchema(sample.schema):_*)
//    sample = sample.drop("o_lineitems")
////    sample.coalesce(1).write.mode(SaveMode.Overwrite).json(testConfiguration.pathToData + "/orderSmall")


//    // create small lineitems
//    var sample = nestedCustomer.withColumn("order", explode($"c_orders"))
//    sample = sample.withColumn("lineitem", explode($"order.o_lineitems"))
//    sample = sample.select($"lineitem")
//    sample = sample.select(flattenSchema(sample.schema):_*)
//    sample.coalesce(1).write.mode(SaveMode.Overwrite).json(testConfiguration.pathToData + "/lineitemSmall")


//    println(cntLineItem)
//    println(sampleSize)
    sample

  }

}
