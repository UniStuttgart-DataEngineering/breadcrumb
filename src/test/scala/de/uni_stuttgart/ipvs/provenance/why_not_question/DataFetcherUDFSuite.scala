package de.uni_stuttgart.ipvs.provenance.why_not_question

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.FunSuite

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class DataFetcherUDFSuite extends FunSuite with SharedSparkTestDataFrames with DataFrameComparer {



  val oschema = ArrayType(StructType(Array(StructField("names",StringType))))
  lazy val dataFetcherUDF = spark.udf.register("dfu", new DataFetcherUDF().call _)


  def generateSerializedTreeToTopLevelString(): Seq[(Short, Short, Byte, Int, Int, String, String)] = {
    var nodes = new ListBuffer[(Short, Short, Byte, Int, Int, String, String)]
    nodes += new Tuple7(0, 0, 0, 0, 0, "root", "")
    nodes += new Tuple7(1, 0, 1, 0, 0, "flat_key", "1_1_flat_val")
    nodes.toList
  }

  test("UDF Call on top level string"){
    val df = getDataFrame()
    df.collect().foreach(println)
    val udfInitilialization = dataFetcherUDF
    val res = df.filter(dataFetcherUDF(struct(df.columns.toSeq.map(col(_)): _*), typedLit(generateSerializedTreeToTopLevelString())))
    val colRes = res.collect()
    colRes.foreach(println)
    assert(colRes.length == 1)
    colRes.foreach(x => assert(x.getAs[String]("flat_key") == "1_1_flat_val"))

  }

  def generateSerializedTreeToNestedString(): Seq[(Short, Short, Byte, Int, Int, String, String)] = {
    var nodes = new ListBuffer[(Short, Short, Byte, Int, Int, String, String)]
    nodes += new Tuple7(0, 0, 0, 0, 0, "root", "")
    nodes += new Tuple7(1, 0, 0, 0, 0, "nested_obj", "")
    nodes += new Tuple7(2, 1, 0, 0, 0, "nested_obj", "")
    nodes += new Tuple7(3, 2, 1, 0, 0, "nested_key", "1_nested_val")
    nodes.toList
  }

  test("UDF Call on nested object"){
    val df = getDataFrame()
    df.collect().foreach(println)
    val udfInitilialization = dataFetcherUDF
    val res = df.filter(dataFetcherUDF(struct(df.columns.toSeq.map(col(_)): _*), typedLit(generateSerializedTreeToNestedString())))
    val colRes = res.collect()
    colRes.foreach(println)
    assert(colRes.length == 2)
    colRes.foreach(x => assert(x.getAs[Row]("nested_obj").getAs[Row]("nested_obj").getAs[String]("nested_key") == "1_nested_val"))
  }


  def generateSerializedTreeToNestedCollection(): Seq[(Short, Short, Byte, Int, Int, String, String)] = {
    var nodes = new ListBuffer[(Short, Short, Byte, Int, Int, String, String)]
    nodes += {(0, 0, 0, 0, 0, "root", "")}
    nodes += {(1, 0, 0, 1, 1, "flat_list", "")}
    nodes += {(2, 1, 1, 0, 0, "element", "1_list_val_1_x")}
    nodes.toList
  }

  test("UDF Call on nested list 1"){
    val df = getDataFrame()
    df.collect().foreach(println)
    val udfInitilialization = dataFetcherUDF
    val res = df.filter(dataFetcherUDF(struct(df.columns.toSeq.map(col(_)): _*), typedLit(generateSerializedTreeToNestedCollection())))
    val colRes = res.collect()
    colRes.foreach(println)
    //assert(colRes.length == 2)
    assert(colRes.count(x => x.getAs[mutable.WrappedArray[String]]("flat_list").contains("1_list_val_1_x")) == 2)
  }

  def generateSerializedTreeToNestedCollection21(): Seq[(Short, Short, Byte, Int, Int, String, String)] = {
    var nodes = new ListBuffer[(Short, Short, Byte, Int, Int, String, String)]
    nodes += {(0, 0, 0, 0, 0, "root", "")}
    nodes += {(1, 0, 0, 1, 1, "nested_list", "")}
    nodes += {(2, 1, 0, 0, 0, "element", "")}
    nodes += {(3, 2, 1, 0, 0, "element", "1_list_val_1_2")}
    nodes.toList
  }

  test("UDF Call on nested list 21"){
    val df = getDataFrame()
    df.collect().foreach(println)
    val udfInitilialization = dataFetcherUDF
    val res = df.filter(dataFetcherUDF(struct(df.columns.toSeq.map(col(_)): _*), typedLit(generateSerializedTreeToNestedCollection21())))
    val colRes = res.collect()
    colRes.foreach(println)
    //assert(colRes.length == 2)
    assert(colRes.count(row => {
      var valid = false
      val queried_array = row.getAs[mutable.WrappedArray[mutable.WrappedArray[String]]]("nested_list")
      queried_array.foreach{
        nested =>
          valid |= nested.contains("1_list_val_1_2")
      }
      valid
    } ) == 2)
  }

  def generateSerializedTreeToNestedCollection22(): Seq[(Short, Short, Byte, Int, Int, String, String)] = {
    var nodes = new ListBuffer[(Short, Short, Byte, Int, Int, String, String)]
    nodes += {(0, 0, 0, 0, 0, "root", "")}
    nodes += {(1, 0, 0, 1, 1, "nested_list", "")}
    nodes += {(2, 1, 0, 1, 1, "element", "")}
    nodes += {(3, 2, 1, 0, 0, "element", "1_list_val_1_2")}
    nodes.toList
  }

  test("UDF Call on nested list 22"){
    val df = getDataFrame()
    df.collect().foreach(println)
    val udfInitilialization = dataFetcherUDF
    val res = df.filter(dataFetcherUDF(struct(df.columns.toSeq.map(col(_)): _*), typedLit(generateSerializedTreeToNestedCollection22())))
    val colRes = res.collect()
    colRes.foreach(println)
    //assert(colRes.length == 2)
    assert(colRes.count(row => {
      var valid = false
      val queried_array = row.getAs[mutable.WrappedArray[mutable.WrappedArray[String]]]("nested_list")
      queried_array.foreach{
        nested =>
          valid |= nested.contains("1_list_val_1_2")
      }
      valid
    } ) == 2)
  }

  def getSimplePathAmbiguousTreePattern1() : Twig = {
    var twig = new Twig()
    val a = twig.createNode("a")
    val b = twig.createNode("b")
    val d = twig.createNode("d")
    twig = twig.createEdge(a, b, true)
    twig = twig.createEdge(b, d, false)
    twig.validate.get
  }

  test("Entire Workflow"){
    val df = getDataFrame(pathToDoc4)
    val twig = getSimplePathAmbiguousTreePattern1()
    val schema = new Schema(df)
    val schemaMatcher = SchemaMatcher(twig, schema)
    val firstMatch = schemaMatcher.getCandidates()(0)
    val serialization = firstMatch.serialize()
    val udfInitilialization = dataFetcherUDF


    val res = df.filter(dataFetcherUDF(struct(df.columns.toSeq.map(col(_)): _*), typedLit(serialization)))
    assert(res.count == 1)

    val cropped = res.select(res.col("a.b.c.b.d").alias("d"))
    assert(cropped.collect()(0)(0) == 42)
  }

  def getTwigForNestedData(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val a = twig.createNode("flat_key")
    twig = twig.createEdge(root, a, false)
    twig.validate().get
  }

}
