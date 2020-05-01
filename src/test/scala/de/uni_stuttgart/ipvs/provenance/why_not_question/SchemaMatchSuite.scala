package de.uni_stuttgart.ipvs.provenance.why_not_question

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import org.apache.spark.sql.DataFrame
import org.scalatest.FunSuite

class SchemaMatchSuite extends FunSuite with SharedSparkTestDataFrames with DataFrameComparer {


  def getValidatedPCTwig() : Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val flat_key = twig.createNode("flat_key")
    val nested_obj = twig.createNode("nested_obj")
    twig = twig.createEdge(root, flat_key, false)
    twig = twig.createEdge(root, nested_obj, false)
    twig.validate.get
  }

  def getContainsConditionTwig() : Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val flat_key = twig.createNode("flat_key", condition = "contains_1")
    twig = twig.createEdge(root, flat_key, false)
    twig.validate.get
  }

  def getNotContainsConditionTwig() : Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val flat_key = twig.createNode("flat_key", condition = "nconncon_1")
    twig = twig.createEdge(root, flat_key, false)
    twig.validate.get
  }

  def getMultiplicityTwig() : Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val flat_list = twig.createNode("flat_list", min = 1, max = 5)
    val element = twig.createNode("element", condition = "contains_1")
    twig = twig.createEdge(root, flat_list, false)
    twig = twig.createEdge(flat_list, element, false)
    twig.validate.get
  }

  def getSingleMatchADTwig(): Twig = {
    var twig = new Twig()
    val a = twig.createNode("a", condition = "c a")
    val b = twig.createNode("b")
    val c = twig.createNode("c")
    val f = twig.createNode("f")
    val g = twig.createNode("g", condition = "c g")
    twig = twig.createEdge(a, b)
    twig = twig.createEdge(a, c, true)
    twig = twig.createEdge(c, f, true)
    twig = twig.createEdge(c, g, true)
    twig.validate().get
  }

  def getEqualityConditionTwig(): Twig = {
    var twig = new Twig()
    val a = twig.createNode("a")
    val b = twig.createNode("b", condition="1")
    twig = twig.createEdge(a, b)
    twig.validate().get
  }

  def getGTConditionTwig(): Twig = {
    var twig = new Twig()
    val a = twig.createNode("a")
    val b = twig.createNode("b", condition="gtgtgtgt1")
    twig = twig.createEdge(a, b)
    twig.validate().get
  }

  def getLTConditionTwig(): Twig = {
    var twig = new Twig()
    val a = twig.createNode("a")
    val b = twig.createNode("b", condition="ltltltlt1")
    twig = twig.createEdge(a, b)
    twig.validate().get
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

  def getDataFrameAndMatches(pathToDf: String, twig: Twig): (DataFrame, Seq[SchemaMatch]) = {
    val df = getDataFrame(pathToDf)
    val schema = new Schema(df)
    val schemaMatcher = SchemaMatcher(twig, schema)
    (df, schemaMatcher.getCandidates())
  }


  test("serialization PC") {
    val input = getDataFrameAndMatches(pathToNestedData2, getValidatedPCTwig())
    val firstMatch = input._2(0)
    val serialization = firstMatch.serialize()
    //input._1.printSchema()
    //serialization.foreach(println)

    assert(serialization.length == 3)
    assert(serialization.contains((0, 0, 0, 1, -1, "root", "")))
    var option1 = serialization.contains((1, 0, 0, 1, -1, "nested_obj", ""))
    option1 &= serialization.contains((2, 0, 0, 1, -1, "flat_key", ""))
    var option2 = serialization.contains((2, 0, 0, 1, -1, "nested_obj", ""))
    option2 &= serialization.contains((1, 0, 0, 1, -1, "flat_key", ""))
    assert(option1 || option2)
  }

  test("serialization AD") {
    val input = getDataFrameAndMatches(pathToDoc2, getSingleMatchADTwig())
    val firstMatch = input._2(0)
    val serialization = firstMatch.serialize()
    //input._1.printSchema()
    //serialization.foreach(println)

    assert(serialization.length == 10)
    assert(serialization.contains((1, 0, 1, 1, -1, "a", "c a")))
    assert(serialization.contains((2, 1, 0, 1, -1, "a", "")) || serialization.contains((2, 1, 0, 1, -1, "b", "")))
    assert(serialization.map(x => x._1).distinct.length == 10)
    assert(serialization.map(x => x._2).distinct.length == 7)
  }

  test("serialization condition equal") {
    val input = getDataFrameAndMatches(pathToDoc2, getEqualityConditionTwig())
    val firstMatch = input._2(0)
    val serialization = firstMatch.serialize()
    //input._1.printSchema()
    //serialization.foreach(println)

    assert(serialization.size == 3)
    assert(serialization.filter(x => x._3 == 1.toByte && x._6 == "b" && x._7 == "1").length == 1)
  }

  test("serialization condition greater than") {
    val input = getDataFrameAndMatches(pathToDoc2, getGTConditionTwig())
    val firstMatch = input._2(0)
    val serialization = firstMatch.serialize()
    //input._1.printSchema()
    //serialization.foreach(println)

    assert(serialization.size == 3)
    assert(serialization.filter(x => x._3 == 4.toByte && x._6 == "b" && x._7 == "1").length == 1)
  }

  test("serialization condition less than") {
    val input = getDataFrameAndMatches(pathToDoc2, getLTConditionTwig())
    val firstMatch = input._2(0)
    val serialization = firstMatch.serialize()
    //input._1.printSchema()
    //serialization.foreach(println)

    assert(serialization.size == 3)
    assert(serialization.filter(x => x._3 == 3.toByte && x._6 == "b" && x._7 == "1").length == 1)
  }

  test("serialization condition contains") {
    val input = getDataFrameAndMatches(pathToNestedData2, getContainsConditionTwig())
    val firstMatch = input._2(0)
    val serialization = firstMatch.serialize()
    //input._1.printSchema()
    //serialization.foreach(println)

    assert(serialization.size == 2)
    assert(serialization.filter(x => x._3 == 2.toByte && x._6 == "flat_key" && x._7 == "_1").length == 1)

  }

  test("serialization condition not contains") {
    val input = getDataFrameAndMatches(pathToNestedData2, getNotContainsConditionTwig())
    val firstMatch = input._2(0)
    val serialization = firstMatch.serialize()
    //input._1.printSchema()
    //serialization.foreach(println)

    assert(serialization.size == 2)
    assert(serialization.filter(x => x._3 == 5.toByte && x._6 == "flat_key" && x._7 == "_1").length == 1)
  }

  test("serialization cardinality") {
    val input = getDataFrameAndMatches(pathToNestedData2, getMultiplicityTwig())
    val firstMatch = input._2(0)
    val serialization = firstMatch.serialize()
    //input._1.printSchema()
    //serialization.foreach(println)

    assert(serialization.size == 3)
    assert(serialization.filter(x => x._3 == 0.toByte && x._4 == 1 && x._5 == 5 && x._6 == "flat_list" && x._7 == "").length == 1)

    assert(serialization(0) == (0,0,0,1, -1, "root", ""))
    assert(serialization(1) ==(1,0,0,1,5, "flat_list", ""))
    assert(serialization(2) == (2,1,2,1, -1,"element","_1"))

  }

  test("non schema-root serialization") {
    val input = getDataFrameAndMatches(pathToDoc4, getSimplePathAmbiguousTreePattern1())
    val firstMatch = input._2(0)
    val serialization = firstMatch.serialize()
    //input._1.printSchema()
    //serialization.foreach(println)

    assert(serialization.size == 6)
    assert(serialization(0) == (0,0,0,1, -1, "root", ""))
    assert(serialization(1) == (1,0,0,1, -1, "a", ""))
    assert(serialization(2) == (2,1,0,1, -1, "b", ""))
    assert(serialization(3) == (3,2,0,1, -1, "c", ""))
    assert(serialization(4) == (4,3,0,1, -1, "b", ""))
    assert(serialization(5) == (5,4,0,1, -1, "d", ""))
  }
}
