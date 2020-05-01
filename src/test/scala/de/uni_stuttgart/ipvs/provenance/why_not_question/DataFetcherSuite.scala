package de.uni_stuttgart.ipvs.provenance.why_not_question

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.Constants._
import org.apache.spark.sql.functions.{col, lit, struct, typedLit}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.FunSuite

import scala.collection.mutable


class DataFetcherSuite extends FunSuite with SharedSparkTestDataFrames {

  lazy val myTest = spark.udf.register("myTest", (input: mutable.WrappedArray[_], other: Any, min: Long, max: Long) => {
    var foundCounter = 0L
    if (input != null) {
      for (input_item <- input) {
        foundCounter = if (input_item == other) foundCounter + 1L else foundCounter
      }
    }
    if (foundCounter >= min && foundCounter <= max) {
      true
    } else {
      false
    }
  })

  def getValidatedPCTwig(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", condition = "")
    val flat_key = twig.createNode("flat_key", condition = "")
    val nested_obj = twig.createNode("nested_obj", condition = "")
    twig = twig.createEdge(root, flat_key, false)
    twig = twig.createEdge(root, nested_obj, false)
    twig.validate.get
  }

  def getValidatedPCTwigWithCondition(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val flat_key = twig.createNode("flat_key", condition = "1_1_flat_val")
    val nested_obj = twig.createNode("nested_obj")
    twig = twig.createEdge(root, flat_key, false)
    twig = twig.createEdge(root, nested_obj, false)
    twig.validate.get
  }

  def getValidatedADTwig(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val nested_obj = twig.createNode("nested_obj")
    twig = twig.createEdge(root, nested_obj, true)
    twig.validate.get
  }

  def getValidatedCollectionTwig(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val flat_list = twig.createNode("flat_list")
    val element = twig.createNode("element", 1, 2)
    twig = twig.createEdge(root, flat_list, false)
    twig = twig.createEdge(flat_list, element, false)
    twig.validate.get
  }

  def getValidatedCollectionTwigWithCondition(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val flat_list = twig.createNode("flat_list")
    val element = twig.createNode("element", 1, 2, "1_list_val_1_x")
    twig = twig.createEdge(root, flat_list, false)
    twig = twig.createEdge(flat_list, element, false)
    twig.validate.get
  }

  def getValidatedDoublyNestedCollectionTwigWithCondition(): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root")
    val nested_list = twig.createNode("nested_list", 1, 1000)
    val element1 = twig.createNode("element", 1, 1000)
    val element2 = twig.createNode("element", 1, 1000, "2_list_val_1_x")
    twig = twig.createEdge(root, nested_list, false)
    twig = twig.createEdge(nested_list, element1, false)
    twig = twig.createEdge(element1, element2, false)
    twig.validate.get
  }


  def getDataFrameAndSchemaMatches(twig: Twig = getValidatedPCTwig()): (Dataset[_], Seq[SchemaMatch]) = {
    val df = getDataFrame()
    val schema = new Schema(df)
    val schemaMatcher = SchemaMatcher(twig, schema)
    val candidates = schemaMatcher.getCandidates()
    (df, candidates)
  }


  test("multiple register of udf") {

    val dfAndMatches = getDataFrameAndSchemaMatches()
    val annotatedData = dfAndMatches._1

    val dataFetcher1 = DataFetcher(annotatedData, dfAndMatches._2(0)).getItems()
    val dataFetcher2 = DataFetcher(annotatedData, dfAndMatches._2(0)).getItems()


  }

  test("simple data fetcher test with single match") {
    val dfAndMatches = getDataFrameAndSchemaMatches()
    val annotatedData = dfAndMatches._1

    val dataFetcher1 = DataFetcher(annotatedData, dfAndMatches._2(0))
    val res = dataFetcher1.getItems()
    assert(dfAndMatches._2.size == 1)
    assert(res.count() == 4)
    assert(res.count == 4)
  }

  test("simple data fetcher test with two matches") {
    val dfAndMatches = getDataFrameAndSchemaMatches(getValidatedADTwig())
    val annotatedData = dfAndMatches._1

    val dataFetcher1 = DataFetcher(annotatedData, dfAndMatches._2(0))
    val res = dataFetcher1.getItems()
    assert(dfAndMatches._2.size == 2)
    assert(res.count() == 4) // used to be more matches than one, thus number may be smaller
  }

  test("simple data fetcher with condition") {
    val dfAndMatches = getDataFrameAndSchemaMatches(getValidatedPCTwigWithCondition())
    val annotatedData = dfAndMatches._1

    val dataFetcher1 = DataFetcher(annotatedData, dfAndMatches._2(0))
    val res = dataFetcher1.getItems()
    assert(dfAndMatches._2.size == 1)
    assert(res.count() == 1)
  }

  test("data fetcher on nested data") {
    val dfAndMatches = getDataFrameAndSchemaMatches(getValidatedCollectionTwig())
    val annotatedData = dfAndMatches._1

    val dataFetcher1 = DataFetcher(annotatedData, dfAndMatches._2(0))
    val res = dataFetcher1.getItems()
    assert(dfAndMatches._2.size == 1)
    assert(res.count() == 4)
  }

  test("data fetcher on nested data with condition") {
    val dfAndMatches = getDataFrameAndSchemaMatches(getValidatedCollectionTwigWithCondition())
    val annotatedData = dfAndMatches._1

    val dataFetcher1 = DataFetcher(annotatedData, dfAndMatches._2(0))
    val res = dataFetcher1.getItems()
    assert(dfAndMatches._2.size == 1)
    assert(res.count() == 2)
  }


  test("data fetcher on multiply nested data with condition") {
    val dfAndMatches = getDataFrameAndSchemaMatches(getValidatedDoublyNestedCollectionTwigWithCondition())
    val annotatedData = dfAndMatches._1

    val dataFetcher1 = DataFetcher(annotatedData, dfAndMatches._2(0))
    val res = dataFetcher1.getItems()
    assert(dfAndMatches._2.size == 1)
    //TODO solution does not work multiple nested lists -- should not be a problem in this context
    assert(res.count() == 2)
  }
}


