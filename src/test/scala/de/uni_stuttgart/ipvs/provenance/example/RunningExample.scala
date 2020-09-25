package de.uni_stuttgart.ipvs.provenance.example

import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.WhyNotProvenance
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{collect_list, explode}
import org.scalatest.FunSuite

class RunningExample extends FunSuite with SharedSparkTestDataFrames {

//  protected val pathToExampleData = baseDir + "exampleData.json"

  def getExampleDataFrame(): DataFrame = getDataFrame(pathToExampleData)
  def getExampleDataFrameFromPaper(): DataFrame = getDataFrame(pathToExampleDataPaper)

  import spark.implicits._

  def runningExample(input: DataFrame = getExampleDataFrame()): DataFrame = {
    var exampleData = input
    //exampleData.show(false)
    exampleData = exampleData.withColumn("address", explode($"address2"))
    exampleData = exampleData.filter($"address.year" >= 2019)
    exampleData = exampleData.select($"name", $"address.city")
    exampleData.groupBy($"city").agg(collect_list($"name").alias("nList"))
  }

  def runningExampleShortened(): DataFrame = {
    val exampleData = getExampleDataFrame()
    exampleData.show(false)
    exampleData.withColumn("address", explode($"address2"))
  }

  def runningExampleShortened2(input: DataFrame = getExampleDataFrame()): DataFrame = {
    var exampleData = input
    exampleData.show(false)
    exampleData = exampleData.withColumn("address", explode($"address2"))
    exampleData = exampleData.filter($"address.year" >= 2019)
    exampleData = exampleData.select($"name", $"address.city")
    exampleData
  }

  def exampleWhyNotTuple() = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val user = twig.createNode("city", 1, 1, "NY")
    twig = twig.createEdge(root, user, false)
    twig.validate().get
  }

  def exampleWhyNotTupleRefined() = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val user = twig.createNode("city", 1, 1, "NY")
    val nList = twig.createNode("nList", 1, 1000000, "")
    val element = twig.createNode("element", 1, 1, "")

    twig = twig.createEdge(root, user, false)
    twig = twig.createEdge(root, nList, false)
    twig = twig.createEdge(nList, element, false)
    twig.validate().get
  }

  def exampleWhyNotTupleShortened() = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val address = twig.createNode("address", 1, 1, "")
    val user = twig.createNode("city", 1, 1, "NY")
    twig = twig.createEdge(root, address, false)
    twig = twig.createEdge(address, user, false)
    twig.validate().get
  }

  def exampleWhyNotTupleShortened2() = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
//    val address = twig.createNode("address", 1, 1, "")
    val user = twig.createNode("city", 1, 1, "NY")
//    twig = twig.createEdge(root, address, false)
    twig = twig.createEdge(root, user, false)
    twig.validate().get
  }


  test("Running example without rewrite") {
    val exampleData = runningExample()
    exampleData.show()
    exampleData.explain(true)
  }

  test("Running example paper with schema Alternative MSR") {
    val exampleData = runningExample(getExampleDataFrameFromPaper())
    val wnTuple = exampleWhyNotTuple()
    val rewrittenData = WhyNotProvenance.computeMSRsWithAlternatives(exampleData, wnTuple)
    rewrittenData.show(false)
    rewrittenData.explain()
  }

  test("Running example with rewrite") {
    val exampleData = runningExample()
    val wnTuple = exampleWhyNotTupleRefined()
    val rewrittenData = WhyNotProvenance.rewrite(exampleData, wnTuple)
    exampleData.show()
    rewrittenData.show(false)
    rewrittenData.explain(true)
    rewrittenData.printSchema()
  }

  test("Flatten example with rewrite") {
    val exampleData = runningExampleShortened()
    val wnTuple = exampleWhyNotTupleShortened()
    val rewrittenData = WhyNotProvenance.rewrite(exampleData, wnTuple)
    exampleData.show(false)
    rewrittenData.show(false)
    rewrittenData.explain(true)
    rewrittenData.printSchema()
  }

  test("Project example with rewrite") {
    val exampleData = runningExampleShortened2()
    val wnTuple = exampleWhyNotTupleShortened2()
    val rewrittenData = WhyNotProvenance.rewrite(exampleData, wnTuple)
    exampleData.show(false)
    rewrittenData.show(false)
    rewrittenData.explain(true)
    rewrittenData.printSchema()
  }

  test("Running example with msrComputation") {
    val exampleData = runningExample()
    val wnTuple = exampleWhyNotTuple()
    val rewrittenData = WhyNotProvenance.computeMSRs(exampleData, wnTuple)
    exampleData.show()
    rewrittenData.show()
    rewrittenData.explain(true)
    rewrittenData.printSchema()
  }

  test("Running example with schema Alternative") {
    val exampleData = runningExample()
    val wnTuple = exampleWhyNotTuple()
    val rewrittenData = WhyNotProvenance.rewriteWithAlternatives(exampleData, wnTuple)
    exampleData.show()
    rewrittenData.show(false)
    rewrittenData.explain(true)
    rewrittenData.printSchema()
    //rewrittenData.filter($"__ORIGINAL_0000_0015" === true && $"__VALID_0000_0015" === true).show()
  }

  test("Running example with schema Alternative Simple") {
    val exampleData = runningExampleShortened()
    val wnTuple = exampleWhyNotTupleShortened()
    val rewrittenData = WhyNotProvenance.rewriteWithAlternatives(exampleData, wnTuple)
    exampleData.show()
    rewrittenData.show()
    rewrittenData.explain(true)
    rewrittenData.printSchema()
  }

  test("Running example with schema Alternative Simple 2") {
    val exampleData = runningExampleShortened2()
    val wnTuple = exampleWhyNotTupleShortened2()
    val rewrittenData = WhyNotProvenance.rewriteWithAlternatives(exampleData, wnTuple)
    exampleData.show()
    rewrittenData.show()
    rewrittenData.explain(true)
    rewrittenData.printSchema()
  }

  test("Running example shortened with schema Alternative MSR") {
    val exampleData = runningExampleShortened2()
    val wnTuple = exampleWhyNotTupleShortened2()
    val rewrittenData = WhyNotProvenance.MSRsWithAlternatives(exampleData, wnTuple)
    exampleData.show()
    //rewrittenData.show()
    //rewrittenData.explain(true)
    //rewrittenData.printSchema()
  }

  test("Running example full with schema Alternative MSR") {
    val exampleData = runningExample()
    exampleData.explain(true)
    val wnTuple = exampleWhyNotTuple()
    val rewrittenData = WhyNotProvenance.computeMSRsWithAlternatives(exampleData, wnTuple)
    rewrittenData.show(false)
    rewrittenData.explain()
    //rewrittenData.filter($"__ORIGINAL_0000_0015" === true && $"__VALID_0000_0015" === true).show()
  }

}
