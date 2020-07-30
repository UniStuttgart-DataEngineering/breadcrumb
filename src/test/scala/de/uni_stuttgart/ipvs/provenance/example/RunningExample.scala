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

  import spark.implicits._

  def runningExample(): DataFrame = {
    var exampleData = getExampleDataFrame()
    exampleData = exampleData.withColumn("address", explode($"address2"))
    exampleData = exampleData.filter($"address.year" >= 2019)
    exampleData = exampleData.select($"name", $"address.city")
    exampleData.groupBy( $"city").agg(collect_list($"name").alias("nList"))
  }

  def runningExampleShortened(): DataFrame = {
    val exampleData = getExampleDataFrame()
    exampleData.withColumn("address", explode($"address2"))
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


  test("Running example without rewrite") {
    val exampleData = runningExample()
    exampleData.show()
  }

  test("Running example with rewrite") {
    val exampleData = runningExample()
    val wnTuple = exampleWhyNotTupleRefined()
    val rewrittenData = WhyNotProvenance.rewrite(exampleData, wnTuple)
    exampleData.show()
    rewrittenData.show()
    rewrittenData.printSchema()
  }

  test("Flatten example with rewrite") {
    val exampleData = runningExample()
    val wnTuple = exampleWhyNotTupleShortened()
    val rewrittenData = WhyNotProvenance.rewrite(runningExampleShortened, wnTuple)
    exampleData.show()
    rewrittenData.show()
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

}
