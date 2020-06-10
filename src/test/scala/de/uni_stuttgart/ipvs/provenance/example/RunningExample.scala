package de.uni_stuttgart.ipvs.provenance.example

import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import de.uni_stuttgart.ipvs.provenance.nested_why_not.WhyNotProvenance
import de.uni_stuttgart.ipvs.provenance.why_not_question.Twig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{collect_list, explode}
import org.scalatest.FunSuite

class RunningExample extends FunSuite with SharedSparkTestDataFrames {

  protected val pathToExampleData = baseDir + "exampleData.json"

  def getExampleDataFrame(): DataFrame = getDataFrame(pathToExampleData)

  import spark.implicits._

  def runningExample() = {
    var exampleData = getExampleDataFrame()
    exampleData = exampleData.withColumn("address", explode($"address2"))
    exampleData = exampleData.filter($"address.year" === 2019)
    exampleData = exampleData.select($"name", $"address.city")
    exampleData.groupBy( $"city").agg(collect_list($"name").alias("nList"))
  }

  def whyNotTupleFilterNewName(newName: String): Twig = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val user = twig.createNode(newName, 1, 1, "")
    twig = twig.createEdge(root, user, false)
    twig.validate().get
  }

  def exampleWhyNotTuple() = {
    var twig = new Twig()
    val root = twig.createNode("root", 1, 1, "")
    val user = twig.createNode("city", 1, 1, "NY")
    twig = twig.createEdge(root, user, false)
    twig.validate().get

  }


  test("Running example without rewrite") {
    val exampleData = runningExample()
    exampleData.show()
  }

  test("Running example with rewrite") {
    val exampleData = runningExample()
    val wnTuple = exampleWhyNotTuple()
    val rewrittenData = WhyNotProvenance.rewrite(exampleData, wnTuple)
    exampleData.show()
    rewrittenData.show()
  }

}
