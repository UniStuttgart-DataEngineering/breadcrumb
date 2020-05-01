package de.uni_stuttgart.ipvs.provenance

import org.apache.spark.sql.DataFrame

trait SharedSparkTestDataFrames extends SharedSparkTestInstance {

  import spark.implicits._

  protected val baseDir = "src/main/resources/"

  protected val pathToNestedData1 = baseDir + "nestedData.json"
  protected val pathToNestedData2 = baseDir + "nestedData_moreComplex.json"
  protected val pathToNestedData3 = baseDir + "nestedData_withKeyDuplicates.json"
  protected val pathToNestedData4 = baseDir + "nestedData_moreComplex_renamed.json"
  protected val pathToNestedData5 = baseDir + "nestedData_renamedKeys.json"
  protected val pathToNestedData6 = baseDir + "nestedData_deepNested.json"
  protected var pathToNestedData7 = baseDir + "orders.json"
  protected var pathToNestedData8 = baseDir + "countMinMax.json"
  protected val pathToTweetsDummy = baseDir + "tweets_dummy.json"
  protected val pathToDoc1 = baseDir + "doc1.json"
  protected val pathToDoc2 = baseDir + "doc2.json"
  protected val pathToDoc3 = baseDir + "doc3.json"
  protected val pathToDoc4 = baseDir + "doc4.json"
  protected val pathToDemoData = baseDir + "demo.json"

  def singleInputColumnDataFrame(): DataFrame = {
    Seq(1, 2, 3, 4, 5, 6).toDF("MyIntCol")
  }


  def getDataFrame(): DataFrame = {
    val initial: DataFrame = spark.read.json(pathToNestedData2)
    initial.filter(initial.col("flat_key") isNotNull)
  }



}
