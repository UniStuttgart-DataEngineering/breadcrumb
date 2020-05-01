package de.uni_stuttgart.ipvs.provenance.why_not_question

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import de.uni_stuttgart.ipvs.provenance.SharedSparkTestDataFrames
import org.scalatest.FunSuite

class SchemaSuite extends FunSuite with SharedSparkTestDataFrames with DataFrameComparer {

  test("nestedData_moreComplex.json"){
    val df = spark.read.json(pathToNestedData2)
    val testSchema = new Schema(df)
    assert(testSchema.labeles.size == 10)
    assert(testSchema.nameStreams.getOrElse("element",Seq.empty[String]).size == 3)
  }

  test("trailing node ids"){
    val df = spark.read.json(pathToNestedData2)
    val testSchema = new Schema(df)
    val res = testSchema.getTrailingNodeIDs("0.1.3", "0.1.3.5.0")
    assert(res === ("5" :: "0" :: Nil).toArray)
  }

  test("nestedData_deep.json"){
    val df = spark.read.json(pathToDoc2)
    val testSchema = new Schema(df)
    df.printSchema()
    assert(testSchema.labeles.size == 10)
    assert(testSchema.nameStreams.getOrElse("element",Seq.empty[String]).size == 0)
    assert(testSchema.nameStreams.getOrElse("a",Seq.empty[String]).size == 3)
  }

}
