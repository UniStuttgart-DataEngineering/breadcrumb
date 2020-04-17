package de.uni_stuttgart.ipvs.provenance.nested_why_not

import de.uni_stuttgart.ipvs.provenance.SharedSparkTestInstance
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.FunSuite
import org.scalatest.Assertions._

class WhyNotProvenanceTest extends FunSuite with SharedSparkTestInstance{

  import spark.implicits._

  def inputDataFrame(): DataFrame = {
    Seq(1, 2, 3, 4, 5, 6).toDF("MyIntCol")
  }

  test("The first test, fail fast, fail often") {
    var df = inputDataFrame()
    df = df.filter($"MyIntCol" > 2)
    //df.explain(true)
    df = WhyNotProvenance.annotate(df)
    //df.explain(true)
    df.show
    val res = df.collect()
    assert(res.length == 4)
  }

}
