package de.uni_stuttgart.ipvs.provenance.evaluation

import java.io.IOException

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

class EvaluationResult(spark: SparkSession, testConfiguration: TestConfiguration) {

  /*
  @throws[IOException]
  def writeResultToHDFS(hdfsPath: String, content: String, fs: FileSystem) {
    val path: Path = new Path(hdfsPath)
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
    val dataOutputStream: FSDataOutputStream = fs.create(path)
    val bw: BufferedWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream, "UTF-8"))
    bw.write(content)
    bw.close
  }


  hfdsPath und content sind dabei klar, fs ist wie folgt zu setzen

  val spark = SparkSession.builder()
    .appName("interact-finding-k")
    .getOrCreate()
  val sc = spark.sparkContext
  val fs = FileSystem.get(sc.hadoopConfiguration)

   */

}

