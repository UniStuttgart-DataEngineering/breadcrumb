package de.uni_stuttgart.ipvs.provenance.evaluation

object TestConfiguration {

  private def toInt(s: String):Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: NumberFormatException => None
    }
  }

  //0 -> reference
  //1 -> size
  //2 -> repetitions
  //3 -> warmup
  //4 -> testMask
  //5 -> dataPath

  def apply(parameters: Array[String]) : TestConfiguration = {
    val testConfiguration = new TestConfiguration()
    testConfiguration._referenceScenario = parameters(0) match {
      case "true" => true
      case "false" => false
      case _ => throw new MatchError("Reference query flag must be \"true\" or \"false\"")
    }

    testConfiguration._dataSize = parameters(1) match {
      case  "100" => 100
      case  "200" => 200
      case  "300" => 300
      case  "400" => 400
      case  "500" => 500
      case _ => throw new MatchError("Data Size must be either of the values [100|200|300|400|500]")
    }
    val iterations = toInt(parameters(2))
    testConfiguration._iterations = iterations match {
      case Some(n) => n
      case _ => throw new MatchError("Repetitions must be between 1 and including 10")
    }

    testConfiguration._warmUp = parameters(3) match {
      case "true" => true
      case "false" => false
      case _ => throw new MatchError("Warmup flag must be \"true\" or \"false\"")
    }

    val testMask = toInt(parameters(4))
    testConfiguration._testMask = testMask match {
      case Some(n) => n
      case _ => throw new MatchError("TestMask should be Integer")
    }

    testConfiguration._pathToData = parameters(5)
    testConfiguration.build()
  }

  def default(pathToData: String) = {
    val testConfiguration = new TestConfiguration()
    testConfiguration._pathToData = pathToData
    testConfiguration._dataSize = -1
    testConfiguration._testMask = 127
    testConfiguration.build()
  }

}

class TestConfiguration {

  private var _referenceScenario: Boolean = false
  private var _pathToData : String = "/"
  private var _iterations : Int = 1
  private var _warmUp: Boolean = false
  private var _dataSize : Int = 100
  private var _testMask = 0

  private var _built = false




  def getZeros(): String = {
    _dataSize match {
      case 100 => "000000"
      case 200 => "00000"
      case 300 => "0000"
      case 400 => "000"
      case 500 => "00"
      case -1 => ""
      case _ => throw new RuntimeException("Something went wrong converting the input dataSize to zeros")
    }
  }


  def referenceScenario = _referenceScenario

  def referenceScenario(boolean: Boolean) : TestConfiguration = {
    if (!_built) _referenceScenario = boolean
    this
  }


  def pathToData = _pathToData

  def pathToData(path: String) : TestConfiguration = {
    if (!_built) _pathToData = path
    this
  }

  def iterations = _iterations

  def iterations(it: Int) : TestConfiguration = {
    if (!_built) _iterations = it
    this
  }

  def warmUp = _warmUp

  def warmUp(warmUp:Boolean) : TestConfiguration = {
    if (!_built) _warmUp = warmUp
    this
  }

  def testMask(testMask: Int): TestConfiguration = {
    if (!_built) _testMask = testMask
    this
  }

  def testMask = _testMask

  def dataSize = _dataSize

  def dataSize(dataSize:Int) : TestConfiguration = {
    if (!_built) _dataSize = dataSize
    this
  }

  def build() : TestConfiguration = {
    _built = true
    this
  }

}


