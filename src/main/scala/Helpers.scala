object Helpers {
  // Random generator
  val random = new scala.util.Random

  def randomLong() = {
    random.nextLong()
  }

  def serializeTuple(tuple: (Int, Int)): String = {
    tuple._1.toString + "," + tuple._2.toString
  }

  def deserializeTuple(str: String): (Int, Int) = {
    val splits = str.split(',')
    (splits(0).toInt, splits(1).toInt)
  }
}
