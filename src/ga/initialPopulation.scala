import org.apache.spark.rdd.RDD

object initialPopulation {

  val LB = Array(0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5)
  val UB = Array(5, 4, 5, 4, 5, 5, 5, 5, 5, 4)

  def initialPopulation(initArray: Array[Double]): Array[Double] = {
    val length = initArray.length
    var populationArray = new Array[Double](length)
    var i = 0

    var tmp_fit = 100.0
    while (tmp_fit > 99) {
      for (i <- 0 until length) {
        var value = Math.random()
        populationArray(i) = (UB(i) - LB(i)) * value + LB(i)
      }
      tmp_fit = fitness.fitnessFcn(populationArray)
    }
    populationArray
  }
}
