import ga.{LB, UB}

object PinitialPopulation {

  def calTzb(x: Array[Double], yingliK: Array[Array[Double]], zaihe: Array[Array[Double]]): Array[Array[Double]] = {
    val ylength = yingliK.length
    val zlength = zaihe(0).length
    val Tzb = Array.ofDim[Double](ylength, zlength)
    var i, j = 0
    for (i <- 0 until ylength) {
      for (j <- 0 until zlength) {
        Tzb(i)(j) = yingliK(i)(0) * zaihe(0)(j) * x(0) + yingliK(i)(1) * zaihe(1)(j) * x(1) + yingliK(i)(2) * zaihe(2)(j) * x(2) +
          yingliK(i)(3) * zaihe(3)(j) * x(3) + yingliK(i)(4) * zaihe(4)(j) * x(4) + yingliK(i)(5) * zaihe(5)(j) * x(5) +
          yingliK(i)(6) * zaihe(6)(j) * x(6) + yingliK(i)(7) * zaihe(7)(j) * x(7) + yingliK(i)(8) * zaihe(8)(j) * x(8) +
          yingliK(i)(9) * zaihe(9)(j) * x(9)
      }
    }
    Tzb
  }

  def initialPopulation(initArray: (Int, Array[Double]), yingliK: Array[Array[Double]], zaihe: Array[Array[Double]], Dysum: Array[Double], dt: Array[Double]): (Int, (Array[Double], Double)) = {
    val length = initArray._2.length
    var populationArray = new Array[Double](length)
    var Tzb = Array.ofDim[Double](yingliK.length, zaihe(0).length)
    var i = 0

    var tmp_fit = 100.0
    while (tmp_fit > 1.0) {
      for (i <- 0 until length) {
        var value = Math.random()
        populationArray(i) = (UB(i) - LB(i)) * value + LB(i)
      }
      Tzb = calTzb(populationArray, yingliK, zaihe)
      tmp_fit = fitness.fitnessFcn(Tzb, populationArray, yingliK, zaihe, Dysum, dt)
    }
    (initArray._1, (populationArray, tmp_fit))
  }

}
