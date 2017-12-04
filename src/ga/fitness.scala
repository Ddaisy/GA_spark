object fitness {
  def calFit(Dysum: Array[Double], Dzsum: Array[Double]): Double = {
    val length = Dysum.length
    var fit = 0.0
    var i = 0
    for (i <- 0 until length) {
      val c = Dysum(i) - Dzsum(i)
      if (c <= 0) {
        fit += Math.pow(c, 2)
      } else {
        fit = 100
      }
    }
    fit
  }

  def fitnessFcn(Tzb: Array[Array[Double]], populationArray: Array[Double], yingliK: Array[Array[Double]], zaihe: Array[Array[Double]], Dysum: Array[Double], dt: Array[Double]): Double = {
    val Dzsum = preDysum.testPreDzsum(Tzb, dt)
    val fit = calFit(Dysum, Dzsum)
    fit
  }
}
