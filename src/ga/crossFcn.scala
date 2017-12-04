import ga.{crossRate, chromosomeSize}

object crossFcn {
  def cross(id : Int,x: (Array[Double], Array[Array[Double]], Double), y: (Array[Double], Array[Array[Double]], Double)): (Int, (Array[Double], Array[Array[Double]], Double)) = {
    val pick1 = Math.random()
    var i = 0
    if (pick1 < crossRate) {
      for (i <- 0 until chromosomeSize) {
        val pick2 = (new util.Random).nextInt()
        if (pick2 % 2 == 0) {
          val tmp = x._1(i)
          x._1(i) = y._1(i)
          y._1(i) = tmp
        }
      }
    }
    (id,x)
  }
}
