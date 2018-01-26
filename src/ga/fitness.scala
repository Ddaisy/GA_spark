import org.apache.spark.TaskContext

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

  def updateFit(populationArray: (Int, (Array[Double], Double)),yingliK: Array[Array[Double]], zaihe: Array[Array[Double]], Dysum: Array[Double], dt: Array[Double]): (Int, (Array[Double], Double)) = {
    var Tzb = Array.ofDim[Double](yingliK.length, zaihe(0).length)
    var i = 0
    var fit = 0.0
    Tzb = initialPopulation.calTzb(populationArray._2._1, yingliK, zaihe)
    fit = fitnessFcn(Tzb,populationArray._2._1,yingliK,zaihe,Dysum,dt)
    val task = TaskContext.get
    val stageId = task.stageId()
    val parId = task.partitionId()
    val taskId = task.taskAttemptId()
    println(s"--------updateFit : stageID:$stageId\t partitionID:$parId\t taskID:$taskId fit:$fit--------------")
    (populationArray._1,(populationArray._2._1, fit))
  }
}
