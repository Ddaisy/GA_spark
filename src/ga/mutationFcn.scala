import ga.{LB, UB, chromosomeSize, maxGeneration, mutationRate, populationSize}
import org.apache.spark.TaskContext

object mutationFcn {
  def mutation(x: (Int, (Array[Double], Double))): (Int, (Array[Double], Double)) = {
    var scale = 0.5
    val shrink = 0.75
    var i = 0
    for (i <- 0 until populationSize) {
      scale -= scale * shrink * i / maxGeneration
    }
    val pick1 = Math.random()
    if (pick1 < mutationRate) {
      for (i <- 0 until chromosomeSize) {
        val pick2 = (new util.Random).nextInt()
        if (pick2 % 2 == 0) {
          var tmp = 0.0
          do {
            val pick3 = Math.random() / Int.MaxValue * 2 - 1
            tmp = x._2._1(i) + scale * (UB(i) - LB(i)) * pick3
          } while (tmp > UB(i) || tmp < LB(i))
          x._2._1(i) = tmp
        }
      }
    }
    val task = TaskContext.get
    val stageId = task.stageId()
    val parId = task.partitionId()
    val taskId = task.taskAttemptId()
    println(s"--------mutation : stageID:$stageId\t partitionID:$parId\t taskID:$taskId--------------")
    x
  }
}
