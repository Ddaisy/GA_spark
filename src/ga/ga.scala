/**
  * Created by daisy on 11/7/17.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object ga {

  val LB = Array(0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5)
  val UB = Array(5, 4, 5, 4, 5, 5, 5, 5, 5, 4)
  val populationSize = 100
  val chromosomeSize = 10
  val crossRate = 0.8
  val mutationRate = 0.01
  val maxGeneration = 500
  var G = 0
  var fval = 100.0
  var bestFit = 0.0
  var bestX = new Array[Double](chromosomeSize)

  def transFormat(s: String): Array[Double] = {
    s.replace(" ", "\t").split("\t").map(_.toDouble)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GAtest")
    val sc = new SparkContext(conf)

    val sigy = sc.textFile("hdfs://10.131.247.181:9000/user/hadoop/test.txt").map(transFormat)
    val dty = sc.textFile("hdfs://10.131.247.181:9000/user/hadoop/testa.txt").map(transFormat).collect().flatten

    val initDysum = preDysum.testPreDysum(sigy, dty).collect()
    val Dysum = sc.broadcast(initDysum)

    val a = sc.textFile("hdfs://10.131.247.181:9000/user/hadoop/Tzaihe_sig.txt").map(transFormat).collect()
    val aa = sc.textFile("hdfs://10.131.247.181:9000/user/hadoop/yingliK.txt").map(transFormat).collect()
    val dt = sc.textFile("hdfs://10.131.247.181:9000/user/hadoop/Tzaihe_dt.txt").map(transFormat).collect().flatten

    val zaihe = sc.broadcast(a)
    val yingliK = sc.broadcast(aa)


    var initArray = sc.parallelize(0 until populationSize).map(x => (x, new Array[Double](chromosomeSize)))
    //populationArray : RDD[(id:Int, (x:Array[Double], Tzb : Array[Array[Double]], fitness: Double))]
    var populationArray = initArray.mapValues(x => initialPopulation.initialPopulation(x, yingliK.value, zaihe.value, Dysum.value, dt))

    var i = 0
    for (i <- 0 until maxGeneration) {
      var bestPop = populationArray.min()(new Ordering[(Int, (Array[Double], Array[Array[Double]], Double))] {
        def compare(x: (Int, (Array[Double], Array[Array[Double]], Double)), y: (Int, (Array[Double], Array[Array[Double]], Double))): Int = x._2._3 compare y._2._3
      })

      bestFit = bestPop._2._3

      if (bestFit < fval) {
        fval = bestFit
        bestX = bestPop._2._1
      }

      if (i != (maxGeneration - 1)){
        var selectPop = populationArray.sortBy(x => x._2._3)
        var initCrossPop = selectPop.map(x => ((x._1 + 1) % 100, x._2))
        var crossPop = selectPop.join(initCrossPop).map(x => crossFcn.cross(x._1, x._2._1, x._2._2))
        var mutationPop = crossPop.map(x => mutationFcn.mutation(x))

        populationArray = mutationPop.map(x => fitness.updateFit(x, yingliK.value, zaihe.value, Dysum.value, dt))
      }
    }

    println(fval)
    bestX.foreach(println)


    sc.stop()
  }
}
