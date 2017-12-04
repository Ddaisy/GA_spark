/**
  * Created by daisy on 11/7/17.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object ga {

  val LB = Array(0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5)
  val UB = Array(5, 4, 5, 4, 5, 5, 5, 5, 5, 4)

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


    var initArray = sc.parallelize(0 to 99).map(x => (x, new Array[Double](10)))
    //populationArray : RDD[(id:Int, (x:Array[Double], Tzb : Array[Array[Double]], fitness: Double))]
    var populationArray = initArray.mapValues(x => initialPopulation.initialPopulation(x, yingliK.value, zaihe.value, Dysum.value, dt))

    //var fit = populationArray.mapValues(x => fitness.fitnessFcn(x._2, x._1, yingliK.value, zaihe.value, Dysum.value, dt))
    //bestFit : (Int,(Array[Double], Array[Array[Double]],Double))
    var bestPop = populationArray.min()(new Ordering[(Int,(Array[Double], Array[Array[Double]],Double))] {
      def compare(x: (Int,(Array[Double], Array[Array[Double]],Double)), y: (Int,(Array[Double], Array[Array[Double]],Double))): Int = x._2._3 compare y._2._3
    })

    var bestIndex = bestPop._1
    var bestFit = bestPop._2._3
    var bestX = bestPop._2._1

    



    sc.stop()
  }
}
