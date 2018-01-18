/**
  * Created by daisy on 11/7/17.
  */

import ga.chromosomeSize
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object Pga {

  val LB = Array(0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5)
  val UB = Array(5, 4, 5, 4, 5, 5, 5, 5, 5, 4)
  val populationSize = 100
  val chromosomeSize = 10
  val crossRate = 0.8
  val mutationRate = 0.01
  val maxGeneration = 100
  var G = 0
  var fval = 100.0
  var bestFit = 0.0
  var bestX = new Array[Double](chromosomeSize)

  var resArray = List[(Int, (Array[Double], Double))]()

  def transFormat(s: String): Array[Double] = {
    s.replace(" ", "\t").split("\t").map(_.toDouble)
  }

  def minOfPartition(x: Iterator[(Int,(Array[Double],Double))]) : Iterator[(Int,(Array[Double],Double))] = {
    var pop = List[(Int, (Array[Double], Double))]()
    var tmpPop = (0,(new Array[Double](chromosomeSize), Double.MaxValue))
    while(x.hasNext){
      var tt = x.next()
      if(tt._2._2 < tmpPop._2._2)
        tmpPop = tt
    }
    pop.::(tmpPop).iterator
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GAtest")
    val sc = new SparkContext(conf)

    val sigy = sc.textFile("hdfs://10.141.208.44:9000/ddy/Tyingli_sigy.txt").map(transFormat)
    val dty = sc.textFile("hdfs://10.141.208.44:9000/ddy/Tyingli_dty.txt").map(transFormat).collect().flatten

    val initDysum = preDysum.testPreDysum(sigy, dty).collect()
    val Dysum = sc.broadcast(initDysum)

    val a = sc.textFile("hdfs://10.141.208.44:9000/ddy/Tzaihe_sig.txt").map(transFormat).collect()
    val aa = sc.textFile("hdfs://10.141.208.44:9000/ddy/yingliK.txt").map(transFormat).collect()
    val dt = sc.textFile("hdfs://10.141.208.44:9000/ddy/Tzaihe_dt.txt").map(transFormat).collect().flatten

    val zaihe = sc.broadcast(a)
    val yingliK = sc.broadcast(aa)

    var initArray = sc.parallelize(0 until populationSize, 10).map(x => (x, new Array[Double](chromosomeSize)))

    //populationArray : RDD[(id:Int, (x:Array[Double], fitness: Double))]
    //var populationArray = initArray.mapValues(x => initialPopulation.initialPopulation(x, yingliK.value, zaihe.value, Dysum.value, dt))
    var populationArray = initArray.mapPartitions { x => {
        var pop = List[(Int, (Array[Double], Double))]()
        var tmpPop = (0, (new Array[Double](chromosomeSize), Double.MaxValue))
        while (x.hasNext) {
          val tt = x.next()
          tmpPop = PinitialPopulation.initialPopulation(tt, yingliK.value, zaihe.value, Dysum.value, dt)
          pop.::=(tmpPop)
        }
        pop.reverseIterator
      }
    }.cache()
    var crossPop = populationArray
    var mutationPop = populationArray

    var res = populationArray.mapPartitions(x => minOfPartition(x))
    var tmp = res



    var i = 1
    for (i <- 1 to maxGeneration) {

      if (i != (maxGeneration - 1)) {
        //var selectPop = populationArray.sortBy(x => x._2._2)
        //var initCrossPop = populationArray.map(x => ((x._1 + 1) % 100, x._2))
        //var crossPop = populationArray.join(initCrossPop).map(x => crossFcn.cross(x._1, x._2._1, x._2._2))
        //var crossPop = populationArray.map(x => ((x._1 + 1) % 100, x._2)).join(populationArray).map(x => crossFcn.cross(x._1, x._2._1, x._2._2))

        crossPop = populationArray.mapPartitions(x => {
          val pop = x.toArray
          var pop1 = List[(Int,(Array[Double],Double))]()
          var tmpPop = (0,(new Array[Double](chromosomeSize), Double.MaxValue))
          var i = 0
          for(i <- pop.indices){
            if(i % 2 == 0 && i != pop.length - 1){
              tmpPop = crossFcn.cross(pop(i)._1, pop(i)._2, pop(i + 1)._2)
            } else {
              tmpPop = pop(i)
            }
            pop1.::=(tmpPop)
          }
          pop1.reverseIterator
        })
        mutationPop = crossPop.map(x => mutationFcn.mutation(x))
        populationArray = mutationPop.map(x => fitness.updateFit(x, yingliK.value, zaihe.value, Dysum.value, dt))
      }

//      if(i % 10 == 0){
        tmp = populationArray.mapPartitions(x => minOfPartition(x))
        res = res.++(tmp).coalesce(10)
//      }else{
//        res = res.++(populationArray).coalesce(10)
//      }
    }

    var bestPop = res.min()(new Ordering[(Int, (Array[Double], Double))] {
      def compare(x: (Int, (Array[Double], Double)), y: (Int, (Array[Double], Double))): Int = x._2._2 compare y._2._2
    })

    //var bestPop = res.collect().minBy(_._2._2)
    populationArray.unpersist()

    fval = bestPop._2._2
    bestX = bestPop._2._1


    println(fval)
    bestX.foreach(println)


    sc.stop()
  }
}
