/**
  * Created by daisy on 11/7/17.
  */

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object ga {

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

  var resArray = List(Int, (new Array[Double](chromosomeSize), Double))

  def transFormat(s: String): Array[Double] = {
    s.replace(" ", "\t").split("\t").map(_.toDouble)
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("GAtest")
    val sc = new SparkContext(conf)

    val sigy = sc.textFile("hdfs://10.141.208.44:9000/ddy/BTyingli_sigy.txt").map(transFormat)
    val dty = sc.textFile("hdfs://10.141.208.44:9000/ddy/BTyingli_dty.txt").map(transFormat).collect().flatten

    val initDysum = preDysum.testPreDysum(sigy, dty).collect()
    val Dysum = sc.broadcast(initDysum)

    val a = sc.textFile("hdfs://10.141.208.44:9000/ddy/BTzaihe_sig.txt").map(transFormat).collect()
    val aa = sc.textFile("hdfs://10.141.208.44:9000/ddy/yingliK.txt").map(transFormat).collect()
    val dt = sc.textFile("hdfs://10.141.208.44:9000/ddy/BTzaihe_dt.txt").map(transFormat).collect().flatten

    val zaihe = sc.broadcast(a)
    val yingliK = sc.broadcast(aa)

    var initArray = sc.parallelize(0 until populationSize, 10).map(x => (x, new Array[Double](chromosomeSize)))

    //populationArray : RDD[(id:Int, (x:Array[Double], fitness: Double))]
    var populationArray = initArray.mapValues(x => initialPopulation.initialPopulation(x, yingliK.value, zaihe.value, Dysum.value, dt))
    var res = populationArray

    var i = 1
    for (i <- 1 to maxGeneration) {

      if (i != (maxGeneration - 1)) {
        //var selectPop = populationArray.sortBy(x => x._2._2)
        var initCrossPop = populationArray.map(x => ((x._1 + 1) % 100, x._2))
        var crossPop = populationArray.join(initCrossPop).map(x => crossFcn.cross(x._1, x._2._1, x._2._2))
        var mutationPop = crossPop.map(x => mutationFcn.mutation(x))
        populationArray = mutationPop.map(x => fitness.updateFit(x, yingliK.value, zaihe.value, Dysum.value, dt))
      }

      if(i % 10 == 0){
        res = res.mapPartitions{x => {
          var pop = List[(Int,(Array[Double],Double))]()
          var tmpPop = (0,(new Array[Double](chromosomeSize), Double.MaxValue))
          while(x.hasNext){
            var tt = x.next()
            if(tt._2._2 < tmpPop._2._2)
              tmpPop = tt
          }
          pop.::(tmpPop).iterator
        }}
      }else{
        res = res.++(populationArray).coalesce(10).localCheckpoint()
      }
    }

    var best = res.mapPartitions{x => {
      var pop = List[(Array[Double],Double)]()
      var tmpPop = (new Array[Double](chromosomeSize), Double.MaxValue)
      while(x.hasNext){
        var tt = x.next()
        if(tt._2._2 < tmpPop._2)
          tmpPop = tt._2
      }
      pop.::(tmpPop).iterator
    }}

    var bestPop = best.min()(new Ordering[(Array[Double], Double)] {
          def compare(x: (Array[Double], Double), y: (Array[Double], Double)): Int = x._2 compare y._2
        })

    fval = bestPop._2
    bestX = bestPop._1


    println(fval)
    bestX.foreach(println)


    sc.stop()
  }
}
