import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.rdd.RDD

//small test String
//val x = sc.parallelize(Array(("a", "input, 12:00:00, 1"),("a", "output, 12:01:01, 2"),("b", "input, 12:00:32, 3")))

object walletClustering{
	def main(args: Array[String]): Unit = {
		/*data = open file from s3 load into rdd
		pairData = data.split(", ") key = address (3rd item)
		pairData.mapValues(joinPrep).reduceByKey(joinFunc)
		depending upon how much this reduces we may want to write pairData out to s3 and split the program here
		*/
		while(not clustered){
			val neighbors = points.cartesian(points.values).map{distances}.filter(v => v._2._1 < threshold)
			val guassians = neighbors.mapValues(kernelFunc)
			val kernels = guassians.mapValues(v => v._1).reduceByKey(kernelReduce)
			val adjusted = guassians.mapValues(v => v._2.map{_*v._1}).reduceByKey(shiftReduce).join(kernels).mapValues{case (arr, weight) => arr.map{_/weight}}
		}
		/*
		write clusters to file
		write key pair adress and cluster to file
		*/
	}

	//inputs either "input"/"output", timestamp, val
	//returns out key = key, value = inputTotal, ouputTotal, numTransactions
	def joinPrep(value:String): Array[Double] ={
		val val_arr = value.split(", ")
		if(val_arr(0) == "input"){
			return Array(val_arr(2).toDouble, 0, 1)
		}
		else{
			return Array(0, val_arr(2).toDouble, 1)
		}
	}

	//inputs key = key, value = inputTotal, ouputTotal, numTransactions
	//returns out key = key, value = inputTotal, ouputTotal, numTransactions
	def joinFunc(accum:Array[Double], value:Array[Double]): Array[Double] ={
		return accum.zip(value).map{case (x,y) => x+y}
	}

	//input cartestian product of points and point values
	//output key, val = (distance, point = arr[dbl])
	def distances(value:((String, Array[Double]), Array[Double])): (String, (Double, Array[Double]))={
		val source = value._1._2
		val target = value._2
		val dist = scala.math.sqrt(source.zip(target).map{ case (x, y) => scala.math.pow(y-x, 2)}.sum)
		return (value._1._1, (dist, target))
	}

	//input key, values distance array points
//output key, values gaussian point
  def kernelFunc(value:(Double, Array[Double])): (Double, Array[Double])={
	  //gaussian function
	  val distance = value._1
	  val target = value._2
	  val bandwith = 5.0
	  val gaussian = 1/scala.math.pow((2*scala.math.Pi)*bandwith, .5) * scala.math.exp(-.5*scala.math.pow(distance,2)/scala.math.pow(bandwith, 2))
	  return (gaussian, value._2)
  }

  def kernelReduce(accum:Double, target:Double): Double={
	  return accum + target
  }

  def shiftReduce(accum:Array[Double], target:Array[Double]): Array[Double]={
	  return accum.zip(target).map{case (x, y) => x+y}
  }

  def notClustered(old:Array[Double], current:Array[Double]): Boolean ={
    for(w <- 0 to 2){
      if(old(w) != current(w)){
        return false
      }
    }
    return true
  }

}