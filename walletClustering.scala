import org.apache.spark.mllib.stat.KernelDensity
import org.apache.spark.rdd.RDD

//small test String
//val x = sc.parallelize(Array(("a", "input, 12:00:00, 1"),("a", "output, 12:01:01, 2"),("b", "input, 12:00:32, 3")))

object walletClustering{
	def main(args: Array[String]): Unit = {
		/*data = open file from s3 load into rdd
		pairData = data.split(", ") key = address (3rd item)
		pairData.
		depending upon how much this reduces we may want to write pairData out to s3 and split the program here

		while(not clustered){
			//get distances from each point to each point
			points.cartesian(points).map(distances)
			//generate kernal from distances

			//apply shift based upon kernal
		}

		write clusters to file
		write key pair adress and cluster to file
		*/
	}
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
def distances(value:((String, Array[Double]), Array[Double])): (String, Array[Array[Double], Array[Double])={
	val source = value._0._1
	val target = value._1
	val dist = scala.math.sqrt(source.zip(target).map{ case (x, y) => scala.math.pow(y-x, 2)}.sum)
	return (value._0._0, (dist, target))
}

//inputs point [inputTotal, outputTotal, numTransactions], RDD original points
//ouputs point [inputTotal, outputTotal, numTransactions]
def shiftFunc(val:Array[((String, Array[Double]), Array[Double])], kernal:KernelDensity): Array[Double]={
	//find neighbors with euclidean distance
	val threshold = 10
	//neighbors is key distance value arr[dbl]
	val neighbors = points.map{point => (distance(point, p), point)}

	//.flatMap{case(distance, value) => if(distance < threshold) (distance, value)}
	//create kernal from samples
	//todo is kernal from distances?
	val kernel = new KernelDensity().setSample(neighbors.keys}).setBandwidth(1.0)
	val adjusted = neighbors.map{case(key, value) => (v, value.zip(kernelFunc(Array(key), kernel)).map{case(x, y) => x*y})}
	return adjusted
}

def kernelFunc(distance:Array[Double], kernal:KernelDensity): Array[Double]={
	kernal.estimate(distance)
}

def distance(source:Array[Double], target:Array[Double]): Double ={
	return 
}

/*
def notClustered{
	what do we need for this old and new point locations
	can we pull out points that have stopped moving?
}*/
