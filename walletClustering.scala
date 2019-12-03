//small test String
//val x = sc.parallelize(Array(("a", "input, 12:00:00, 1"),("a", "output, 12:01:01, 2"),("b", "input, 12:00:32, 3")))

object walletClustering{
	def main(args: Array[String]): Unit = {
		/*data = open file from s3 load into rdd
		pairData = data.split(", ") key = address (3rd item)
		pairData.mapValues(joinPrep).reduceByKey(joinFunc)
		depending upon how much this reduces we may want to write pairData out to s3 and split the program here

		while(not clustered){
			pairData.flatMap(shiftFunc)
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
/*
def shiftFunc{
	tempValues = values
	find neighbors within defined distance using euclidean distance
	forEach(neighbor){
		weight = guassianKernalFunct(distance, bandwidth)
		forEach(i in values.length){
			tempValues += neighbor.value[i] * wieght
		}
	}
}

def guassianKernalFunc{
	use already built kernals from mlib
	https://spark.apache.org/docs/2.2.1/api/java/org/apache/spark/mllib/stat/KernelDensity.html
}

def notClustered{
	what do we need for this old and new point locations
	can we pull out points that have stopped moving?
}*/
