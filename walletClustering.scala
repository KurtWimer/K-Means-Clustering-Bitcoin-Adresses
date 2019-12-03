object walletClustering{
	def main(args: Array[String]): Unit = {
		/*data = open file from s3 load into rdd*/
		
		val chainFile = args(0)
    		val spark = SparkSession.builder.appName("BlockChain Work").getOrCreate()
    		import spark.implicits._
   		val chainData = spark.read.textFile(chainFile).map(s => (s.split(", ")(2), s)).cache()
		
		/*pairData.mapValues(joinPrep).reduceByKey(joinFunc)
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
def joinPrep(key:String, value:String): String ={
	val val_arr = value.split(", ")
	if(val_arr(0) == "input"){
		return "%s, 0, 1".format(val_arr(2))
	}
	else{
		return "0, %s, 1".format(val_arr(2))
	}
}

//inputs key = key, value = inputTotal, ouputTotal, numTransactions
//returns out key = key, value = inputTotal, ouputTotal, numTransactions
def joinFunc(accum:String, value:String): String ={
	//split inputs into parts
	val accum_arr = accum.split(", ").map(v => v.toLong)
	val val_arr = value.split(", ").map(v => v.toLong)
	//add components
	val result = accum_arr.zip(val_arr).map{case (x,y) => x+y}
	return result.mkString(", ")
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
