//make RDD
val rawBlocks = sc.textFile("/home/lefteris/heart")
rawBlocks.first

//match data
case class MatchData(scores: Array[Double], num: Int)

//define parse
def toDouble(s: String) = {
	if ("?".equals(s)) Double.NaN else s.toDouble
}
def parse(line: String) = { 
	val pieces = line.split(',')                                            
	if ("1.0".equals(pieces(2))) pieces(2)="0.0" else if ("2.0".equals(pieces(2))) pieces(2)="1.0" else if ("3.0".equals(pieces(2))) pieces(2)="2.0" else pieces(2)="3.0"
	if ("1.0".equals(pieces(10))) pieces(10)="0.0" else if ("2.0".equals(pieces(10))) pieces(10)="1.0" else pieces(10)="2.0"
	if ("3.0".equals(pieces(12))) pieces(12)="1.0" else if ("6.0".equals(pieces(12))) pieces(12)="2.0" else if ("7.0".equals(pieces(12))) pieces(12)="3.0" else pieces(12)="0.0"               
	val scores = pieces.slice(0,13).map(toDouble)
	val num = pieces(13).toInt
	MatchData(scores, num)
}


//read first line
val line=rawBlocks.first
val md = parse(line)

//read entire file
val parsed = rawBlocks.map(line => parse(line))
parsed.cache()


//find NaNs 
import java.lang.Double.isNaN
import java.lang.Double.isNaN
val stats = (0 until 13).map(i => {parsed.map(md => md.scores(i)).filter(!isNaN(_)).stats() })
stats(11)
stats(12)


//clear NaNs 
def naz(d: Double) = if (Double.NaN.equals(d)) 0.0 else d


//prepare classification        
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
	val data = parsed.map { md =>
	val scores = Array(0,1,2,3,4,5,6,7,8,9,10,11,12).map(i => naz(md.scores(i)))
	val featureVector = Vectors.dense(scores)
	val label = if (md.num==0) 0 else 1 //either sick or not (sick includes values 1,2,3,4)
	LabeledPoint(label, featureVector)
}


//split data
val splits = data.randomSplit(Array(0.7, 0.3))
val (trainingData, testData) = (splits(0), splits(1))

//set parameters
val numClasses = 2
val categoricalFeaturesInfo = Map(
1 -> 2,
2 -> 4,
5 -> 2,
6 -> 3,
8 -> 2,
10 -> 3,
12 -> 4
)
val numTrees = 10
val featureSubsetStrategy = "auto"
val impurity = "gini"
val maxDepth = 4
val maxBins = 32


//training
import org.apache.spark.mllib.tree.RandomForest
val model = RandomForest.trainClassifier(trainingData,
numClasses, categoricalFeaturesInfo,
numTrees, featureSubsetStrategy, impurity,
maxDepth, maxBins)

//testing
val labelAndPreds = testData.map { point =>
val prediction = model.predict(point.features)
(point.label, prediction)
}

//results
val testErr = labelAndPreds.filter(r => r._1 !=r._2).count.toDouble / testData.count()
val accuracy = 1-testErr
