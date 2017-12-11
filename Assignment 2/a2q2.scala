//////////////////////////////////////////////////////////////////////////
// Name: Waghe Shubham Yatindra
// Roll No.: 13MF3IM17
// MapReduce on Spark
// spark-submit --class "KthPercentile" <jar> input_list.txt
//////////////////////////////////////////////////////////////////////////
import org.apache.spark.{SparkConf, SparkContext}

object KthPercentile {

  def main(args: Array[String]) {

    val inputFile = args(0)
    val conf = new SparkConf().setAppName("KthPercentile")
    val sc = new SparkContext(conf)
    val input =  sc.textFile(inputFile)
    val listCount = input.count
    val counts = input.map(number => (number, 1)).reduceByKey(_+_).sortByKey(ascending = true, 1)
    val numberList : List[(String, Int)] = counts.collect().toList

    def calculatePercentile(nth: Int): Float = {
      var breakPoint = listCount*nth*0.01
      var temp = 0
      var myPercentile: Float = 0
      if (breakPoint != breakPoint.toInt) {
        breakPoint = breakPoint.toInt + 1
        for (v <- numberList) {
          temp += v._2
          if(temp >= breakPoint) {
            myPercentile = v._1.toFloat
            return myPercentile
          }
        }
        myPercentile
      } else {
        var bFlag: Boolean = false
        for (v <- numberList) {
          temp += v._2
          if (bFlag) {
            myPercentile += v._1.toFloat
            myPercentile = myPercentile/2
            return myPercentile
          }
          if(temp == breakPoint) {
            bFlag = true
            myPercentile += v._1.toFloat
          }
          if (temp > breakPoint) {
            myPercentile = v._1.toFloat
            return myPercentile
          }
        }
        myPercentile
      }
    }
    println(calculatePercentile(25).toString+"\t"+calculatePercentile(50)+"\t"+calculatePercentile(75))
  }
}