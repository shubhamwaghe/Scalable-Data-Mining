//////////////////////////////////////////////////////////////////////////
// Name: Waghe Shubham Yatindra
// Roll No.: 13MF3IM17
// MapReduce on Spark
// spark-submit --class "MatrixMultiplication" <jar> input_matrices.txt output
//////////////////////////////////////////////////////////////////////////
import org.apache.spark.{SparkConf, SparkContext}

object MatrixMultiplication {

  def parseVector(line: String, matrix: String): (String, List[String]) = {
    val elems = line.split(",")
    if (matrix == "A") {
      (elems(2), List(elems(1), elems(3)))
    } else {
      (elems(1), List(elems(2), elems(3)))
    }
  }

  def main(args: Array[String]) {
    val inputFile = args(0)
    val outputFile = args(1)
    val conf = new SparkConf().setAppName("MatrixMultiplication")
    val sc = new SparkContext(conf)
    val input =  sc.textFile(inputFile)
    val matrixA = input.map(line => line).filter { s: String => s.split(",")(0) == "A" }
    val matrixB = input.subtract(matrixA)
    val Avalues = matrixA.map{ line: String => parseVector(line, "A") }
    val Bvalues = matrixB.map{ line: String => parseVector(line, "B") }
    val vProducts = Avalues.join(Bvalues).map{
      case (_: String, (x: List[String], y: List[String])) => ((x.head, y.head), x(1).toInt*y(1).toInt)
    }.reduceByKey(_+_).sortByKey().map{ case ((i: String, j: String), mij: Int) => "%s,%s,%d".format(i,j,mij) }
    vProducts.repartition(1).saveAsTextFile(outputFile)
  }
}