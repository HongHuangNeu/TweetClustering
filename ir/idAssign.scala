package ir
import org.apache.spark.SparkContext
import scala.math._
import java.lang.Math
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.StringTokenizer
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import java.io.FileWriter
object idAssign {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("hello").setMaster("local").set("spark.executor.memory", "3g")
    System.setProperty("spark.executor.memory", "3g")
    val sc = new SparkContext(conf)

    println("test")
    val File = sc.textFile("/home/honghuang/iphone/usefulTweets")
    val textFile = File.filter(e => { !e.trim().equals("") }).filter(e => { !e.contains("Round of") })
    val text = textFile.zipWithIndex
    val lines = text.flatMap(e => {
      val termList = Array((e._2, e._1))
      termList
    })
    val fw = new FileWriter("/home/honghuang/iphone2/idIphoneFile")
    lines.collectAsMap.foreach(f => fw.write(f._1 + "\t" + f._2 + "\n"))
    fw.flush()
  }
}