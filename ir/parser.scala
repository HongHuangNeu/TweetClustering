package ir
import java.io.FileWriter
import org.apache.spark.SparkContext
import scala.math._
import java.lang.Math
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.util.StringTokenizer
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
object parser {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("hello").setMaster("local").set("spark.executor.memory", "3g")
    System.setProperty("spark.executor.memory", "3g")
    val sc = new SparkContext(conf)

    println("test")
    val textFile = sc.textFile("/home/honghuang/iphone2/idIphoneFile")

    val vocabulary = textFile.flatMap(e => {
      val str = e;
      //var stemmer = new Stemmer()
      var termList: Array[(String, Long)] = new Array[(String, Long)](0)
      val cleartext = str.replaceAll("https?://\\S+\\s?", "");
      val tmp = safeChar(cleartext)
      val stem = Stemmer.stem(tmp.trim())

      termList = termList ++ Array((safeChar(stem), 1L))
      termList
    }).keys
    //vocabulary.foreach(f=>println(f))
    vocabulary.collect.foreach(f => println(f))
    val fw = new FileWriter("/home/honghuang/iphone2/tempFile")
    vocabulary.collect.foreach(f => fw.write(f + "\n"))
  }

  def safeChar(input: String): String = {
    val allowed = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ \t".toCharArray();
    val charArray = input.toString().toCharArray();
    val result = new StringBuilder();

    for (c <- charArray) {
      for (a <- allowed) {
        if (c == a) result.append(a);
      }
    }
    result.toString();
  }
}