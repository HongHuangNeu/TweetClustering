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
object sampleTweets {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("hello").setMaster("local").set("spark.executor.memory", "3g")
    System.setProperty("spark.executor.memory", "3g")
    val sc = new SparkContext(conf)

    println("test")
    val File = sc.textFile("/home/honghuang/iphone2/result2")

    val lines = File.flatMap(e => {
      val str = new StringTokenizer(e, "\t")
      val tweet = str.nextToken()
      val id = str.nextToken()
      Array((tweet, id))
    })
    val total = lines.count
    val usefulTweets = lines.filter(f => { f._2.equals("cluster5") })
    val fw = new FileWriter("/home/honghuang/iphone2/usefulTweets")
    usefulTweets.collect.foreach(f => fw.write(f._1 + "\n"))
    fw.flush()
    val sw = new FileWriter("/home/honghuang/iphone2/sampleTweets")
    usefulTweets.sample(false, 0.1, 12345).collect.foreach(f => sw.write(f._1 + "\n"))
    sw.flush()

    println("total tweets" + total)
    //println("190: "+lines.filter(f=>f._2==190L).count)
    //println("188: "+lines.filter(f=>f._2==188L).count)
    //println("96: "+lines.filter(f=>f._2==96L).count)
    //println("283: "+lines.filter(f=>f._2==283L).count)
    //println("215: "+lines.filter(f=>f._2==215L).count)
    //println("221: "+lines.filter(f=>f._2==221L).count)
  }
}