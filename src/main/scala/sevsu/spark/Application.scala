package sevsu.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Application {
  private val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("spark_example")
    .set("spark.ui.showConsoleProgress", "false")

  private val sc: SparkContext = new SparkContext(conf)

  private val resourcesRoot: String = this.getClass.getResource("/").toString
  private val personPath: String = resourcesRoot + "person.csv"
  private val apartmentPath: String = resourcesRoot + "apartment.csv"

  def main(args: Array[String]): Unit = {
    val personRdd: RDD[String] = sc.textFile(personPath)
    val apartmentRdd: RDD[String] = sc.textFile(apartmentPath)

    println(personRdd.take(10).mkString("\n"))
    println
    println(apartmentRdd.take(10).mkString("\n"))


    sc.stop()
  }
}
