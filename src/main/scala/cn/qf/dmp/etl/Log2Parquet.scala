package cn.qf.dmp.etl

import java.util.Properties

import cn.qf.dmp.utils.SparkUtils
import org.apache.spark.rdd.RDD

object Log2Parquet {

  val SPARK_PROPERTIES="spark.properties"

  def main(args: Array[String]): Unit = {

    if(args == null||args.length!=2){
      println("参数不对")
      System.exit(-1)
    }


    val Array(input,output)=args
     val properties=new Properties()
     properties.load(Log2Parquet.getClass.getClassLoader.getResourceAsStream(SPARK_PROPERTIES))
    val spark= SparkUtils.getLocalSparkSession(Log2Parquet.getClass.getSimpleName)
    spark.sqlContext.setConf(properties)

    val lines: RDD[String] = spark.sparkContext.textFile(input)
    val count: Long = lines.map(_.split(",")).filter(_.length>=85).count()
    println(count)



  }
}
