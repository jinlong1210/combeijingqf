package Rpt

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import utils.RptUtils


object LocaltionRpt {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    // 创建执行入口
    val spark = SparkSession.builder().config(conf)
      .config("spark.sql.warehouse.dir", "D://spark-warehouse")
      // 设置压缩方式 使用Snappy方式进行压缩
      .config("spark.sql.parquet.compression.codec", "snappy")
      .enableHiveSupport()
      .getOrCreate()

    // 进行数据的读取，处理分析数据
    val df = spark.read.parquet(inputPath)
    import spark.implicits._
    val value = df.map(row => {
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // key 值  是地域的省市
      val pro = row.getAs[String]("provincename")
      val city = row.getAs[String]("cityname")
      RptUtils.request(requestmode, processnode)
      RptUtils.click(requestmode, iseffective)
      RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)
      ((pro, city), RptUtils.lst)
    }).rdd.reduceByKey((x, y) => {
      x.zip(y).map(x => {
        x._1 + x._2
      })
    }).map(x => {
      (x._1._1,x._1._2,x._2(0),x._2(1),x._2(2),x._2(3),x._2(4),x._2(5),x._2(6),x._2(7),x._2(8))
    })
    val load=ConfigFactory.load()
    val prop=new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    value.toDF("provincename","cityname","originalNumber","effectiveNumber","AdNumber","haveNumber","sucessNumber","showNumber","clickNumber","winprice","adpay").write.mode("append").jdbc(load.getString("jdbc.url"),load.getString("jdbc.tablename"),prop)

  }
}
