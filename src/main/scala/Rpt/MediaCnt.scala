package Rpt

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import utils.RptUtils

object MediaCnt {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath,inputPath1) = args
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
    val df = spark.read.textFile(inputPath)
    import spark.implicits._
    val res = df.filter(_.length >= 5).map(x => {
      val strings = x.split("\t")
      (strings(4), strings(1))
    })
    val broadcast: Broadcast[Map[String, String]] = spark.sparkContext.broadcast(res.collect().toMap)
    val dataframe = spark.read.parquet(inputPath1)
    import spark.implicits._
    val value = dataframe.map(row => {
      val requestmode = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      // 获得appname
      val name = row.getAs[String]("appname")
      val appid = row.getAs[String]("appid")
      var appname=""
        if(name=="其他"){
          appname=broadcast.value.getOrElse(appid,"暂无")
      }else
        {
          appname=name
        }
      RptUtils.request(requestmode, processnode)
      RptUtils.click(requestmode, iseffective)
      RptUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)
      (appname, RptUtils.lst)
    }).rdd.reduceByKey((x, y) => {
      x.zip(y).map(x => {
        x._1 + x._2
      })
    }).map(x => {
      (x._1,x._2(0),x._2(1),x._2(2),x._2(3),x._2(4),x._2(5),x._2(6),x._2(7),x._2(8))
    })
    val load=ConfigFactory.load()
    val prop=new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    value.toDF("appname","originalNumber","effectiveNumber","AdNumber","haveNumber","sucessNumber","showNumber","clickNumber","winprice","adpay").write.mode("append").jdbc(load.getString("jdbc.url"),load.getString("jdbc.tablename6"),prop)
  }

}
