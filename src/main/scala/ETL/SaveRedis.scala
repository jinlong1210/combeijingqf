package ETL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import utils.JedisConnectionPool

object SaveRedis {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath) = args
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")

    // 创建执行入口
    val spark = SparkSession.builder().config(conf)
      .getOrCreate()
    val data = spark.read.textFile(inputPath)
    import spark.implicits._
    val res: Dataset[(String, String)] = data.filter(_.length >= 5).map(x => {
      val strings = x.split("\t")
      (strings(4), strings(1))
    })
    res.foreachPartition(iter=>{
      val jedis = JedisConnectionPool.getConnection()
      iter.foreach(x=>{
        val appid = x._1
        val appname = x._2
        jedis.set(appid,appname)
      })
      jedis.close()
    })
    spark.stop()
  }

}
