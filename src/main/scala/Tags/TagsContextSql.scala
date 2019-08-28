package Tags

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import redis.clients.jedis.Jedis
import utils.{JedisConnectionPool, TagUtils}

object TagsContextSql {
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath, stopPath) = args
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
    import spark.implicits._
    // 获取停用词库
    val stopword = spark.read.textFile(stopPath).map((_,0)).collect().toMap
    val bcstopword = spark.sparkContext.broadcast(stopword)

    // 进行数据的读取，处理分析数据
    val df: DataFrame = spark.read.parquet(inputPath)

    df.createOrReplaceTempView("dmp_table")
    spark.udf.register("getname",(appid:String,jedis:Jedis)=>{
        val jedis: Jedis = JedisConnectionPool.getConnection()
      (jedis.get(appid),1)
    })
    import spark.implicits._
   // val value = df.filter(TagUtils.OneUserId)
   //   // 接下来所有的标签都在内部实现
   //   .mapPartitions(iter=>{
   //   val jedis: Jedis = JedisConnectionPool.getConnection()
   //   val frame = spark.sql("select getname(appid,jedis) name from dmp_table")
   //   frame.toLocalIterator()
   //   iter
   // })
  }
}
