package Tags

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import redis.clients.jedis.Jedis
import utils.{JedisConnectionPool, TagUtils}

object TagsContext {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("目录参数不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合保存输入和输出目录
    val Array(inputPath, stopPath,days) = args
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
    // todo 调用Hbase API
    // 加载配置文件
    val load = ConfigFactory.load()
    val hbaseTableName = load.getString("hbase.TableName")
    // 创建Hadoop任务
    val configuration = spark.sparkContext.hadoopConfiguration
    configuration.set("hbase.zookeeper.quorum",load.getString("hbase.host"))
    // 创建HbaseConnection
    val hbconn = ConnectionFactory.createConnection(configuration)
    val hbadmin = hbconn.getAdmin
    // 判断表是否可用
    if(!hbadmin.tableExists(TableName.valueOf(hbaseTableName))){
      // 创建表操作
      val tableDescriptor = new HTableDescriptor(TableName.valueOf(hbaseTableName))
      val descriptor = new HColumnDescriptor("tags")
      tableDescriptor.addFamily(descriptor)
      hbadmin.createTable(tableDescriptor)
      hbadmin.close()
      hbconn.close()
    }
    // 创建JobConf
    val jobconf: JobConf = new JobConf(configuration)
    // 指定输出类型和表
    jobconf.setOutputFormat(classOf[TableOutputFormat])
    jobconf.set(TableOutputFormat.OUTPUT_TABLE,hbaseTableName)
    import spark.implicits._
    // 获取停用词库
    val stopword = spark.read.textFile(stopPath).map((_,0)).collect().toMap
    val bcstopword = spark.sparkContext.broadcast(stopword)

    // 进行数据的读取，处理分析数据
    val df: DataFrame = spark.read.parquet(inputPath)
    import spark.implicits._
    val value = df.filter(TagUtils.OneUserId)
      // 接下来所有的标签都在内部实现
      .mapPartitions(iter => {
      val jedis: Jedis = JedisConnectionPool.getConnection()
      var res = List[(String,List[(String,Int)])]()
      while (iter.hasNext) {
        val row: Row = iter.next()
        val userId = TagUtils.getOneUserId(row)
        // 接下来通过row数据 打上 所有标签（按照需求）
        val adList = TagsAd.makeTags(row)
        val appList = TagAPP.makeTags(row, jedis)
        val adpidList = Tagchannel.makeTags(row)
        val clientList = TagClient.makeTags(row)
        val netList = TagNet.makeTags(row)
        val ispnameList = TagIspname.makeTags(row)
        val provinceList = TagProvince.makeTags(row)
        val cityList = TagCity.makeTags(row)
        val keywordList = TagKeyWord.makeTags(row, bcstopword)
        val business = BusinessTag.makeTags(row)
        res :+= (userId, (adList++appList++adpidList++clientList++netList++ispnameList++provinceList++cityList++keywordList++business))
      }
      jedis.close()
      res.iterator
    }).rdd.reduceByKey((list1,list2)=>{
      list1++list2.groupBy(_._1).mapValues(_.foldLeft[Int](0)(_+_._2))
    }).map{
      case(userid,userTag)=>{

        val put = new Put(Bytes.toBytes(userid))
        // 处理下标签
        val tags = userTag.map(t=>t._1+","+t._2).mkString(",")
        put.addImmutable(Bytes.toBytes("tags"),Bytes.toBytes(days),Bytes.toBytes(tags))
        (new ImmutableBytesWritable(),put)
      }
    }
      // 保存到对应表中
      .saveAsHadoopDataset(jobconf)

    spark.stop()

  }
}
