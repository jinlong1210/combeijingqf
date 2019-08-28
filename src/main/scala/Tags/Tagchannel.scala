package Tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row
import utils.Tag

object Tagchannel extends Tag{
  /**
    * 打标签的统一接口
    * 3)	渠道（标签格式： CNxxxx->1）xxxx 为渠道 ID(adplatformproviderid)
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val adpid = row.getAs[Int]("adplatformproviderid")
    // 按照过滤条件进行过滤数据
    if(StringUtils.isNotBlank(adpid+"")){
      list:+=("CN"+adpid,1)
    }
    list
  }
}
