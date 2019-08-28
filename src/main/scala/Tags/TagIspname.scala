package Tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import utils.Tag

object TagIspname extends Tag {
  /**
    * 打标签的统一接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    val row = args(0).asInstanceOf[Row]
    val ispname = row.getAs[String]("ispname")
    if(StringUtils.isNotBlank(ispname)){
      list:+=(ispname,1)
    }
    list
  }
}
