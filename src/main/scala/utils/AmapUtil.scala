package utils

import com.alibaba.fastjson.{JSON, JSONObject}

/**
  * 商圈解析工具
  * */
object AmapUtil {
def getBusinessFromAmap(long:Double,lat:Double): String ={
  val location= long+","+lat
  val urlStr="https://restapi.amap.com/v3/geocode/regeo?location="+location+"&key=fc6597eb574ce2d406c539043c59d618"
  val jsonstr = HttpUtil.get(urlStr)
  val jsonparse = JSON.parseObject(jsonstr)
  val status = jsonparse.getIntValue("status")
  if(status==0) return ""
  //接下来解析内部json串，判断每个key的value都不能为空
  val regeocodesJson = jsonparse.getJSONObject("regeocode")
  if (regeocodesJson == null || regeocodesJson.keySet().isEmpty) return ""
  val addressComponentJson = regeocodesJson.getJSONObject("addressComponent")
  if (addressComponentJson == null || addressComponentJson.keySet().isEmpty) return ""
  val businessAreasArray = addressComponentJson.getJSONArray("businessAreas")
  if (businessAreasArray == null || businessAreasArray.isEmpty) return ""
  //创建集合 保存数据
  val buffer = collection.mutable.ListBuffer[String]()
  //循环输出
  for(item<-businessAreasArray.toArray){
    if (item.isInstanceOf[JSONObject]) {
      val json = item.asInstanceOf[JSONObject]
      buffer.append(json.getString("name"))
    }
  }
  buffer.mkString(",")


}
}
