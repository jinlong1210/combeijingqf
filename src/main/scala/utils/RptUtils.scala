package utils

import scala.collection.mutable.ListBuffer

object RptUtils {
   val lst = new ListBuffer[Double]
  //此方法处理请求数
  def request(requestmode:Int,processnode:Int):ListBuffer[Double]={
    lst.clear()
    if(requestmode==1&&processnode>=1){
      lst +=1
    }else{
      lst+=0
    }
    if(requestmode==1&&processnode>=2){
      lst +=1
    }else{
      lst+=0
    }
    if(requestmode==1&&processnode==3){
      lst+=1
    }else{
      lst+=0
    }
    lst
  }
 // //此方法处理点击数
  def click(requestmode:Int,iseffective:Int):ListBuffer[Double]={
    if(requestmode==2&&iseffective==1){
      lst+=1
    }else{
      lst+=0
    }
    if(requestmode==3&&iseffective==1){
      lst+=1
    }else{
      lst+=0
    }
    lst
  }
  def Ad(iseffective:Int,isbilling:Int,isbid:Int,iswin:Int,adorderid:Int,WinPrice:Double,adpayment:Double):ListBuffer[Double]={
    if(iseffective==1&&isbilling==1&&isbid==1){
      lst+=1
    }else{
      lst+=0
    }
    if(iseffective==1&&isbilling==1&&iswin==1&&adorderid!=0){
      lst+=1
    }else{
      lst+=0
    }
    if(iseffective==1&&isbilling==1&&iswin==1){
      lst+=WinPrice/1000
    }else{
      lst+=0
    }
    if(iseffective==1&&isbilling==1&&iswin==1){
      lst+=adpayment/1000
    }else{
      lst+=0
    }
    lst
  }
  def clear(): Unit ={
    lst.clear()
  }

}
