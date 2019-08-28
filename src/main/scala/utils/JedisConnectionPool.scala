package utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConnectionPool {

  val config = new JedisPoolConfig()
  //最大连接数
  config.setMaxTotal(10)
  //最大空闲连接数
  config.setMaxIdle(5)
  //当调用borrow object 方法时,是否进行有效性验证
  config.setTestOnBorrow(true)
  config.setTestOnCreate(false)
  val pool = new JedisPool(config, "localhost", 6379)

  def getConnection(): Jedis = {
    pool.getResource
  }
}