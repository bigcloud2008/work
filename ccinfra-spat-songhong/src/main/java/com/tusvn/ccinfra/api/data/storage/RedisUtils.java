package com.tusvn.ccinfra.api.data.storage;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.tusvn.ccinfra.config.ConfigProxy;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.SortingParams;
import redis.clients.util.SafeEncoder;


/********
 * 对redis的读写工具类
 * 
 * 
 * @author liaoxb
 **/ 
public class RedisUtils {
	
    /**
	 * 日志定义 Logger
	 */
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RedisUtils.class);
	
	///////public static String JEDIS_PROPERTIES_FILE="redis.properties";
	
	
	
	/**
	 * 0: jedispool 1：jedisCluster
	 */
	private static String access_type = "0";
	
	/**
	 * 是否是 cluster 模式
	 */
	private static boolean isClusterMode = true;
	/**
	 * jedisCluster 实例
	 * <p>
	 * 当 redis 模式是群集模式时，redis.properties 中 redis.addr 的值应该配置为
	 * ip:port,ip:port,ip:port 模式
	 * </p>
	 * <p>
	 * 此时 <code>RedisUtils.getJedis() 将返回接到 redis.addr 中第一个节点
	 * </p>
	 */
	
	private static JedisPool jedisPool = null;
	
	private static JedisCluster jedisCluster = null;
	
	private static int timeout    =20000;
	private static int maxAttempts=5;
	
	//////////////////////////////////////////////////////////////////////////////
	//////copy code from cuiyan's redis tools
	/** 缓存生存时间 */
	 private static final int expire = 60000;
	/** 操作Key的方法 */
	 public static Keys 		KEYS;
	/** 对存储结构为String类型的操作 */
	 public static Strings 	STRINGS;
	/** 对存储结构为List类型的操作 */
	 public static Lists 		LISTS;
	/** 对存储结构为Set类型的操作 */
	 public static Sets 		SETS;
	/** 对存储结构为HashMap类型的操作 */
	 public static Hash 		HASH;
	/** 对存储结构为Set(排序的)类型的操作 */
	 public static SortSet 	SORTSET;
	//////////////////////////////////////////////////////////////////////////////
	
	static {
	   
		try {
			
			String addr   	= ConfigProxy.getProperty("redis.hosts");
			String s_port 	= ConfigProxy.getPropertyOrDefault("redis.port", 						"6379");
			int total 		= Integer.valueOf(ConfigProxy.getPropertyOrDefault("redis.maxTotal", 	"1000"));
			int idl 		= Integer.valueOf(ConfigProxy.getPropertyOrDefault("redis.maxIdle", 	"20"));
			long maxwait 	= Long.valueOf(ConfigProxy.getPropertyOrDefault("redis.maxWaitMillis", 	"10000"));
	        String auth		= ConfigProxy.getPropertyOrDefault("redis.password",					"");

			access_type 	= ConfigProxy.getPropertyOrDefault("redis.access.type",					"1");

			JedisPoolConfig config = new JedisPoolConfig();
			config.setMaxTotal(total);
			config.setMaxIdle(idl);
			config.setMaxWaitMillis(maxwait);
			
	        if(access_type!=null && access_type.equals("0"))   ////jedis standalone mode
	        {
	        	
	        	if(addr==null || addr.trim().equals("") || addr.trim().equals("null"))
	        		logger.error("redis.addr is not configed!");
	        	
	        	if(s_port==null || s_port.trim().equals("") || s_port.trim().equals("null"))
	        		logger.error("redis.port is not configed!");
	        	
	        	
	        	if(auth==null || auth.trim().equals("") || auth.trim().equals("null"))
	        	{
	        		jedisPool = new JedisPool(config, addr, Integer.parseInt(s_port), timeout);
	        	}
	        	else
	        	{
	        		if(addr.indexOf(",")!=-1)
	        		{
	        			String [] arr0=addr.split(",");
	        			String [] arr1=arr0[0].split(":");
	        			
	        			jedisPool = new JedisPool(config, arr1[0], Integer.parseInt(s_port), timeout, auth );
	        		}
	        		else
	        		{
	        			////JedisPool(GenericObjectPoolConfig poolConfig, String host, int port, int timeout, String password)
	        			jedisPool = new JedisPool(config, addr, Integer.parseInt(s_port), timeout, auth );
	        		}
	        	}
	        	
	        	
	        	isClusterMode=false;
	        }
	        else  if(access_type!=null && access_type.equals("1"))  ////jedis cluster mode
	        {
	        	if(addr==null || addr.trim().equals("") || addr.trim().equals("null"))
	        		logger.error("redis.addr is not configed!");
	        	
	            if(addr.indexOf(",") >= 0 || addr.indexOf(":") >= 0) {
	                String[] addrs = addr.split(",");
	                Set<HostAndPort> hostAndPorts = new HashSet<>();
	                for (String host : addrs) {
	                    HostAndPort hap = new HostAndPort(host.split(":")[0], Integer.parseInt(host.split(":")[1]));
	                    hostAndPorts.add(hap);

	                }
	                
	                if(auth!=null && !auth.trim().equals("") && !auth.trim().equals("null")  )
	                	jedisCluster = new JedisCluster(hostAndPorts, timeout, timeout, maxAttempts, auth, config);
	                else
	                	jedisCluster = new JedisCluster(hostAndPorts, timeout, timeout, maxAttempts, config);
	                
	            }
	            
	            isClusterMode=true;
	        }
		} catch (Exception e) {
			logger.error("参数初始化失败",e);
		}
	
	} 
	
	/**
	 * 返回 JedisCluster 实例
	 * <p>
	 * 当 redis 模式是群集模式时，redis.properties 中 redis.addr 的值应该配置为
	 * ip:port,ip:port,ip:port 模式
	 * </p>
	 * <p>
	 * 此时 <code>RedisUtils.getJedis() 将返回接到 redis.addr 中第一个节点
	 * </p>
	 * 
	 * @return
	 */
	public static JedisCluster getJedisCluster() {
	    return jedisCluster;
	}
	
	/**
	 * 获取Jedis实例
	 */
	public static Jedis getJedis() {
		Jedis resource = null;
		try {
			
			if(isClusterMode)
				logger.error("current is redis cluster mode!");
			
			
			if(access_type==null || access_type.trim().equals("") || access_type.trim().equals("null"))
				access_type="0";
			
			
			if ("0".equals(access_type)) {
				if (jedisPool != null) {
					resource = jedisPool.getResource();
				}
				else
					logger.error("can not get jedis resource1, jedis is null.");
			}else{
				if (jedisPool != null) {
					resource = jedisPool.getResource();
					resource.select(Integer.parseInt(access_type));
				}else{
					logger.error("can not get jedis resource2, jedis is null.");
				}
					
			}
			
			
		} catch (Exception ex) {
			logger.error("in getJedis", ex);
		}
		
		return resource;
	}

	/**
	 * 释放jedis资源
	 */
	public static void returnResource(final Jedis jedis) {
		if (jedis != null) {
			jedis.close();
		}
	}
	
	

	public RedisUtils() {
	}
	
	  
    /**
	 * 设置过期时间
	 * 
	 * @author ruan 2013-4-11
	 * @param key
	 * @param seconds
	 */
	public static void expire(String key, int seconds) {
		if (seconds <= 0) { 
			return;
		}
		
		if(isClusterMode)
			jedisCluster.expire(key, seconds);
		else
		{
		
			Jedis jedis =null;
			try {
				jedis=getJedis();
				jedis.expire(key, seconds);
	        }
		    catch (Exception e) {
			    e.printStackTrace();
			    logger.error("in expire", e);
	 	    }
		    finally{
			    returnResource(jedis);
		    }
		}
	}

	/**
	 * 设置默认过期时间
	 * 
	 * @author ruan 2013-4-11
	 * @param key
	 */
	public static void expire(String key) {
		expire(key, expire);
	}
	
	
	/**
	 * Keys
	 *
	 */
	public static class Keys {

		/**
		 * 清空所有key
		 */
		public static String flushAll() {
			String stata=null;
			
			if (isClusterMode) {
				stata = jedisCluster.flushAll();
			}
			else
			{
			Jedis jedis =null;
			
			
			try {
				jedis=getJedis();
				stata= jedis.flushAll();
			} catch (Exception e) {
				e.printStackTrace();
			}
			finally
			{
				returnResource(jedis);
			}
			}
			return stata;
		}

		/**
		 * 更改key
		 * 
		 * @param String
		 *            oldkey
		 * @param String
		 *            newkey
		 * @return 状态码
		 */
		public static String rename(String oldkey, String newkey) { 
			return rename(SafeEncoder.encode(oldkey),
					SafeEncoder.encode(newkey));
		}

		/**
		 * 更改key,仅当新key不存在时才执行
		 * 
		 * @param String
		 *            oldkey
		 * @param String
		 *            newkey
		 * @return 状态码
		 */
		public static long renamenx(String oldkey, String newkey) {
			long status=0L;
			if (isClusterMode) {
				status = jedisCluster.renamenx(oldkey, newkey);
			}
			else
			{
			Jedis jedis =null;
			
			try
			{
			jedis=getJedis();
			status = jedis.renamenx(oldkey, newkey);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			return status;
		}

		/**
		 * 更改key
		 * 
		 * @param String
		 *            oldkey
		 * @param String
		 *            newkey
		 * @return 状态码
		 */
		public static String rename(byte[] oldkey, byte[] newkey) {
			String status=null;
			if (isClusterMode) {
				status = jedisCluster.rename(oldkey, newkey);
			}
			else
			{
			Jedis jedis = null;
			
			try {
				jedis=getJedis();
				status = jedis.rename(oldkey, newkey);
			} catch (Exception e) {
				
				e.printStackTrace();
			}
			finally{
			returnResource(jedis);
			}
			}
			return status;
		}

		/**
		 * 设置key的过期时间，以秒为单位
		 * 
		 * @param String
		 *            key
		 * @param 时间,已秒为单位
		 * @return 影响的记录数
		 */
		public static long expired(String key, int seconds) {
			long count=0L;
			if (isClusterMode) {
				count = jedisCluster.expire(key, seconds);
			}
			else
			{
			Jedis jedis =null;
			
			
			try {
				jedis=getJedis();
				count = jedis.expire(key, seconds);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			return count;
		}

		/**
		 * 设置key的过期时间,它是距历元（即格林威治标准时间 1970 年 1 月 1 日的 00:00:00，格里高利历）的偏移量。
		 * 
		 * @param String
		 *            key
		 * @param 时间,已秒为单位
		 * @return 影响的记录数
		 */
		public static long expireAt(String key, long timestamp) {
			long count=0L;
			
			if (isClusterMode) {
				count = jedisCluster.expireAt(key, timestamp);
			}
			else
			{
			Jedis jedis =null;
			
			
			try {
				jedis=getJedis();
				count= jedis.expireAt(key, timestamp);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			return count;
		}

		/**
		 * 查询key的过期时间
		 * 
		 * @param String
		 *            key
		 * @return 以秒为单位的时间表示
		 */
		public static long ttl(String key) {
			long len=0L;
			if (isClusterMode) {
				len = jedisCluster.ttl(key);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis=null;
			
			try {
				sjedis=getJedis(); 
				len= sjedis.ttl(key);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(sjedis);
			}
			}
			return len;
		}

		/**
		 * 取消对key过期时间的设置
		 * 
		 * @param key
		 * @return 影响的记录数
		 */
		public static long persist(String key) {
			long count=0L;
			if (isClusterMode) {
				count = jedisCluster.persist(key);
			}
			else
			{
			Jedis jedis =null;
			
			try {
				jedis=getJedis();
				count = jedis.persist(key);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
				returnResource(jedis);
			}
			}
			return count;
		}

		/**
		 * 删除keys对应的记录,可以是多个key
		 * 
		 * @param String
		 *            ... keys
		 * @return 删除的记录数
		 */
		public static long del(String... keys) {
			long count =0L;
			
			if (isClusterMode) {
				count = jedisCluster.del(keys);
			}
			else
			{
			Jedis jedis =null;
			
			try
			{
			jedis=getJedis();
			count = jedis.del(keys);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
			finally
			{
				returnResource(jedis);
			}
			}
			return count;
		}

		/**
		 * 删除keys对应的记录,可以是多个key
		 * 
		 * @param String
		 *            .. keys
		 * @return 删除的记录数
		 */
		public static long del(byte[]... keys) {
			long count =0L;
			
			if (isClusterMode) {
				count = jedisCluster.del(keys);
			}
			else
			{
			Jedis jedis = null;
			
			try {
			jedis = getJedis();
			count= jedis.del(keys);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			return count;
		}

		/**
		 * 判断key是否存在
		 * 
		 * @param String
		 *            key
		 * @return boolean
		 */
		public static boolean exists(String key) {
			boolean exis = false;
			
			if (isClusterMode) {
				exis = jedisCluster.exists(key);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis=null;
			
			try {
				sjedis=getJedis();  
				exis = sjedis.exists(key);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally
			{
				returnResource(sjedis);
			}
			}
			return exis;
		}

		/**
		 * 对List,Set,SortSet进行排序,如果集合数据较大应避免使用这个方法
		 * 
		 * @param String
		 *            key
		 * @return List<String> 集合的全部记录
		 **/
		public static List<String> sort(String key) {
			
			List<String> list=null;
			if (isClusterMode) {
				list = jedisCluster.sort(key);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis=null;
			
			try
			{
				sjedis=getJedis();  
				list = sjedis.sort(key);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
			finally {
				returnResource(sjedis);
			}
			}
			return list;
		}

		/**
		 * 对List,Set,SortSet进行排序或limit
		 * 
		 * @param String
		 *            key
		 * @param SortingParams
		 *            parame 定义排序类型或limit的起止位置.
		 * @return List<String> 全部或部分记录
		 **/
		public static List<String> sort(String key, SortingParams parame) {
			List<String> list=null;
			
			if (isClusterMode) {
				list = jedisCluster.sort(key, parame);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis(); 
			Jedis sjedis=null;
			
			
			try {
				sjedis=getJedis(); 
				list = sjedis.sort(key, parame);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(sjedis);
			}
			}
			return list;
		}

		/**
		 * 返回指定key存储的类型
		 * 
		 * @param String
		 *            key
		 * @return String string|list|set|zset|hash
		 **/
		public static String type(String key) {
			String type =null;
			if (isClusterMode) {
				type = jedisCluster.type(key);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis(); 
			Jedis sjedis=null;
			
			try {
				sjedis=getJedis();  
				type= sjedis.type(key);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			} 
			finally {
			returnResource(sjedis);
			}
			}
			return type;
		}

		/**
		 * 查找所有匹配给定的模式的键
		 * 
		 * @param String
		 *            key的表达式,*表示多个，？表示一个
		 */
		public static Set<String> keys(String pattern) {
			Set<String> set = new TreeSet<String>();
			
			if (isClusterMode) {
				//TreeSet<String> keys = new TreeSet<>();
		        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();
		        
		        for(String k : clusterNodes.keySet()){
		            logger.info("Getting keys from: {}"+ k);
		            JedisPool jp = clusterNodes.get(k);
		            
		            Jedis connection = jp.getResource();
		            try {
		            	set.addAll(connection.keys(pattern));
		            } catch(Exception e){
		                logger.error("Getting keys error: {}", e);
		            } finally{
		                if(connection!=null)
							connection.close();// 用完一定要close这个链接！！！
		            }
		        }
			}
			else
			{
			Jedis jedis = null;
			
			try {
				jedis=getJedis();
				set = jedis.keys(pattern);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			return set;
		}
	}

	/**
	 * Sets
	 *
	 */
	public static class Sets {

		/**
		 * 向Set添加一条记录，如果member已存在返回0,否则返回1
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            member
		 * @return 操作码,0或1
		 */
		public static long sadd(String key, String member) {
			long s =0L;
			if (isClusterMode) {
				s = jedisCluster.sadd(key, member);
			}
			else
			{
			Jedis jedis = null;
			
			try {
				jedis = getJedis();
				s=jedis.sadd(key, member);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
				returnResource(jedis);
			}
			}
			return s;
		}

		public static long sadd(byte[] key, byte[] member) {
			long s=0L;
			if (isClusterMode) {
				s = jedisCluster.sadd(key, member);
			}
			else
			{
			Jedis jedis = null;
			
			try {
				jedis=getJedis();
				 s= jedis.sadd(key, member);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			return s;
		}

		/**
		 * 获取给定key中元素个数
		 * 
		 * @param String
		 *            key
		 * @return 元素个数
		 */
		public static long scard(String key) {
			long len =0L;
			if (isClusterMode) {
				len = jedisCluster.scard(key);
			}
			else
			{
		    //ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = null;
			
			try {
				sjedis=getJedis(); 
				len= sjedis.scard(key);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally
			{
				returnResource(sjedis);
			}
			}
			return len;
		}

		/**
		 * 返回从第一组和所有的给定集合之间的差异的成员
		 * 
		 * @param String
		 *            ... keys
		 * @return 差异的成员集合
		 */
		public static Set<String> sdiff(String... keys) {
			Set<String> set=null;
			if (isClusterMode) {
				set = jedisCluster.sdiff(keys);
			}
			else
			{
			Jedis jedis =null;
			
			
			try {
				jedis=getJedis();
				set = jedis.sdiff(keys);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally
			{
			returnResource(jedis);
			}
			}
			return set;
		}

		/**
		 * 这个命令等于sdiff,但返回的不是结果集,而是将结果集存储在新的集合中，如果目标已存在，则覆盖。
		 * 
		 * @param String
		 *            newkey 新结果集的key
		 * @param String
		 *            ... keys 比较的集合
		 * @return 新集合中的记录数
		 **/
		public static long sdiffstore(String newkey, String... keys) {
			long s=0L;
			if (isClusterMode) {
				s = jedisCluster.sdiffstore(newkey, keys);
			}
			else
			{
			Jedis jedis = null;
			
			
			try {
				jedis=getJedis();
				s = jedis.sdiffstore(newkey, keys);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally
			{
			returnResource(jedis);
			}
			}
			return s;
		}

		/**
		 * 返回给定集合交集的成员,如果其中一个集合为不存在或为空，则返回空Set
		 * 
		 * @param String
		 *            ... keys
		 * @return 交集成员的集合
		 **/
		public static Set<String> sinter(String... keys) {
			Set<String> set=null;
			if (isClusterMode) {
				set = jedisCluster.sinter(keys);
			}
			else
			{
			Jedis jedis =null;
			
			try {
				jedis=getJedis();
				set = jedis.sinter(keys);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally
			{
			returnResource(jedis);
			}
			}
			return set;
		}

		/**
		 * 这个命令等于sinter,但返回的不是结果集,而是将结果集存储在新的集合中，如果目标已存在，则覆盖。
		 * 
		 * @param String
		 *            newkey 新结果集的key
		 * @param String
		 *            ... keys 比较的集合
		 * @return 新集合中的记录数
		 **/
		public static long sinterstore(String newkey, String... keys) {
			long s=0L;
			if (isClusterMode) {
				s = jedisCluster.sinterstore(newkey, keys);
			}
			else
			{
			Jedis jedis =null;
			
			try {
				jedis=getJedis();
				s = jedis.sinterstore(newkey, keys);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			return s;
		}

		/**
		 * 确定一个给定的值是否存在
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            member 要判断的值
		 * @return 存在返回1，不存在返回0
		 **/
		public static boolean sismember(String key, String member) {
			boolean s=false;
			
			if (isClusterMode) {
				s = jedisCluster.sismember(key, member);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = null;
			
			
			try {
				sjedis=getJedis(); 
				s = sjedis.sismember(key, member);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally
			{
			returnResource(sjedis);
			}
			}
			return s;
		}

		/**
		 * 返回集合中的所有成员
		 * 
		 * @param String
		 *            key
		 * @return 成员集合
		 */
		public static Set<String> smembers(String key) {
			Set<String> set=null;
			
			if (isClusterMode) {
				set = jedisCluster.smembers(key);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis =null;
			
			
			try {
				sjedis=getJedis(); 
				set = sjedis.smembers(key);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(sjedis);
			}
			}
			return set;
		}

		public static Set<byte[]> smembers(byte[] key) {
			Set<byte[]> set=null;
			
			if (isClusterMode) {
				set = jedisCluster.smembers(key);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis =null;
			
			try {
				sjedis=getJedis();  
				set = sjedis.smembers(key);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally
			{
			returnResource(sjedis);
			}
			}
			return set;
		}

		/**
		 * 将成员从源集合移出放入目标集合 <br/>
		 * 如果源集合不存在或不包哈指定成员，不进行任何操作，返回0<br/>
		 * 否则该成员从源集合上删除，并添加到目标集合，如果目标集合中成员已存在，则只在源集合进行删除
		 * 
		 * @param String
		 *            srckey 源集合
		 * @param String
		 *            dstkey 目标集合
		 * @param String
		 *            member 源集合中的成员
		 * @return 状态码，1成功，0失败
		 */
		public static long smove(String srckey, String dstkey, String member) {
			long s =0L;
			
			if (isClusterMode) {
				s = jedisCluster.smove(srckey, dstkey, member);
			}
			else
			{
			Jedis jedis = null;
			
			try {
				jedis=getJedis();
				s = jedis.smove(srckey, dstkey, member);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally
			{
			returnResource(jedis);
			}
			}
			return s;
		}

		/**
		 * 从集合中删除成员
		 * 
		 * @param String
		 *            key
		 * @return 被删除的成员
		 */
		public static String spop(String key) {
			String s =null;
			if (isClusterMode) {
				s = jedisCluster.spop(key);
			}
			else
			{
			Jedis jedis = null;
			
			try {
				jedis=getJedis();
				s = jedis.spop(key);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally
			{
			returnResource(jedis);
			}
			}
			return s;
		}

		/**
		 * 从集合中删除指定成员
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            member 要删除的成员
		 * @return 状态码，成功返回1，成员不存在返回0
		 */
		public static long srem(String key, String member) {
			long s=0L;
			
			if (isClusterMode) {
				s = jedisCluster.srem(key, member);
			}
			else
			{
			Jedis jedis = null;
			
			try {
				jedis=getJedis();
				s = jedis.srem(key, member);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			return s;
		}

		/**
		 * 合并多个集合并返回合并后的结果，合并后的结果集合并不保存<br/>
		 * 
		 * @param String
		 *            ... keys
		 * @return 合并后的结果集合
		 * @see sunionstore
		 */
		public static Set<String> sunion(String... keys) {
			Set<String> set=null;
			if (isClusterMode) {
				set = jedisCluster.sunion(keys);
			}
			else
			{
			Jedis jedis =null;
			
			
			try {
				jedis=getJedis();
				set = jedis.sunion(keys);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally
			{
			returnResource(jedis);
			}
			}
			return set;
		}

		/**
		 * 合并多个集合并将合并后的结果集保存在指定的新集合中，如果新集合已经存在则覆盖
		 * 
		 * @param String
		 *            newkey 新集合的key
		 * @param String
		 *            ... keys 要合并的集合
		 **/
		public static long sunionstore(String newkey, String... keys) {
			long s =0L;
			if (isClusterMode) {
				s = jedisCluster.sunionstore(newkey, keys);
			}
			else
			{
			Jedis jedis =null;
			
			
			try {
				jedis=getJedis();
				s = jedis.sunionstore(newkey, keys);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			return s;
		}
	}

	/**
	 * SortSet
	 *
	 */
	public static class SortSet {

		/**
		 * 向集合中增加一条记录,如果这个值已存在，这个值对应的权重将被置为新的权重
		 * 
		 * @param String
		 *            key
		 * @param double
		 *            score 权重
		 * @param String
		 *            member 要加入的值，
		 * @return 状态码 1成功，0已存在member的值
		 */
		public static long zadd(String key, double score, String member) {
			long s =0;
			
			if (isClusterMode) {
				s = jedisCluster.zadd(key, score, member);
			}
			else
			{
			Jedis jedis=null;
			
			try {
				jedis= getJedis();
				s = jedis.zadd(key, score, member);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
				returnResource(jedis);
			}
			}
			return s;
		}

		/*public long zadd(String key, Map<Double, String> scoreMembers) {
			Jedis jedis = getJedis();
			long s = jedis.zadd(key, scoreMembers);
			returnResource(jedis);
			return s;
		}*/

		/**
		 * 获取集合中元素的数量
		 * 
		 * @param String
		 *            key
		 * @return 如果返回0则集合不存在
		 */
		public static long zcard(String key) {
			long len =0;
			
			if (isClusterMode) {
				len = jedisCluster.zcard(key);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis=null;
			
			try {
				sjedis= getJedis();
				len = sjedis.zcard(key);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(sjedis);
			}
			}
			return len;
		}

		/**
		 * 获取指定权重区间内集合的数量
		 * 
		 * @param String
		 *            key
		 * @param double
		 *            min 最小排序位置
		 * @param double
		 *            max 最大排序位置
		 */
		public static long zcount(String key, double min, double max) {
			long len=0;
			
			if (isClusterMode) {
				len = jedisCluster.zcount(key, min, max);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis =null;
			
			try {
				sjedis = getJedis();
				len = sjedis.zcount(key, min, max);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
				returnResource(sjedis);
			}
			}
			return len;
		}

		/**
		 * 获得set的长度
		 * 
		 * @param key
		 * @return
		 */
		public static long zlength(String key) {
			long len = 0;
			Set<String> set = zrange(key, 0, -1);
			len = set.size();
			return len;
		}

		/**
		 * 权重增加给定值，如果给定的member已存在
		 * 
		 * @param String
		 *            key
		 * @param double
		 *            score 要增的权重
		 * @param String
		 *            member 要插入的值
		 * @return 增后的权重
		 */
		public static double zincrby(String key, double score, String member) {
			
			double s =0;
			
			if (isClusterMode) {
				s = jedisCluster.zincrby(key, score, member);
			}
			else
			{
			Jedis jedis =null;
			
			try {
				jedis=getJedis();
				s = jedis.zincrby(key, score, member);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
				returnResource(jedis);
			}
			}
			return s;
		}

		/**
		 * 返回指定位置的集合元素,0为第一个元素，-1为最后一个元素
		 * 
		 * @param String
		 *            key
		 * @param int
		 *            start 开始位置(包含)
		 * @param int
		 *            end 结束位置(包含)
		 * @return Set<String>
		 */
		public static Set<String> zrange(String key, int start, int end) {
			Set<String> set=null;
			
			
			if (isClusterMode) {
				set = jedisCluster.zrange(key, start, end);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis =null;
			
			
			try {
				sjedis= getJedis(); 
				set = sjedis.zrange(key, start, end);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally
			{
			returnResource(sjedis);
			}
			}
			return set;
		}

		/**
		 * 返回指定权重区间的元素集合
		 * 
		 * @param String
		 *            key
		 * @param double
		 *            min 上限权重
		 * @param double
		 *            max 下限权重
		 * @return Set<String>
		 */
		public static Set<String> zrangeByScore(String key, double min, double max) {
			
			Set<String> set=null;
			if (isClusterMode) {
				set = jedisCluster.zrangeByScore(key, min, max);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis =null;
			
			
			try {
				sjedis= getJedis(); 
				set = sjedis.zrangeByScore(key, min, max);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
				returnResource(sjedis);
			}
			}
			return set;
		}

		/**
		 * 获取指定值在集合中的位置，集合排序从低到高
		 * 
		 * @see zrevrank
		 * @param String
		 *            key
		 * @param String
		 *            member
		 * @return long 位置
		 */
		public static long zrank(String key, String member) {
			
			long index=0;
			
			if (isClusterMode) {
				index = jedisCluster.zrank(key, member);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis =null;
			
			try {
				sjedis=getJedis(); 
				index = sjedis.zrank(key, member);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally
			{
				returnResource(sjedis);
			}
			}
			return index;
		}

		/**
		 * 获取指定值在集合中的位置，集合排序从高到低
		 * 
		 * @see zrank
		 * @param String
		 *            key
		 * @param String
		 *            member
		 * @return long 位置
		 */
		public static long zrevrank(String key, String member) {
			long index=0;
			
			if (isClusterMode) {
				index = jedisCluster.zrevrank(key, member);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis =null;
			
			try {
				sjedis=getJedis(); 
				index = sjedis.zrevrank(key, member);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
				returnResource(sjedis);
			}
			}
			return index;
		}

		/**
		 * 从集合中删除成员
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            member
		 * @return 返回1成功
		 */
		public static long zrem(String key, String member) {
			long s=0;
			
			if (isClusterMode) {
				s = jedisCluster.zrem(key, member);
			}
			else
			{
			Jedis jedis =null;
			
			try {
				jedis=getJedis();
				s = jedis.zrem(key, member);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			return s;
		}

		/**
		 * 删除
		 * 
		 * @param key
		 * @return
		 */
		public static long zrem(String key) {
			long s=0;
			if (isClusterMode) {
				s = jedisCluster.del(key);
			}
			else
			{
			
			Jedis jedis =null;
			
			try {
				jedis=getJedis();
				s = jedis.del(key);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally{
				returnResource(jedis);
			}
			}
			return s;
		}

		/**
		 * 删除给定位置区间的元素
		 * 
		 * @param String
		 *            key
		 * @param int
		 *            start 开始区间，从0开始(包含)
		 * @param int
		 *            end 结束区间,-1为最后一个元素(包含)
		 * @return 删除的数量
		 */
		public static long zremrangeByRank(String key, int start, int end) {
			long s=0;
			
			if (isClusterMode) {
				s = jedisCluster.zremrangeByRank(key, start, end);
			}
			else
			{
			Jedis jedis =null;
			
			try {
				jedis=getJedis();
				s = jedis.zremrangeByRank(key, start, end);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
				returnResource(jedis);
			}
			}
			return s;
		}

		/**
		 * 删除给定权重区间的元素
		 * 
		 * @param String
		 *            key
		 * @param double
		 *            min 下限权重(包含)
		 * @param double
		 *            max 上限权重(包含)
		 * @return 删除的数量
		 */
		public static long zremrangeByScore(String key, double min, double max) {
			
			long s=0;
			
			if (isClusterMode) {
				s = jedisCluster.zremrangeByScore(key, min, max);
			}
			else
			{
			Jedis jedis =null;
			
			try {
				jedis=getJedis();
				s = jedis.zremrangeByScore(key, min, max);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
				returnResource(jedis);
			}
			}
			return s;
		}

		/**
		 * 获取给定区间的元素，原始按照权重由高到低排序
		 * 
		 * @param String
		 *            key
		 * @param int
		 *            start
		 * @param int
		 *            end
		 * @return Set<String>
		 */
		public static Set<String> zrevrange(String key, int start, int end) {
			Set<String> set=null;
			
			if (isClusterMode) {
				set = jedisCluster.zrevrange(key, start, end);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis =null;
			
			try {
				sjedis=getJedis(); 
				set = sjedis.zrevrange(key, start, end);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			    returnResource(sjedis);
			}
			}
			return set;
		}

		/**
		 * 获取给定值在集合中的权重
		 * 
		 * @param String
		 *            key
		 * @param memeber
		 * @return double 权重
		 */
		public static double zscore(String key, String memebr) {
			Double score=null;
			
			if (isClusterMode) {
				score = jedisCluster.zscore(key, memebr);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis =null;
			
			try {
				sjedis=getJedis(); 
				score = sjedis.zscore(key, memebr);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
				returnResource(sjedis);
			}
			}
			if (score != null)
				return score;
			return 0;
		}
	}
	
	/**
	 * Hash
	 *
	 */
	public static class Hash {

		/**
		 * 从hash中删除指定的存储
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            field 存储的名字
		 * @return 状态码，1成功，0失败
		 */
 
		public static long hdel(String key, String fieid) {
			long s=0L;
			
			
			Jedis jedis = null;
			
			if (isClusterMode) {
				s = jedisCluster.hdel(key, fieid);
			}
			else
			{
			
			
			try {
				jedis=getJedis();
				 s= jedis.hdel(key, fieid);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally
			{
				returnResource(jedis);
			}
			}
			return s;
		}

		public static long hdel(String key, String... fieid) {
			long s=0L;
			
			
			Jedis jedis = null;
			
			if (isClusterMode) {
				s = jedisCluster.hdel(key, fieid);
			}
			else
			{
			
			
			try {
				jedis=getJedis();
				 s= jedis.hdel(key, fieid);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally
			{
				returnResource(jedis);
			}
			}
			return s;
		}
		
		public static long hdel(String key) {
			long s=0L;
			
			if (isClusterMode) {
				s = jedisCluster.del(key);
			}
			else
			{
			Jedis jedis = null;
			
			try {
				jedis=getJedis();
				s = jedis.del(key);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			return s;
		}

		/**
		 * 测试hash中指定的存储是否存在
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            field 存储的名字
		 * @return 1存在，0不存在
		 */
		public static boolean hexists(String key, String field) {
			boolean s=false;
			
			if (isClusterMode) {
				s = jedisCluster.hexists(key, field);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = null;
			
			try {
				sjedis=getJedis(); 
				s = sjedis.hexists(key, field);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally
			{
			returnResource(sjedis);
			}
			}
			return s;
		}

		/**
		 * 返回hash中指定存储位置的值
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            field 存储的名字
		 * @return 存储对应的值
		 */
		public static String hget(String key, String field) {
			String s=null;
			
			if (isClusterMode) {
				s = jedisCluster.hget(key, field);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis =null;
			
			
			try {
				sjedis=getJedis(); 
				s = sjedis.hget(key, field);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
				returnResource(sjedis);
			}
			}
			
			return s;
		}

		public static byte[] hget(byte[] key, byte[] field) {
			byte[] s=null;
			if (isClusterMode) {
				s = jedisCluster.hget(key, field);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis =null;
			
			try {
				sjedis=getJedis(); 
				s = sjedis.hget(key, field);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally
			{
			returnResource(sjedis);
			}
			}
			return s;
		}

		/**
		 * 以Map的形式返回hash中的存储和值
		 * 
		 * @param String
		 *            key
		 * @return Map<Strinig,String>
		 */
		public static Map<String, String> hgetAll(String key) {
			Map<String, String> map =null;
			
			if (isClusterMode) {
				map = jedisCluster.hgetAll(key);
			}
			else
			{
//			ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = null;
			
			
			try {
				sjedis=getJedis(); 
				map = sjedis.hgetAll(key);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(sjedis);
			}
			}
			return map;
		}

		/**
		 * 添加一个对应关系
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            field
		 * @param String
		 *            value
		 * @return 状态码 1成功，0失败，field已存在将更新，也返回0
		 **/
		public static long hset(String key, String field, String value) {
			
			long s=0L;
			
			if (isClusterMode) {
				s = jedisCluster.hset(key, field, value);
			}
			else
			{
				Jedis jedis =null;
			try {
				jedis= getJedis();
				 s = jedis.hset(key, field, value);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			
			}
			return s;
		}

		public static long hset(String key, String field, byte[] value) {
			long s =0L;
			if (isClusterMode) {
				s = jedisCluster.hset(key.getBytes(), field.getBytes(), value);
			}
			else
			{
			Jedis jedis =null;
			
			try {
				jedis=getJedis();
				s = jedis.hset(key.getBytes(), field.getBytes(), value);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally{
			returnResource(jedis);
			}
			}
			return s;
		}

		/**
		 * 添加对应关系，只有在field不存在时才执行
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            field
		 * @param String
		 *            value
		 * @return 状态码 1成功，0失败field已存
		 **/
		public static long hsetnx(String key, String field, String value) {
			long s=0L;
			
			if (isClusterMode) {
				s = jedisCluster.hsetnx(key, field, value);
			}
			else
			{
			Jedis jedis =null;
			
			try {
				jedis=getJedis();
				s = jedis.hsetnx(key, field, value);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			return s;
		}

		/**
		 * 获取hash中value的集合
		 * 
		 * @param String
		 *            key
		 * @return List<String>
		 */
		public static List<String> hvals(String key) {
			List<String> list=null;
			
			if (isClusterMode) {
				list = jedisCluster.hvals(key);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis =null;
			
			
			
			try {
				sjedis=getJedis(); 
				list = sjedis.hvals(key);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(sjedis); 
			}
			}
			return list;
		}

		/**
		 * 在指定的存储位置加上指定的数字，存储位置的值必须可转为数字类型
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            field 存储位置
		 * @param String
		 *            long value 要增加的值,可以是负数
		 * @return 增加指定数字后，存储位置的值
		 */
		public static long hincrby(String key, String field, long value) {
			long s=0L;
			
			if (isClusterMode) {
				s = jedisCluster.hincrBy(key, field, value);
			}
			else
			{
			Jedis jedis =null;
			
			try {
				jedis=getJedis();
				s = jedis.hincrBy(key, field, value);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			return s;
		}

		/**
		 * 返回指定hash中的所有存储名字,类似Map中的keySet方法
		 * 
		 * @param String
		 *            key
		 * @return Set<String> 存储名称的集合
		 */
		public static Set<String> hkeys(String key) {
			Set<String> set=null;
			if (isClusterMode) {
				set = jedisCluster.hkeys(key);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = null;
			
			
			try {
				sjedis=getJedis(); 
				set = sjedis.hkeys(key);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(sjedis);
			}
			}
			return set;
		}

		/**
		 * 获取hash中存储的个数，类似Map中size方法
		 * 
		 * @param String
		 *            key
		 * @return long 存储的个数
		 */
		public static long hlen(String key) {
			//ShardedJedis sjedis = getShardedJedis();
			long len=0L;
			
			if (isClusterMode) {
				len = jedisCluster.hlen(key);
			}
			else
			{
			Jedis sjedis =null;
			
			
			try {
				sjedis=getJedis();  
				len = sjedis.hlen(key);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(sjedis);
			}
			}
			return len;
		}

		/**
		 * 根据多个key，获取对应的value，返回List,如果指定的key不存在,List对应位置为null
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            ... fields 存储位置
		 * @return List<String>
		 */
		public static List<String> hmget(String key, String... fields) {
			//ShardedJedis sjedis = getShardedJedis();
			List<String> list =null;
			if (isClusterMode) {
				list = jedisCluster.hmget(key, fields);
			}
			else
			{
			Jedis sjedis = null;
			
			
			try {
				sjedis=getJedis(); 
				list = sjedis.hmget(key, fields);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(sjedis);
			}
			}
			return list;
		}

		public static List<byte[]> hmget(byte[] key, byte[]... fields) {
			List<byte[]> list =null;
			
			if (isClusterMode) {
				list = jedisCluster.hmget(key, fields);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis =null;
			
			
			try {
				sjedis=getJedis();  
				list = sjedis.hmget(key, fields);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(sjedis);
			}
			}
			return list;
		}

		/**
		 * 添加对应关系，如果对应关系已存在，则覆盖
		 * 
		 * @param Strin
		 *            key
		 * @param Map
		 *            <String,String> 对应关系
		 * @return 状态，成功返回OK
		 */
		public static String hmset(String key, Map<String, String> map) {
			String s=null;
			if (isClusterMode) {
				s = jedisCluster.hmset(key, map);
			}
			else
			{
			Jedis jedis =null;
			
			
			try {
				jedis=getJedis();
				s = jedis.hmset(key, map);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			return s;
		}

		/**
		 * 添加对应关系，如果对应关系已存在，则覆盖
		 * 
		 * @param Strin
		 *            key
		 * @param Map
		 *            <String,String> 对应关系
		 * @return 状态，成功返回OK
		 */
		public static String hmset(byte[] key, Map<byte[], byte[]> map) {
			String s=null;
			
			if (isClusterMode) {
				s = jedisCluster.hmset(key, map);
			}
			else
			{
			Jedis jedis =null;
			
			try {
				jedis=getJedis();
				s = jedis.hmset(key, map);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			return s;
		}

	}
	
	
	/**
	 * Strings
	 *
	 */
	public static class Strings {
		/**
		 * 根据key获取记录
		 * 
		 * @param String
		 *            key
		 * @return 值
		 */
		public static String get(String key) {
			//ShardedJedis sjedis = getShardedJedis();
			
			String value =null;
			
			
			Jedis jedis = null;
			
			
			if(isClusterMode)
			{
				value=jedisCluster.get(key);
			}
			else
			{
			try {
				jedis=getJedis();  
				value = jedis.get(key);
			} catch (Exception e) {
				// 
				e.printStackTrace();
				logger.error("in get",e);
			}
			finally {
				returnResource(jedis);
			}
			}
			return value;
		}

		/**
		 * 根据key获取记录
		 * 
		 * @param byte[]
		 *            key
		 * @return 值
		 */
		public static byte[] get(byte[] key) {
			byte[] value =null;
			
			//ShardedJedis sjedis = getShardedJedis();
			
			if(isClusterMode)
			{
				value=jedisCluster.get(key);
			}
			else
			{
			Jedis sjedis =null;
			
			try {
				sjedis=getJedis();  
				value = sjedis.get(key);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(sjedis);
			}
			}
			
			
			return value;
		}

		/**
		 * 添加有过期时间的记录
		 * 
		 * @param String
		 *            key
		 * @param int
		 *            seconds 过期时间，以秒为单位
		 * @param String
		 *            value
		 * @return String 操作状态
		 */
		public static String setEx(String key, int seconds, String value) {
			String res_value =null;
			
			if(isClusterMode)
			{
				res_value=jedisCluster.setex(key, seconds, value);
			}
			else
			{
			Jedis jedis =null;
			
			
			try {
				jedis=getJedis();
				res_value = jedis.setex(key, seconds, value);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally{
			returnResource(jedis);
			}
			}
			
			return value;
		}

		/**
		 * 添加有过期时间的记录
		 * 
		 * @param String
		 *            key
		 * @param int
		 *            seconds 过期时间，以秒为单位
		 * @param String
		 *            value
		 * @return String 操作状态
		 */
		public static String setEx(byte[] key, int seconds, byte[] value) {
			Jedis jedis = null;
			String str=null;
			
			if (isClusterMode) {
				str = jedisCluster.setex(key, seconds, value);
			} else {

				jedis = getJedis();
				try {
					str = jedis.setex(key, seconds, value);

				} catch (Exception e) {
					// 
					e.printStackTrace();
				} finally {
					returnResource(jedis);
				}
			}
			
			return str;
		}

		/**
		 * 添加一条记录，仅当给定的key不存在时才插入
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            value
		 * @return long 状态码，1插入成功且key不存在，0未插入，key存在
		 */
		public static long setnx(String key, String value) {
			Jedis jedis = null;
			long str=0L;
			
			if (isClusterMode) {
				str = jedisCluster.setnx(key, value);
			}
			else
			{
			
			try {
				jedis=getJedis();
				str = jedis.setnx(key, value);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			
			}
			
			return str;
		}

		/**
		 * 添加记录,如果记录已存在将覆盖原有的value
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            value
		 * @return 状态码
		 */
		public static String set(String key, String value) {
			return set(SafeEncoder.encode(key), SafeEncoder.encode(value));
		}
		

		/**
		 * 添加记录,如果记录已存在将覆盖原有的value
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            value
		 * @return 状态码
		 */
		public static String set(String key, byte[] value) {
			return set(SafeEncoder.encode(key), value);
		}

		/**
		 * 添加记录,如果记录已存在将覆盖原有的value
		 * 
		 * @param byte[]
		 *            key
		 * @param byte[]
		 *            value
		 * @return 状态码
		 */
		public static String set(byte[] key, byte[] value) {
			
			String status=null;
			
			if (isClusterMode) {
				status = jedisCluster.set(key, value);
			}
			else
			{
			
			Jedis jedis =null;
			
			
			try {
				jedis=getJedis();
				status = jedis.set(key, value);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			
			
			return status;
		}
		
		public static String setWithTTL(String key, String value, int seconds) {
			
			String status=null;
			
			if (isClusterMode) {
				status = jedisCluster.setex(key, seconds, value);
			}
			else
			{
			
			Jedis jedis =null;
			
			
			try {
				jedis=getJedis();
				status = jedis.setex(key, seconds, value);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
				returnResource(jedis);
			}
			}
			
			
			return status;
		}
		
		
		

		/**
		 * 从指定位置开始插入数据，插入的数据会覆盖指定位置以后的数据<br/>
		 * 例:String str1="123456789";<br/>
		 * 对str1操作后setRange(key,4,0000)，str1="123400009";
		 * 
		 * @param String
		 *            key
		 * @param long
		 *            offset
		 * @param String
		 *            value
		 * @return long value的长度
		 */
		public static long setRange(String key, long offset, String value) {
			long len =0L;
			
			if (isClusterMode) {
				len = jedisCluster.setrange(key, offset, value);
			}
			else
			{
			
			Jedis jedis =null;
			
			
			try {
				jedis=getJedis();
				len= jedis.setrange(key, offset, value);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			
			return len;
		}

		/**
		 * 在指定的key中追加value
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            value
		 * @return long 追加后value的长度
		 **/
		public static long append(String key, String value) {
			
			long len=0L;
			
			if (isClusterMode) {
				len = jedisCluster.append(key, value);
			}
			else
			{
			
			Jedis jedis =null;
			
			try {
				jedis=getJedis();
				len= jedis.append(key, value);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			
			return len;
		}

		/**
		 * 将key对应的value减去指定的值，只有value可以转为数字时该方法才可用
		 * 
		 * @param String
		 *            key
		 * @param long
		 *            number 要减去的值
		 * @return long 减指定值后的值
		 */
		public static long decrBy(String key, long number) {
			long len=0L;
			
			if (isClusterMode) {
				len = jedisCluster.decrBy(key, number);
			}
			else
			{
			
			Jedis jedis =null;
			
			
			try {
				jedis=getJedis();
				len= jedis.decrBy(key, number);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			return len;
		}

		/**
		 * <b>可以作为获取唯一id的方法</b><br/>
		 * 将key对应的value加上指定的值，只有value可以转为数字时该方法才可用
		 * 
		 * @param String
		 *            key
		 * @param long
		 *            number 要减去的值
		 * @return long 相加后的值
		 */
		public static long incrBy(String key, long number) {
			
			long len=0L;
			
			if (isClusterMode) {
				len = jedisCluster.incrBy(key, number);
			}
			else
			{
			Jedis jedis =null;
			
			try {
				jedis=getJedis();
				 len= jedis.incrBy(key, number);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			
			return len;
		}

		/**
		 * 对指定key对应的value进行截取
		 * 
		 * @param String
		 *            key
		 * @param long
		 *            startOffset 开始位置(包含)
		 * @param long
		 *            endOffset 结束位置(包含)
		 * @return String 截取的值
		 */
		public static String getrange(String key, long startOffset, long endOffset) {
			
			String value =null;
			
			
			if (isClusterMode) {
				value = jedisCluster.getrange(key, startOffset, endOffset);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis =null;
			
			
			try {
				sjedis=getJedis();  
				value = sjedis.getrange(key, startOffset, endOffset);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(sjedis); 
			}
			}
			return value;
		}

		/**
		 * 获取并设置指定key对应的value<br/>
		 * 如果key存在返回之前的value,否则返回null
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            value
		 * @return String 原始value或null
		 */
		public static String getSet(String key, String value) {
			
			String str =null;
			
			if (isClusterMode) {
				str = jedisCluster.getSet(key, value);
			}
			else
			{
			Jedis jedis = null;
			
			
			try {
				jedis=getJedis();
				str = jedis.getSet(key, value);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			return str;
		}

		/**
		 * 批量获取记录,如果指定的key不存在返回List的对应位置将是null
		 * 
		 * @param String
		 *            keys
		 * @return List<String> 值得集合
		 */
		public static List<String> mget(String... keys) {
			
			List<String> str =null;
			
			if (isClusterMode) {
				str = jedisCluster.mget(keys);
			}
			else
			{
				Jedis jedis = null;
				
			try {
				jedis=getJedis();
				str = jedis.mget(keys);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			
			return str;
		}

		/**
		 * 批量存储记录
		 * 
		 * @param String
		 *            keysvalues 例:keysvalues="key1","value1","key2","value2";
		 * @return String 状态码
		 */
		public static String mset(String... keysvalues) {
			
			String str=null;
			
			
			if (isClusterMode) {
				str = jedisCluster.mset(keysvalues);
			}
			else
			{
				Jedis jedis = null;
				
			try {
				jedis=getJedis();
				str = jedis.mset(keysvalues);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			return str;
		}

		/**
		 * 获取key对应的值的长度
		 * 
		 * @param String
		 *            key
		 * @return value值得长度
		 */
		public static long strlen(String key) {
			
			long len=0L;
			
			if (isClusterMode) {
				len = jedisCluster.strlen(key);
			}
			else
			{
			Jedis jedis =null;
			
			try {
				jedis=getJedis();
				len = jedis.strlen(key);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			return len;
		}
	}
	
	
	/**
	 * Lists
	 *
	 */
	public static class Lists {
		/**
		 * List长度
		 * 
		 * @param String
		 *            key
		 * @return 长度
		 */
		public static long llen(String key) {
			return llen(SafeEncoder.encode(key));
		}

		/**
		 * List长度
		 * 
		 * @param byte[]
		 *            key
		 * @return 长度
		 */
		public static long llen(byte[] key) {
			long count=0L;
			
			if (isClusterMode) {
				count = jedisCluster.llen(key);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis=null;
			
			try {
			sjedis= getJedis();  
			count= sjedis.llen(key);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
			finally {
				returnResource(sjedis);
			}
			}
			return count;
		}

		/**
		 * 覆盖操作,将覆盖List中指定位置的值
		 * 
		 * @param byte[]
		 *            key
		 * @param int
		 *            index 位置
		 * @param byte[]
		 *            value 值
		 * @return 状态码
		 */
		public static String lset(byte[] key, int index, byte[] value) {
			String status =null;
			
			if (isClusterMode) {
				status = jedisCluster.lset(key, index, value);
			}
			else
			{
			Jedis jedis =null;
			
			try {
			jedis=getJedis();
			status = jedis.lset(key, index, value);
			
			}
			catch(Exception ex)
			{
					ex.printStackTrace();
			}
			finally {
				returnResource(jedis);
			}
			}
			return status;
		}

		/**
		 * 覆盖操作,将覆盖List中指定位置的值
		 * 
		 * @param key
		 * @param int
		 *            index 位置
		 * @param String
		 *            value 值
		 * @return 状态码
		 */
		public static String lset(String key, int index, String value) {
			return lset(SafeEncoder.encode(key), index,
					SafeEncoder.encode(value));
		}

		/**
		 * 在value的相对位置插入记录
		 * 
		 * @param key
		 * @param LIST_POSITION
		 *            前面插入或后面插入
		 * @param String
		 *            pivot 相对位置的内容
		 * @param String
		 *            value 插入的内容
		 * @return 记录总数
		 */
		public static long linsert(String key, LIST_POSITION where, String pivot,
				String value) {
			return linsert(SafeEncoder.encode(key), where,
					SafeEncoder.encode(pivot), SafeEncoder.encode(value));
		}

		/**
		 * 在指定位置插入记录
		 * 
		 * @param String
		 *            key
		 * @param LIST_POSITION
		 *            前面插入或后面插入
		 * @param byte[]
		 *            pivot 相对位置的内容
		 * @param byte[]
		 *            value 插入的内容
		 * @return 记录总数
		 */
		public static long linsert(byte[] key, LIST_POSITION where, byte[] pivot,
				byte[] value) {
			long count=0L;
			
			if (isClusterMode) {
				count = jedisCluster.linsert(key, where, pivot, value);
			}
			else
			{
			Jedis jedis = null;
			
			try
			{
			jedis=getJedis();
			count= jedis.linsert(key, where, pivot, value);
			}
			catch(Exception ex)
			{
					ex.printStackTrace();
			}
			finally {
				returnResource(jedis);
			}
			}
			return count;
		}

		/**
		 * 获取List中指定位置的值
		 * 
		 * @param String
		 *            key
		 * @param int
		 *            index 位置
		 * @return 值
		 **/
		public static String lindex(String key, int index) {
			return SafeEncoder.encode(lindex(SafeEncoder.encode(key), index));
		}

		/**
		 * 获取List中指定位置的值
		 * 
		 * @param byte[]
		 *            key
		 * @param int
		 *            index 位置
		 * @return 值
		 **/
		public static byte[] lindex(byte[] key, int index) { 
			
			byte[] value=null;
			
			
			if (isClusterMode) {
				value = jedisCluster.lindex(key, index);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis =null;
			
			try {
			sjedis=getJedis();  
			value = sjedis.lindex(key, index);
			}
			catch(Exception ex)
			{
					ex.printStackTrace();
			}
			finally {
				returnResource(sjedis);
			}
			}
			return value;
		}

		/**
		 * 将List中的第一条记录移出List
		 * 
		 * @param String
		 *            key
		 * @return 移出的记录
		 */
		public static String lpop(String key) {
			return SafeEncoder.encode(lpop(SafeEncoder.encode(key)));
		}

		/**
		 * 将List中的第一条记录移出List
		 * 
		 * @param byte[]
		 *            key
		 * @return 移出的记录
		 */
		public static byte[] lpop(byte[] key) {
			byte[] value=null;
			
			
			if (isClusterMode) {
				value = jedisCluster.lpop(key);
			}
			else
			{
			Jedis jedis =null;
			
			try
			{
			jedis= getJedis();
			value = jedis.lpop(key);
			}
			catch(Exception ex)
			{
					ex.printStackTrace();
			}
			finally {
				returnResource(jedis);
			}
			}
			return value;
		}

		/**
		 * 将List中最后第一条记录移出List
		 * 
		 * @param byte[]
		 *            key
		 * @return 移出的记录
		 */
		public static String rpop(String key) {
			String value ="";
			
			if (isClusterMode) {
				value = jedisCluster.rpop(key);
			}
			else
			{
			
			Jedis jedis = null;
			
			try {
			jedis=getJedis();
			value = jedis.rpop(key);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
			finally {
				returnResource(jedis);
			}
			}
			return value;
		}

		/**
		 * 向List尾部追加记录
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            value
		 * @return 记录总数
		 */
		public static long lpush(String key, String value) {
			return lpush(SafeEncoder.encode(key), SafeEncoder.encode(value));
		}

		/**
		 * 向List头部追加记录
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            value
		 * @return 记录总数
		 */
		public static long rpush(String key, String value) {
			
			long count =0L;
			
			if (isClusterMode) {
				count = jedisCluster.rpush(key, value);
			}
			else
			{
			Jedis jedis =null;
			
			jedis=getJedis();
			
			try
			{
			count = jedis.rpush(key, value);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
			finally {
				returnResource(jedis);
				}
			}
			return count;
		}

		/**
		 * 向List头部追加记录
		 * 
		 * @param String
		 *            key
		 * @param String
		 *            value
		 * @return 记录总数
		 */
		public static long rpush(byte[] key, byte[] value) {
			long count=0L;
			
			
			if (isClusterMode) {
				count = jedisCluster.rpush(key, value);
			}
			else
			{
			Jedis jedis = null;
			
			
			
			try
			{
			jedis=getJedis();
			count = jedis.rpush(key, value);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
			finally {
				returnResource(jedis);
				}
			}
			return count;
		}

		/**
		 * 向List中追加记录
		 * 
		 * @param byte[]
		 *            key
		 * @param byte[]
		 *            value
		 * @return 记录总数
		 */
		public static long lpush(byte[] key, byte[] value) {
			long count=0L;
			
			if (isClusterMode) {
				count = jedisCluster.lpush(key, value);
			}
			else
			{
			
			Jedis jedis =null;
			
			try
			{
			jedis=getJedis();
			count = jedis.lpush(key, value);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
			finally {
			returnResource(jedis);
			}
			}
			
			return count;
		}

		/**
		 * 获取指定范围的记录，可以做为分页使用
		 * 
		 * @param String
		 *            key
		 * @param long
		 *            start
		 * @param long
		 *            end
		 * @return List
		 */
		public static List<String> lrange(String key, long start, long end) {
			List<String> list=null;
			
			if (isClusterMode) {
				list = jedisCluster.lrange(key, start, end);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = null;
			
			try {
				sjedis=getJedis();   
				list = sjedis.lrange(key, start, end);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally
			{
				returnResource(sjedis);
			}
			}
			return list;
		}

		/**
		 * 获取指定范围的记录，可以做为分页使用
		 * 
		 * @param byte[]
		 *            key
		 * @param int
		 *            start
		 * @param int
		 *            end 如果为负数，则尾部开始计算
		 * @return List
		 */
		public static List<byte[]> lrange(byte[] key, int start, int end) {
			List<byte[]> list=null;
			
			if (isClusterMode) {
				list = jedisCluster.lrange(key, start, end);
			}
			else
			{
			//ShardedJedis sjedis = getShardedJedis();
			Jedis sjedis = null;
			
			
			try {
				sjedis=getJedis();   
				list = sjedis.lrange(key, start, end);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally
			{
				returnResource(sjedis);
			}
			}
			
			return list;
		}

		/**
		 * 删除List中c条记录，被删除的记录值为value
		 * 
		 * @param byte[]
		 *            key
		 * @param int
		 *            c 要删除的数量，如果为负数则从List的尾部检查并删除符合的记录
		 * @param byte[]
		 *            value 要匹配的值
		 * @return 删除后的List中的记录数
		 */
		public static long lrem(byte[] key, int c, byte[] value) {
			long count =0L;
			if (isClusterMode) {
				count = jedisCluster.lrem(key, c, value);
			}
			else
			{
			
			Jedis jedis =null;
			
			try {
				jedis=getJedis();
				count = jedis.lrem(key, c, value);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally
			{
				returnResource(jedis);
			}
			}
			return count;
		}

		/**
		 * 删除List中c条记录，被删除的记录值为value
		 * 
		 * @param String
		 *            key
		 * @param int
		 *            c 要删除的数量，如果为负数则从List的尾部检查并删除符合的记录
		 * @param String
		 *            value 要匹配的值
		 * @return 删除后的List中的记录数
		 */
		public static long lrem(String key, int c, String value) {
			return lrem(SafeEncoder.encode(key), c, SafeEncoder.encode(value));
		}

		/**
		 * 算是删除吧，只保留start与end之间的记录
		 * 
		 * @param byte[]
		 *            key
		 * @param int
		 *            start 记录的开始位置(0表示第一条记录)
		 * @param int
		 *            end 记录的结束位置（如果为-1则表示最后一个，-2，-3以此类推）
		 * @return 执行状态码
		 */
		public static String ltrim(byte[] key, int start, int end) {
			String str=null;
			
			if (isClusterMode) {
				str = jedisCluster.ltrim(key, start, end);
			}
			else
			{
			
			Jedis jedis = null;
			
			
			try {
				jedis=getJedis();
				str = jedis.ltrim(key, start, end);
			} catch (Exception e) {
				// 
				e.printStackTrace();
			}
			finally
			{
				returnResource(jedis);
			}
			}
			return str;
		}

		/**
		 * 算是删除吧，只保留start与end之间的记录
		 * 
		 * @param String
		 *            key
		 * @param int
		 *            start 记录的开始位置(0表示第一条记录)
		 * @param int
		 *            end 记录的结束位置（如果为-1则表示最后一个，-2，-3以此类推）
		 * @return 执行状态码
		 */
		public static String ltrim(String key, int start, int end) {
			return ltrim(SafeEncoder.encode(key), start, end);
		}
	} 
	////////////////////////////////////////////////////////////////////////////////////////////////////
	

	
	
	@SuppressWarnings("static-access")
	public static void main(String[] args) {
		
		String pattern="Test_CAR";
		Set<String> mykeys=RedisUtils.KEYS.keys(pattern);
		
		long n=0;
		for(String tmp: mykeys)
		{
			n++;
			RedisUtils.KEYS.del(tmp);
			
			System.out.println("del ["+n+"] ok");
		}
		System.out.println("ok");
		
		
		// TODO Auto-generated method stub
		
//		Jedis jedis=RedisUtils.getJedis();
//		
//		System.out.println("===========>jedis="+jedis);
// 
//		RedisUtils.HASH.hset("sharphero", "name", "008");
//		
//		logger.info("====>write hash name ok");
//		
//		String name=RedisUtils.HASH.hget("sharphero", "name");
//		
//		logger.info("====>get hash name="+name);
//		
//		
//		if(jedis!=null)
//			jedis.close();

	}

}
