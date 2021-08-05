package com.tusvn.ccinfra.api.data.storage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.tusvn.ccinfra.communication.netty.handler.SpatHandler;
//import com.tusvn.ccinfra.api.data.model.DriverTotalVo;
//import com.tusvn.ccinfra.api.data.model.DurationVo;
import com.tusvn.ccinfra.config.ConfigProxy;

/**
 * 
 * 用于v2x项目
 * 
 * @author liaoxb
 *
 */
public class MongoUtilsForV2x {

	private static final Logger logger = LogManager.getLogger(MongoUtilsForV2x.class);
	/**
	 * mongodb 中存储实时数据的表名
	 */
	public static String MONGO_TABLE_NAME_REAL_INFO = "bsm_real_info";

	public static String MONGO_PROPERTIES_FILE = "mongodb.properties";

	private static MongoClient mongoClient = null;

	public static String MONGO_DB_NAME = "qdyk";
	
	private static String MONGO_HOST = "172.17.1.54:27017,172.17.1.55:27017,172.17.1.56:27017";//上海
	//private static String MONGO_HOST = "10.3.2.124,10.3.2.125,10.3.2.126";//长沙
	private static int MONGO_PORT = 27017;

	public static String CAR_REAL_INFO = "bsm_real_info";

	//// public static String F_VIN = "vin";
	public static String F_RECORDDATE = "recordDate";
	public static String F_LOCATION = "location";
	public static String F_VEHICLEID = "vehicleId"; ////////// string 2019-5-10
													////////// liaoxb add
													////////// 改为vehicleId
	public static String F_ID = "_id";
	public static String F_LONGITUDE = "longitude";
	public static String F_LATITUDE = "latitude";

	public static int MAX_CONN = 100;
	public static int MIN_CONN = 10;
	
	public static String ISMONGOAUTH  ="0";
	public static String MONGOUSERNAME="";
	public static String MONGOPASSWORD="";
	
    static {
    	try {
//            MONGO_DB_NAME = ConfigProxy.getProperty("mongodb.db.name");
//            MONGO_HOST = ConfigProxy.getProperty("mongodb.hosts");
//
//            String s_MAX_CONN = ConfigProxy.getProperty("mongodb.connectionsPerHost");
//            if (s_MAX_CONN == null || s_MAX_CONN.trim().equals("") || s_MAX_CONN.trim().equals("null"))
//                MAX_CONN = 100;
//            else
//                MAX_CONN = Integer.parseInt(s_MAX_CONN);
//            String s_MIN_CONN = ConfigProxy.getProperty("mongodb.minConnectionsPerHost");
//            if (s_MIN_CONN == null || s_MIN_CONN.trim().equals("") || s_MIN_CONN.trim().equals("null"))
//                MIN_CONN = 10;
//            else
//                MIN_CONN = Integer.parseInt(s_MIN_CONN);
//            
//            
//
//			ISMONGOAUTH   = ConfigProxy.getPropertyOrDefault("ISMONGOAUTH", "0");   /////配置在default空间下
//			MONGOUSERNAME = ConfigProxy.getPropertyOrDefault("MONGOUSERNAME","");
//			MONGOPASSWORD = ConfigProxy.getPropertyOrDefault("MONGOPASSWORD","");

        	ISMONGOAUTH   = "0";
        	MONGOUSERNAME = "qidi";
        	MONGOPASSWORD = "qidi@123";
        	
        	MONGO_HOST="172.30.11.22:27017,172.30.11.23:27017,172.30.11.33:27017";   ///172.17.5.161:27017,172.17.5.162:27017,172.17.5.163:27017
        	MONGO_PORT=27017; 

			logger.info("=======>ISMONGOAUTH="+ISMONGOAUTH);
			logger.info("=======>MONGOUSERNAME="+MONGOUSERNAME);
			logger.info("=======>MONGOPASSWORD="+MONGOPASSWORD);

            
            mongoClient = getConnection();
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("参数初始化失败",e);
		}

    }
	
	

	public MongoUtilsForV2x() {
		//
	}

	public static MongoClient getConnection() {
		if (mongoClient == null)
			mongoClient = createConnection();

		return mongoClient;
	}

	private static MongoClient createConnection() {
		MongoClient mongoClient = null;

		//////////
		MongoClientOptions options = MongoClientOptions.builder().connectTimeout(60000)
				/*
				 * 每个主机答应的连接数（每个主机的连接池大小）
				 * 
				 * socketTimeout:socket 超时时间（毫秒），用于 I/O 读写操作。1分钟=60000
				 */
				.connectionsPerHost(MAX_CONN)
				.minConnectionsPerHost(MIN_CONN)
                ///.maxConnectionLifeTime(3600*1000)
                .maxConnectionIdleTime(600*1000)
                .socketTimeout(60*1000)
				.build();

		////// mongoClient = new MongoClient( MONGO_HOST , MONGO_PORT );
		//// maxWaitTime, connectTimeout, socketTimeout

		//////// 172.17.1.109:27017,172.17.1.114:27017,172.17.1.119:27017
		List<ServerAddress> v_servers = new ArrayList<ServerAddress>();

		if (MONGO_HOST.indexOf(",") != -1) ////// 说明有多个主机
		{
			if (MONGO_HOST.indexOf(":") != -1) ////// 说明配置了端口
			{
				String[] arr1 = MONGO_HOST.split(","); ///// 切割出172.17.1.109:27017
				for (String tmpStr : arr1) {
					String[] arr2 = tmpStr.split(":"); ////// 切割出0=172.17.1.109
														////// 1=27017

					ServerAddress tmpServer = new ServerAddress(arr2[0], Integer.parseInt(arr2[1]));

					v_servers.add(tmpServer);
				}

			} else /////////// 172.17.1.109,172.17.1.114,172.17.1.119
			{
				String[] arr1 = MONGO_HOST.split(","); ///// 切割出172.17.1.109
				for (String tmpStr : arr1) {
					ServerAddress tmpServer = new ServerAddress(tmpStr, MONGO_PORT);

					v_servers.add(tmpServer);
				}
			}

			////mongoClient = new MongoClient(v_servers, options);
        	if(ISMONGOAUTH!=null && ISMONGOAUTH.equals("1")) ////开启了认证
        	{
        		///////createCredential(java.lang.String userName, java.lang.String database, char[] password)
        		MongoCredential credential = MongoCredential.createCredential(MONGOUSERNAME, MONGO_DB_NAME, MONGOPASSWORD.toCharArray());
        		
        		
        		mongoClient =  new MongoClient(v_servers, Arrays.asList(credential), options);
        	}
        	else
        	{
        		mongoClient = new MongoClient(v_servers, options);
        	}

		} else
		{
			////mongoClient = new MongoClient(new ServerAddress(MONGO_HOST, MONGO_PORT), options);
        	if(ISMONGOAUTH!=null && ISMONGOAUTH.equals("1")) ////开启了认证
        	{
 
        		MongoCredential credential = MongoCredential.createCredential(MONGOUSERNAME, MONGO_DB_NAME, MONGOPASSWORD.toCharArray());
        		
        		mongoClient = new MongoClient(new ServerAddress(MONGO_HOST, MONGO_PORT), Arrays.asList(credential), options);
        		
        		
        		
        	}
        	else
        	{
 
        		mongoClient = new MongoClient(new ServerAddress(MONGO_HOST, MONGO_PORT), options);
         
        	}
		}

		return mongoClient;
	}
	
	public static MongoClient createConnection(String dbName, String userName, String password) {
		MongoClient mongoClient = null;

		//////////
		MongoClientOptions options = MongoClientOptions.builder().connectTimeout(60000)
				/*
				 * 每个主机答应的连接数（每个主机的连接池大小）
				 */
				.connectionsPerHost(MAX_CONN)
				.minConnectionsPerHost(MIN_CONN)
                ///.maxConnectionLifeTime(3600*1000)
                .maxConnectionIdleTime(600*1000)
                .socketTimeout(60*1000)
				.build();

		////// mongoClient = new MongoClient( MONGO_HOST , MONGO_PORT );
		//// maxWaitTime, connectTimeout, socketTimeout

		//////// 172.17.1.109:27017,172.17.1.114:27017,172.17.1.119:27017
		List<ServerAddress> v_servers = new ArrayList<ServerAddress>();

		if (MONGO_HOST.indexOf(",") != -1) ////// 说明有多个主机
		{
			if (MONGO_HOST.indexOf(":") != -1) ////// 说明配置了端口
			{
				String[] arr1 = MONGO_HOST.split(","); ///// 切割出172.17.1.109:27017
				for (String tmpStr : arr1) {
					String[] arr2 = tmpStr.split(":"); ////// 切割出0=172.17.1.109
														////// 1=27017

					ServerAddress tmpServer = new ServerAddress(arr2[0], Integer.parseInt(arr2[1]));

					v_servers.add(tmpServer);
				}

			} else /////////// 172.17.1.109,172.17.1.114,172.17.1.119
			{
				String[] arr1 = MONGO_HOST.split(","); ///// 切割出172.17.1.109
				for (String tmpStr : arr1) {
					ServerAddress tmpServer = new ServerAddress(tmpStr, MONGO_PORT);

					v_servers.add(tmpServer);
				}
			}

			////mongoClient = new MongoClient(v_servers, options);
        	if(ISMONGOAUTH!=null && ISMONGOAUTH.equals("1")) ////开启了认证
        	{
        		///////createCredential(java.lang.String userName, java.lang.String database, char[] password)
        		MongoCredential credential = MongoCredential.createCredential(userName, dbName, password.toCharArray());
        		
        		
        		mongoClient =  new MongoClient(v_servers, Arrays.asList(credential), options);
        	}
        	else
        	{
        		mongoClient = new MongoClient(v_servers, options);
        	}
        	

		} else
		{
			///////mongoClient = new MongoClient(new ServerAddress(MONGO_HOST, MONGO_PORT), options);
			
			if(ISMONGOAUTH!=null && ISMONGOAUTH.equals("1")) ////开启了认证
        	{
        		MongoCredential credential = MongoCredential.createCredential(userName, dbName, password.toCharArray());
        		
        		mongoClient = new MongoClient(new ServerAddress(MONGO_HOST, MONGO_PORT), Arrays.asList(credential), options);
        		
        	}
        	else
        	{
        		mongoClient = new MongoClient(new ServerAddress(MONGO_HOST, MONGO_PORT), options);
        	}
		}

		return mongoClient;
	}

	////////////////////////////////////////////////////////////
	/********
	 * 2018-6-4 liaoxb add
	 *
	 * 连接mongodb集群， 启用用户名，密码登录 暂未使用
	 *
	 **/
	// public static MongoClient createConnection2()
	// {
	// MongoClient mongoClient=null;
	//
	// //////////////////////////////
	// String username="root"; /////////////从配置文件中读取
	// String password="123456";
	//
	//
	// MongoCredential credential =
	// MongoCredential.createMongoCRCredential(username, MONGO_DB_NAME,
	// password.toCharArray());
	// //////////
	// MongoClientOptions options = MongoClientOptions.builder()
	// .connectTimeout(60000)
	// .connectionsPerHost(MAX_CONN) ///////每个主机答应的连接数（每个主机的连接池大小）
	// .minConnectionsPerHost(MIN_CONN)
	// .build();
	//
	// List<ServerAddress> seeds=new ArrayList<ServerAddress>();
	// seeds.add(new ServerAddress(MONGO_HOST , MONGO_PORT ));
	// /////////////////////////////////////////////////
	//
	//
	// //////mongoClient = new MongoClient( MONGO_HOST , MONGO_PORT );
	// ////maxWaitTime, connectTimeout, socketTimeout
	//
	// mongoClient = new MongoClient( seeds, credential, options );
	//
	// return mongoClient;
	// }

	///////////////////////////////////////////////////////////

	/******
	 * 2018-4-21 liaoxb add 写入mongodb
	 *
	 * json格式如下：
	 *
	 * json="{"+ "\"_id\":\""+rowkey+"\",  "+
	 * "\"recordDate\":\"20180421132020123\",     "+
	 * "\"location\": {             "+ "    \"type\": \"Point\","+
	 * "    \"coordinates\": ["+s_lon+", "+s_lat+"]  "+ "  } "+
	 * 
	 * "}";
	 * 
	 * 
	 * 如果车辆上报数据分包，则不需要调用这个函数，这个函数是先判断有没有，有就执行更新，否则执行写入
	 *
	 * db: 数据库名 colName： 集合名，又称表名 rowkey: 记录唯一标识 json: 要新增或修改的值
	 *
	 **/
	public static String insertOrUpdateMongo(MongoClient conn, String db, String colName, String rowkey, String json)
			throws DbException {
		String result = null;

		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);

			Document document = Document.parse(json);

			////// System.out.println("document"+document);

			///// db.a1.update({_id:{$eq:1003}},{$set:{_id:1003,name:'lxb',age:40}},true,false)

			///// collection.insertOne(document );

			Document filter = new Document();
			// filter.append("_id", new ObjectId(rowkey)); ////////////2018-9-3
			// liaoxb add
			/*
			 * 2018-9-3 liaoxb add
			 */
			filter.append("_id", rowkey);

			// 注意update文档里要包含"$set"字段
			Document update = new Document();
			update.append("$set", document);

			/////////// 没有就新增，有就update
			UpdateOptions updateOptions = new UpdateOptions();
			updateOptions.upsert(true);

			UpdateResult res = collection.updateOne(filter, update, updateOptions);
			logger.info("matched count = " + res.getMatchedCount());
			logger.info("mongoDB insert successful.");
		} catch (Exception e) {
			// 
			e.printStackTrace();
			logger.error("in insertOrUpdateMongo", e);

			throw new DbException(2001, "insertOrUpdateMongo error", e);
		}

		return result;

	}

	/******
	 * 单条写入mongo
	 *
	 * 写历史表时，需要在json中加上_id: <rowkey>, 以便到hbase找对应记录
	 *
	 * 写其他表时，_id是自动生成的
	 *
	 * db: database name colName: table name json: value json
	 *
	 **/
	public static String insertMongo(MongoClient conn, String db, String colName, String json) throws DbException {
		String result = null;

		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);

			Document document = null;

			document = Document.parse(json);

			/// System.out.println("document"+document);

			collection.insertOne(document);
			//logger.info("mongoDB insert successful.");

		} catch (Exception e) {
			// 
			e.printStackTrace();
			logger.error("in insertMongo_single", e);

			throw new DbException(1010, e.getMessage(), e);
		}

		return result;

	}
	
	public static String insertMongoWithTTL(MongoClient conn, String db, String colName, String json) throws DbException {
		String result = null;

		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);

			Document document = null;

			document = Document.parse(json);
			
			/////////document.put("createISODate", new Date());
			
			document.put("createISODate", DateUtils.addHours(new Date(), 8));

			/// System.out.println("document"+document);

			collection.insertOne(document);
			//logger.info("mongoDB insert successful.");

		} catch (Exception e) {
			// 
			e.printStackTrace();
			logger.error("in insertMongo_single", e);

			throw new DbException(1010, e.getMessage(), e);
		}

		return result;

	}
	

	////////////////////////////////////////////////////////////////////////////////////////////////

	/*****
	 * 修改monogo中的表
	 *
	 * 判断有没有，没有就新增，有就修改
	 *
	 *
	 *
	 ***/
	public static String updateMongo(MongoClient conn, String db, String colName, String eventId, String json)
			throws DbException {
		String result = null;

		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = mongoClient.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);

			Document document = Document.parse(json);

			////// System.out.println("document"+document);

			///// db.a1.update({_id:{$eq:1003}},{$set:{_id:1003,name:'lxb',age:40}},true,false)

			///// collection.insertOne(document );

			Document filter = new Document();
			filter.append("_id", eventId);

			// 注意update文档里要包含"$set"字段
			Document update = new Document();
			update.append("$set", document);

			/////////// 没有就新增，有就update
			UpdateOptions updateOptions = new UpdateOptions();
			updateOptions.upsert(true);

			UpdateResult res = collection.updateOne(filter, update, updateOptions);

			logger.info("matched count = " + res.getMatchedCount());

		} catch (Exception e) {
			// 
			e.printStackTrace();
			logger.error("in updateMongo", e);

			throw new DbException(2011, "updateMongo error", e);
		}

		return result;

	}

	/*****
	 * 2019-3-5 wangmh add 修改一条
	 *
	 * 没有不新增
	 *
	 *
	 *
	 ***/
	public static String updateOne(MongoClient conn, String db, String colName, String id, String json)
			throws DbException {
		String result = null;

		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);

			Document document = Document.parse(json);

			Document filter = new Document();
			filter.append("_id", new ObjectId(id));

			// 注意update文档里要包含"$set"字段
			Document update = new Document();
			update.append("$set", document);

			UpdateResult res = collection.updateOne(filter, update);
			result = String.valueOf(res.getModifiedCount());
			logger.debug("matched count = " + res.getMatchedCount());

		} catch (Exception e) {
			// 
			e.printStackTrace();
			logger.error("in updateOne", e);

			throw new DbException(2011, "updateOne error", e);
		}

		return result;

	}

	/*****
	 * 2019-3-5 wangmh add 修改一条
	 *
	 * 没有不新增
	 *
	 *
	 *
	 ***/
	public static String updateOneByCondition(MongoClient conn, String db, String colName, String whereJson, String valJson) throws DbException {
		String result = null;

		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);

			Document document = Document.parse(valJson);

			Document filter = Document.parse(whereJson);

			// 注意update文档里要包含"$set"字段
			Document update = new Document();
			update.append("$set", document);
			
			/////////// 没有就新增，有就update
			UpdateOptions updateOptions = new UpdateOptions();
			updateOptions.upsert(true);
			

			UpdateResult res = collection.updateOne(filter, update, updateOptions);
			result = String.valueOf(res.getModifiedCount());
			logger.debug("matched count = " + res.getMatchedCount());

		} catch (Exception e) {
			// 
			e.printStackTrace();
			logger.error("in updateOne", e);

			throw new DbException(2011, "updateOne error", e);
		}

		return result;

	}

	/*****
	 * 2019-3-5 wangmh add 修改多条
	 *
	 * 没有不新增
	 *
	 *
	 *
	 ***/
	public static String updateManyByCondition(MongoClient conn, String db, String colName, String whereJson,
			String valJson) throws DbException {
		String result = null;

		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);

			Document document = Document.parse(valJson);

			Document filter = Document.parse(whereJson);

			// 注意update文档里要包含"$set"字段
			Document update = new Document();
			update.append("$set", document);

			UpdateResult res = collection.updateMany(filter, update);
			result = String.valueOf(res.getModifiedCount());
			logger.debug("matched count = " + res.getMatchedCount());

		} catch (Exception e) {
			// 
			e.printStackTrace();
			logger.error("in updateOne", e);

			throw new DbException(2011, "updateOne error", e);
		}

		return result;

	}

	////////////////////////////////////////////////////////////////////////////////////////////////

	/*****
	 * 2018-7-20 liaoxb add 修改monogo中的表
	 *
	 * 判断有没有，没有就新增，有就修改
	 *
	 *
	 * 比如表db.mytest.insert({"name":"007","level":1})
	 * db.mytest.insert({"name":"007","level":2})
	 *
	 * 更新样例为
	 * db.mytest.update({"name":"007"},{$set:{"level":3,"memo":"test"}},true,
	 * true)
	 *
	 *
	 * whereJson: 查询条件json， 如{"name":"007"}, {"name":"007", "level": 2}, or的查询 {
	 * $or: [ {key1: value1}, {key2:value2}] }
	 *
	 * valJson: 要更新的值json
	 *
	 *
	 ***/
	public static String updateManyMongo(MongoClient conn, String db, String colName, String whereJson, String valJson)
			throws DbException {
		String result = null;

		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);

			Document document = Document.parse(valJson);

			////// System.out.println("document"+document);

			///// db.a1.update({_id:{$eq:1003}},{$set:{_id:1003,name:'lxb',age:40}},true,false)

			///// collection.insertOne(document );

			Document filter = Document.parse(whereJson);

			// 注意update文档里要包含"$set"字段
			Document update = new Document();
			update.append("$set", document);

			/////////// 没有就新增，有就update
			UpdateOptions updateOptions = new UpdateOptions();
			updateOptions.upsert(true);

			UpdateResult res = collection.updateMany(filter, update, updateOptions);

			logger.info("matched count = " + res.getMatchedCount());

		} catch (Exception e) {
			// 
			e.printStackTrace();
			logger.error("in updateManyMongo", e);

			throw new DbException(2011, "updateManyMongo error", e);
		}

		return result;

	}
	////////////////////////////////////////////////////////////////////////////////////////////////

	/*****
	 * 通用查询
	 *
	 * db:数据库名，读配置 colName：表名 jsonCondition : 查询条件， json格式: 
	 * 
	 * { "vehicleId" : "LDC943X35A1224153", "recordDate" : { "$gte" : "20170421132020123" , "$lte" : "20190421132020123"} }
	 *
	 **/
	public static List<Map<String, Object>> queryMongo(MongoClient conn, String db, String colName,
			String jsonCondition, int skip, int limit) throws DbException {
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

		try {
			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			if (jsonCondition == null || jsonCondition.trim().equals("") || jsonCondition.trim().equals("null"))
				jsonCondition = "{}";

			//////// 查询条件
			BasicDBObject queryDb = BasicDBObject.parse(jsonCondition);
			logger.debug("=====>queryDb=" + queryDb);
			////////////////////////////////////////////////////////

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				findIterable = collection.find(queryDb).skip(skip).limit(limit);
			} else if (skip == 0 && limit > 0) {
				findIterable = collection.find(queryDb).limit(limit);
			} else {
				findIterable = collection.find(queryDb);
			}

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				HashMap<String, Object> map = new HashMap<String, Object>();

				for (String key : keys) {
					map.put(key, d.get(key));
				}

				result.add(map);

			}

			logger.debug("count=" + count);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryMongo", e);

			throw new DbException(3012, "queryMongo error", e);
		} finally {

		}

		return result;
	}
	
	public static List<Map<String, Object>> queryMongo(MongoClient conn, String db, String colName, Document jsonCondition, int skip, int limit) throws DbException {
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

		try {
			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			//// System.out.println("集合选择成功");
 
			logger.debug("=====>queryDb=" + jsonCondition.toJson());
			////////////////////////////////////////////////////////

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				findIterable = collection.find(jsonCondition).skip(skip).limit(limit);
			} else if (skip == 0 && limit > 0) {
				findIterable = collection.find(jsonCondition).limit(limit);
			} else {
				findIterable = collection.find(jsonCondition);
			}

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				HashMap<String, Object> map = new HashMap<String, Object>();

				for (String key : keys) {
					
					if(key.equals("_id"))
					{
						ObjectId _id=d.getObjectId("_id");

						map.put("_id", _id.toHexString());
						
					}
					else
					{
						map.put(key,   d.get(key));
					}
				}

				result.add(map);

			}

			logger.debug("count=" + count);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryMongo", e);

			throw new DbException(3012, "queryMongo error", e);
		} finally {

		}

		return result;
	}
	
	
	
	public static List<Map<String, Object>> queryMongoV2X(MongoClient conn, String db, String colName,
			String jsonCondition, String withExclusiveStartKey, 
			int skip, int limit) throws DbException {
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

		try {
			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			//// System.out.println("集合选择成功");
 
			BasicDBObject queryDb = BasicDBObject.parse(jsonCondition);
			
			if(withExclusiveStartKey!=null && !withExclusiveStartKey.trim().equals(""))
			{
				ObjectId _id=new ObjectId(withExclusiveStartKey);
				
				queryDb.put("_id", (new BasicDBObject()).append("$gte", _id));
			}
			
			//////// 查询条件
			
			logger.info("=====>queryDb=" + queryDb);
			////////////////////////////////////////////////////////

			FindIterable<Document> findIterable = null;

			if(limit==1) //////用户传了0，进入函数时加了1，所以在这里1表示查询全部记录
			{
				findIterable = collection.find(queryDb);
			}
			else
			{
				if (skip > 0 && limit > 0) {
					findIterable = collection.find(queryDb).skip(skip).limit(limit);
				} else if (skip == 0 && limit > 0) {
					findIterable = collection.find(queryDb).limit(limit);
				} else {
					findIterable = collection.find(queryDb);
				}
			}

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				HashMap<String, Object> map = new HashMap<String, Object>();

				for (String key : keys) {
					
					
					if(key.equals("_id"))
					{
						ObjectId _id=d.getObjectId("_id");
						
						map.put("rowkey", _id.toHexString());
						map.put("_id",    _id.toHexString());
					}
					else
						map.put(key, d.get(key));
				}

				result.add(map);

			}

			logger.debug("count=" + count);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryMongoV2X", e);

			throw new DbException(3012, "queryMongoV2X error", e);
		} finally {

		}

		return result;
	}
	
	
	
	

	public static MongoResultVo queryMongoWithTotal(MongoClient conn, String db, String colName, String jsonCondition,
			int skip, int limit) throws DbException {

		MongoResultVo resultVo = new MongoResultVo();

		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		long total = 0L;

		try {
			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			if (jsonCondition == null || jsonCondition.trim().equals("") || jsonCondition.trim().equals("null"))
				jsonCondition = "{}";

			//////// 查询条件
			BasicDBObject queryDb = BasicDBObject.parse(jsonCondition);
			logger.debug("=====>queryDb=" + queryDb);
			////////////////////////////////////////////////////////

			total = collection.count(queryDb);
			logger.info("total=" + total);

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				findIterable = collection.find(queryDb).skip(skip).limit(limit);
			} else if (skip == 0 && limit > 0) {
				findIterable = collection.find(queryDb).limit(limit);
			} else {
				findIterable = collection.find(queryDb);
			}

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				HashMap<String, Object> map = new HashMap<String, Object>();

				for (String key : keys) {
					map.put(key, d.get(key));
				}

				result.add(map);

			}

			logger.debug("count=" + count);

			resultVo.setRes(result);
			resultVo.setTotal(total);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryMongoWithTotal", e);

			throw new DbException(3012, "queryMongoWithTotal error", e);
		} finally {

		}

		return resultVo;
	}

	public static List<Map<String, Object>> queryMongo(MongoClient conn, String db, String colName,
			String jsonCondition, List<String> fields, int skip, int limit) throws DbException {
		List<Map<String, Object>> result = new ArrayList<>();

		try {
			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			if (jsonCondition == null || jsonCondition.trim().equals("") || jsonCondition.trim().equals("null"))
				jsonCondition = "{}";
			//////// 查询条件
			BasicDBObject queryDb = BasicDBObject.parse(jsonCondition);
			logger.info("=====>queryDb=" + queryDb);

			////////////////////////////////////////////
			BasicDBObject fieldDb = new BasicDBObject();
			fieldDb.put("_id", 0);
			if (fields != null && fields.size() > 0) {
				for (String field : fields) {
					fieldDb.put(field, 1);
				}

			}
			logger.info("=====>fieldDb=" + fieldDb);
			////////////////////////////////////////////////////////

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				findIterable = collection.find(queryDb).projection(fieldDb).skip(skip).limit(limit);
			} else if (skip == 0 && limit > 0) {
				findIterable = collection.find(queryDb).projection(fieldDb).limit(limit);
			} else {
				findIterable = collection.find(queryDb).projection(fieldDb);
			}

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				HashMap<String, Object> map = new HashMap<String, Object>();

				for (String key : keys) {
					map.put(key, d.get(key));
				}

				result.add(map);

			}

			logger.info("count=" + count);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryMongoByFields", e);

			throw new DbException(3002, "queryMongoByFields error", e);
		} finally {

		}

		return result;
	}

	public static MongoResultVo queryMongoWithTotal(MongoClient conn, String db, String colName, String jsonCondition,
			List<String> fields, int skip, int limit) throws DbException {

		MongoResultVo resultVo = new MongoResultVo();

		List<Map<String, Object>> result = new ArrayList<>();
		long total = 0L;

		try {
			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			if (jsonCondition == null || jsonCondition.trim().equals("") || jsonCondition.trim().equals("null"))
				jsonCondition = "{}";
			//////// 查询条件
			BasicDBObject queryDb = BasicDBObject.parse(jsonCondition);
			logger.info("=====>queryDb=" + queryDb);

			////////////////////////////////////////////
			BasicDBObject fieldDb = new BasicDBObject();
			fieldDb.put("_id", 0);
			if (fields != null && fields.size() > 0) {
				for (String field : fields) {
					fieldDb.put(field, 1);
				}

			}
			logger.info("=====>fieldDb=" + fieldDb);
			////////////////////////////////////////////////////////

			total = collection.count(queryDb);
			logger.info("total=" + total);

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				findIterable = collection.find(queryDb).projection(fieldDb).skip(skip).limit(limit);
			} else if (skip == 0 && limit > 0) {
				findIterable = collection.find(queryDb).projection(fieldDb).limit(limit);
			} else {
				findIterable = collection.find(queryDb).projection(fieldDb);
			}

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				HashMap<String, Object> map = new HashMap<String, Object>();

				for (String key : keys) {
					map.put(key, d.get(key));
				}

				result.add(map);

			}

			logger.info("count=" + count);

			resultVo.setRes(result);
			resultVo.setTotal(total);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryMongoWithTotal ByFields", e);

			throw new DbException(3002, "queryMongoWithTotal ByFields error", e);
		} finally {

		}

		return resultVo;
	}
	////////////////////////////////////////////////////////////////////////////////////////////////

	/***
	 * 带排序字段的通用查询
	 *
	 * orderJson: 排序json, 如 {"level",1} 升序， {"level",-1} 降序
	 *
	 *
	 **/
	public static List<Map<String, Object>> queryMongo(MongoClient conn, String db, String colName,
			String jsonCondition, List<String> fields, String orderJson, int skip, int limit) throws DbException {
		List<Map<String, Object>> result = new ArrayList<>();

		try {
			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			if (jsonCondition == null || jsonCondition.trim().equals("") || jsonCondition.trim().equals("null"))
				jsonCondition = "{}";
			//////// 查询条件
			BasicDBObject queryDb = BasicDBObject.parse(jsonCondition);
			logger.info("=====>queryDb=" + queryDb);

			////////////////////////////////////////////
			BasicDBObject fieldDb = new BasicDBObject();
			fieldDb.put("_id", 0);
			if (fields != null && fields.size() > 0) {
				for (String field : fields) {
					fieldDb.put(field, 1);
				}

			}
			logger.info("=====>fieldDb=" + fieldDb);
			////////////////////////////////////////////////////////

			/////////////// liaoxb add
			if (orderJson == null || orderJson.trim().equals("") || orderJson.trim().equals("null")) {
				orderJson = "{}";
			} 
			BasicDBObject orderDb = BasicDBObject.parse(orderJson);
			logger.info("=====>orderDb=" + orderDb);

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				findIterable = collection.find(queryDb).projection(fieldDb).sort(orderDb).skip(skip).limit(limit);
			} else if (skip == 0 && limit > 0) {
				findIterable = collection.find(queryDb).projection(fieldDb).sort(orderDb).limit(limit);
			} else {
				findIterable = collection.find(queryDb).projection(fieldDb).sort(orderDb);
			}

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				HashMap<String, Object> map = new HashMap<String, Object>();

				for (String key : keys) {
					map.put(key, d.get(key));
				}

				result.add(map);

			}

			logger.info("count=" + count);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryMongoByFields", e);

			throw new DbException(3002, "queryMongoByFields error", e);
		} finally {

		}

		return result;
	}
	
	/***
	 * 带排序字段的通用查询
	 *
	 * orderJson: 排序json, 如 {"level",1} 升序， {"level",-1} 降序
	 *
	 *
	 **/
	public static List<Map<String, Object>> queryMongo(String colName,String jsonCondition, List<String> fields, String orderJson, int skip, int limit) throws DbException {
		List<Map<String, Object>> result = new ArrayList<>();

		try {

			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase(MONGO_DB_NAME);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			if (jsonCondition == null || jsonCondition.trim().equals("") || jsonCondition.trim().equals("null"))
				jsonCondition = "{}";
			//////// 查询条件
			BasicDBObject queryDb = BasicDBObject.parse(jsonCondition);
			logger.info("=====>queryDb=" + queryDb);

			////////////////////////////////////////////
			BasicDBObject fieldDb = new BasicDBObject();
			fieldDb.put("_id", 0);
			if (fields != null && fields.size() > 0) {
				for (String field : fields) {
					fieldDb.put(field, 1);
				}

			}
			logger.info("=====>fieldDb=" + fieldDb);
			////////////////////////////////////////////////////////

			/////////////// liaoxb add
			if (orderJson == null || orderJson.trim().equals("") || orderJson.trim().equals("null")) {
				orderJson = "{}";
			} 
			BasicDBObject orderDb = BasicDBObject.parse(orderJson);
			logger.info("=====>orderDb=" + orderDb);

			FindIterable<Document> findIterable = collection.find(queryDb).projection(fieldDb).sort(orderDb);
			
			if (skip > 0 ) {
				findIterable = findIterable.skip(skip);
				logger.info("=====>skip=" + skip);
			}
			if (limit > 0) {
				findIterable = findIterable.limit(limit);
				logger.info("=====>limit=" + limit);
			}
 
			long count = 0;
			for (Document d : findIterable) {
				count++;
				
				Set<String> keys = d.keySet();
				HashMap<String, Object> map = new HashMap<String, Object>();
				
				for (String key : keys) {
//					if(key.equals("timestamp")){
//						map.put("time", d.get(key));
//					}else{
//						map.put(key.toLowerCase(), JSON.toJSON(d.get(key)));
//					}
					map.put(key.toLowerCase(), JSON.toJSON(d.get(key)));
				}

				result.add(map);

			}
			
			logger.info("count=" + count); 
			
 
		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryMongo", e);

			throw new DbException(3002, "queryMongo error", e);
		} finally {

		}

		return result;
	}
	
	public static List<Map<String, Object>> queryMongoForTest(String colName,String jsonCondition, List<String> fields, String orderJson, int skip, int limit) throws DbException {
		List<Map<String, Object>> result = new ArrayList<>();

		try {

			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase(MONGO_DB_NAME);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			if (jsonCondition == null || jsonCondition.trim().equals("") || jsonCondition.trim().equals("null"))
				jsonCondition = "{}";
			//////// 查询条件
			BasicDBObject queryDb = BasicDBObject.parse(jsonCondition);
			logger.info("=====>queryDb=" + queryDb);

			////////////////////////////////////////////
			BasicDBObject fieldDb = new BasicDBObject();
			fieldDb.put("_id", 0);
			if (fields != null && fields.size() > 0) {
				for (String field : fields) {
					fieldDb.put(field, 1);
				}

			}
			logger.info("=====>fieldDb=" + fieldDb);
			////////////////////////////////////////////////////////

			/////////////// liaoxb add
			if (orderJson == null || orderJson.trim().equals("") || orderJson.trim().equals("null")) {
				orderJson = "{}";
			} 
			BasicDBObject orderDb = BasicDBObject.parse(orderJson);
			logger.info("=====>orderDb=" + orderDb);

			FindIterable<Document> findIterable = collection.find(queryDb).projection(fieldDb).sort(orderDb);
			
			if (skip > 0 ) {
				findIterable = findIterable.skip(skip);
				logger.info("=====>skip=" + skip);
			}
			if (limit > 0) {
				findIterable = findIterable.limit(limit);
				logger.info("=====>limit=" + limit);
			}
 
			long count = 0;
			for (Document d : findIterable) {
				count++;
				
				Set<String> keys = d.keySet();
				HashMap<String, Object> map = new HashMap<String, Object>();
				
				for (String key : keys) {
//					if(key.equals("timestamp")){
//						map.put("time", d.get(key));
//					}else{
//						map.put(key.toLowerCase(), JSON.toJSON(d.get(key)));
//					}
					map.put(key, d.get(key));
				}

				result.add(map);

			}
			
			logger.info("count=" + count); 
			
 
		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryMongo", e);

			throw new DbException(3002, "queryMongo error", e);
		} finally {

		}

		return result;
	}	

	public static MongoResultVo queryMongoWithTotal(MongoClient conn, String db, String colName, String jsonCondition,
			List<String> fields, String orderJson, int skip, int limit) throws DbException {

		MongoResultVo resultVo = new MongoResultVo();

		List<Map<String, Object>> result = new ArrayList<>();
		long total = 0L;

		try {
			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			if (jsonCondition == null || jsonCondition.trim().equals("") || jsonCondition.trim().equals("null"))
				jsonCondition = "{}";
			//////// 查询条件
			BasicDBObject queryDb = BasicDBObject.parse(jsonCondition);
			logger.info("=====>queryDb=" + queryDb);

			////////////////////////////////////////////
			BasicDBObject fieldDb = new BasicDBObject();
			fieldDb.put("_id", 0);
			if (fields != null && fields.size() > 0) {
				for (String field : fields) {
					fieldDb.put(field, 1);
				}

			}
			logger.info("=====>fieldDb=" + fieldDb);
			////////////////////////////////////////////////////////

			/////////////// liaoxb add
			if (orderJson == null || orderJson.trim().equals("") || orderJson.trim().equals("null")) {
				orderJson = "{}";
			}

			BasicDBObject orderDb = BasicDBObject.parse(orderJson);
			logger.info("=====>orderDb=" + orderDb);

			total = collection.count(queryDb);
			logger.info("total=" + total);

			FindIterable<Document> findIterable = collection.find(queryDb).projection(fieldDb).sort(orderDb);

			
			if (skip > 0 ) {
				findIterable = findIterable.skip(skip);
			}
			if (limit > 0) {
				findIterable = findIterable.limit(limit);
			}

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				HashMap<String, Object> map = new HashMap<String, Object>();

				for (String key : keys) {
					map.put(key, d.get(key));
				}

				result.add(map);

			}

			logger.info("count=" + count);

			resultVo.setRes(result);
			resultVo.setTotal(total);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryMongoWithTotal with orderJson", e);

			throw new DbException(3002, "queryMongoWithTotal orderJson  error", e);
		} finally {

		}

		return resultVo;
	}
	
	public static MongoResultVo queryMongoWithTotal(String colName, String jsonCondition,
			List<String> fields, String orderJson, int skip, int limit) throws DbException {

		MongoResultVo resultVo = new MongoResultVo();

		List<Map<String, Object>> result = new ArrayList<>();
		long total = 0L;

		try {
			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase(MONGO_DB_NAME);

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);

			if (jsonCondition == null || jsonCondition.trim().equals("") || jsonCondition.trim().equals("null"))
				jsonCondition = "{}";
			// 查询条件
			BasicDBObject queryDb = BasicDBObject.parse(jsonCondition);
			logger.info("=====>queryDb=" + queryDb);

			////////////////////////////////////////////
			BasicDBObject fieldDb = new BasicDBObject();
			//排除_id
			fieldDb.put("_id", 0);
			if (fields != null && fields.size() > 0) {
				for (String field : fields) {
					fieldDb.put(field, 1);
				}
			}
			logger.info("=====>fieldDb=" + fieldDb);
			////////////////////////////////////////////////////////

			/////////////// liaoxb add
			if (orderJson == null || orderJson.trim().equals("") || orderJson.trim().equals("null")) {
				orderJson = "{}";
			}

			BasicDBObject orderDb = BasicDBObject.parse(orderJson);
			logger.info("=====>orderDb=" + orderDb);

			total = collection.countDocuments(queryDb);
			logger.info("=====>total=" + total);

			FindIterable<Document> findIterable = collection.find(queryDb).projection(fieldDb).sort(orderDb);

			
			if (skip > 0 ) {
				findIterable = findIterable.skip(skip);
				logger.info("=====>skip=" + skip);
			}
			if (limit > 0) {
				findIterable = findIterable.limit(limit);
				logger.info("=====>limit=" + limit);
			}
			for (Document d : findIterable) {

				Set<String> keys = d.keySet();

				HashMap<String, Object> map = new HashMap<String, Object>();

				for (String key : keys) {
//					if(key.equals("timestamp")){
//						map.put("time", d.get(key));
//					}else{
//						map.put(key.toLowerCase(), d.get(key));
//					}
					map.put(key.toLowerCase(), JSON.toJSON(d.get(key)));
				}
				result.add(map);
			}

			resultVo.setRes(result);
			resultVo.setTotal(total);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryMongoWithTotal with orderJson", e);

			throw new DbException(3002, "queryMongoWithTotal orderJson  error", e);
		} finally {

		}

		return resultVo;
	}
	
	public static long queryMongoTotal(String colName, String jsonCondition) throws DbException {

		long total = 0L;

		try {
			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase(MONGO_DB_NAME);

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);

			if (jsonCondition == null || jsonCondition.trim().equals("") || jsonCondition.trim().equals("null"))
				jsonCondition = "{}";
			// 查询条件
			BasicDBObject queryDb = BasicDBObject.parse(jsonCondition);
			logger.info("=====>queryDb=" + queryDb);

			total = collection.countDocuments(queryDb);
			logger.info("=====>total=" + total);

		} catch (Exception e) {
			e.printStackTrace();
			logger.error("in queryMongoWithTotal with orderJson", e);
			throw new DbException(3002, "queryMongoWithTotal orderJson  error", e);
		} 

		return total;
	}


	public static String removeMongo(MongoClient conn, String db, String colName, String jsonCondition)
			throws DbException {
		String result = null;

		try {
			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			//////// 查询条件

			if (jsonCondition == null || jsonCondition.trim().equals("") || jsonCondition.trim().equals("null"))
				jsonCondition = "{}";

			BasicDBObject queryDb = BasicDBObject.parse(jsonCondition);
			logger.info("=====>queryDb=" + queryDb);

			////////////////////////////////////////////
			DeleteResult res = collection.deleteMany(queryDb);

			if (res != null) {
				logger.info("del count=" + res.getDeletedCount());
				result = res.getDeletedCount() + "";
			}

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in removeMongo", e);

			result = "" + e.getMessage();

			throw new DbException(3012, "removeMongo error", e);
		} finally {

		}

		return result;
	}
	////////////////////////////////////////////////////////////////////////////////////////////////

	/*******
	 * 2018-5-16 liaoxb
	 *
	 * 批量的提交还是按单条的处理,先判断是否存在，存在则更新，否则写入
	 *
	 *
	 **/
	@Deprecated
	public static String insertMongoBatch2(MongoClient conn, String db, String colName, List<String[]> jsons)
			throws DbException {
		String result = null;

		if (conn == null)
			conn = mongoClient;

		MongoDatabase mongoDatabase = conn.getDatabase(db);

		MongoCollection<Document> collection = mongoDatabase.getCollection(colName);

		Document document = null;

		List<Document> ls = new ArrayList<Document>();
		for (String[] arr_json : jsons) {
			insertOrUpdateMongo(conn, db, colName, arr_json[0], arr_json[1]);
		}

		return result;

	}
	////////////////////////////////////////////////////////////////////////////////////////////////

	/*******
	 * 2018-4-23 liaoxb
	 *
	 * 一次发完，则可以执行批量写入
	 *
	 * 批量写入mongo:----不支持更新动作(如果数据分包，则不能用这个接口)
	 *
	 *
	 **/
	public static String insertMongoBatch(MongoClient conn, String db, String colName, List<String> jsons)
			throws DbException {
		String result = null;

		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);

			Document document = null;

			List<Document> ls = new ArrayList<Document>();
			for (String json : jsons) {
				document = Document.parse(json);

				/// System.out.println("document"+document);

				ls.add(document);
			}

			collection.insertMany(ls);

		} catch (Exception e) {
			// 
			e.printStackTrace();
			logger.error("in insertMongoBatch", e);

			throw new DbException(1004, "insertMongoBatch", e);
		}

		return result;

	}

	
	/**
	 * 批量写入，带有ISODate
	 * @param conn
	 * @param db
	 * @param colName
	 * @param jsons
	 * @return
	 * @throws DbException
	 */
	public static String insertMongoBatchWithTtl(MongoClient conn, String db, String colName, List<String> jsons)
			throws DbException {
		String result = null;

		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);

			Document document = null;

			List<Document> ls = new ArrayList<Document>();
			for (String json : jsons) {
				document = Document.parse(json);
				//添加ttl时间
				document.put("createISODate", DateUtils.addHours(new Date(), 8));
				/// System.out.println("document"+document);

				ls.add(document);
			}

			collection.insertMany(ls);

		} catch (Exception e) {
			// 
			e.printStackTrace();
			logger.error("in insertMongoBatch", e);

			throw new DbException(1004, "insertMongoBatch", e);
		}

		return result;

	}
	
	///////////////////////////////////////////////////////////

	/*******
	 * 根据开始时间，结束时间，圆心坐标，半径查询表中_id数据（rowkey）
	 * 
	 * 判断一段时间内，在指定圆心，半径内的车信息
	 *
	 * polygons: 圆心经度，纬度 radius： -----------------------半径单位：米，内部已经转换
	 * 
	 * 弧度， miles 英里单位， 如果地图上是米，则1 miles = 1609米， 需要将米转为miles传入
	 *
	 * skip: 0 不跳过 其他正值，跳过的记录数
	 *
	 * limit: 0 不限制 其他正值，一次返回的记录数
	 *
	 * 返回 rowkey的集合
	 **/
	public static List<String> queryCarInfoByCycle(MongoClient conn, String db, String colName, String beginDate,
			String endDate, double[] polygons, double radius, int skip, int limit) throws DbException {
		List<String> result = new ArrayList<String>();
		try {
			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			//// 2.test cycle query
			BasicDBObject condition = new BasicDBObject();

			// 查询两个时间范围的，用map包装一下
			///// query3.put( "recordDate", new BasicDBObject("$gte",
			// 20180101000000L).append("$lte", 20500101000000L) );

			condition.put(F_RECORDDATE, new BasicDBObject("$gte", beginDate).append("$lte", endDate));

			////////// 圆心---------------------------------------
			///// double[] polygons = {100.40111, 39.02222};

			List ls = new ArrayList();
			ls.add(polygons);
			/*
			 * 3963.2英里，是地球半径
			 */
			ls.add(convertMiToYinLi(radius) / 3963.2);
			///// ls.add(radius);

			//// $centerSphere 是MongoDB特有的语法,通过指定中心点和弧度半径来表示圆形区域。
			condition.put(F_LOCATION, new BasicDBObject("$geoWithin", new BasicDBObject("$centerSphere", ls)));

			logger.info("condition=" + condition);

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				findIterable = collection.find(condition).skip(skip).limit(limit);
			} else if (skip == 0 && limit > 0) {
				findIterable = collection.find(condition).limit(limit);
			} else
				findIterable = collection.find(condition);

			long count = 0;
			for (Document d : findIterable) {
				count++;
				////////////////// System.out.println("=====>d="+d);
				result.add("" + d.get("_id"));
			}

			logger.info("count=" + count);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryCarInfoByCycle", e);

			throw new DbException(2002, "queryCarInfoByCycle error", e);
		} finally {

		}

		return result;

	}

	/*******
	 * 
	 * 根据车id， 开始时间，结束时间，圆心坐标，半径查询表中_id数据（rowkey）
	 * 
	 * radius----半径单位：米
	 * 
	 **/
	public static List<String> queryCarInfoByCycle2(MongoClient conn, String db, String colName, String vehicleId,
			String beginDate, String endDate, double[] polygons, double radius, int skip, int limit)
					throws DbException {
		List<String> result = new ArrayList<String>();
		try {
			if (conn == null)
				conn = mongoClient;

			if (vehicleId == null || vehicleId.trim().equals("") || vehicleId.trim().equals("null")) {
				return queryCarInfoByCycle(conn, db, colName, beginDate, endDate, polygons, radius, skip, limit);
			}

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			//// 2.test cycle query
			BasicDBObject condition = new BasicDBObject();

			// 查询两个时间范围的，用map包装一下
			///// query3.put( "recordDate", new BasicDBObject("$gte",
			// 20180101000000L).append("$lte", 20500101000000L) );

			//// condition.put( "vin", new BasicDBObject("$eq", vin) );
			if (vehicleId != null && !vehicleId.trim().equals("") && !vehicleId.trim().equals("null"))
				condition.put(F_VEHICLEID, vehicleId);

			condition.put(F_RECORDDATE, new BasicDBObject("$gte", beginDate).append("$lte", endDate));

			////////// 圆心---------------------------------------
			///// double[] polygons = {100.40111, 39.02222};

			List ls = new ArrayList();
			ls.add(polygons);
			/*
			 * 3963.2英里，是地球半径
			 */
			ls.add(convertMiToYinLi(radius) / 3963.2);
			///// ls.add(radius);

			//// $centerSphere 是MongoDB特有的语法,通过指定中心点和弧度半径来表示圆形区域。
			condition.put(F_LOCATION, new BasicDBObject("$geoWithin", new BasicDBObject("$centerSphere", ls)));

			logger.info("condition=" + condition);

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				findIterable = collection.find(condition).skip(skip).limit(limit);
			} else if (skip == 0 && limit > 0) {
				findIterable = collection.find(condition).limit(limit);
			} else
				findIterable = collection.find(condition);

			long count = 0;
			for (Document d : findIterable) {
				count++;
				//// System.out.println("=====>d="+d);
				result.add("" + d.get("_id"));
			}
			logger.info("count=" + count);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryCarInfoByCycle", e);

			throw new DbException(2003, "queryCarInfoByCycle error", e);
		} finally {

		}

		return result;

	}

	/*****
	 * 将米转为英里
	 * 
	 * 1英里(mi)=1609.344米(m)
	 * 
	 * 1米(m)=0.0006214英里(mi)
	 * 
	 * 
	 **/
	public static double convertMiToYinLi(double radius) {
		double d = 0.0;

		d = radius * 0.0006214;

		return d;
	}

	/**********
	 * 根据圆形求范围内的车辆全部字段信息 ---读实时表
	 * 
	 * double[] polygons: 圆心坐标，经纬度
	 * 
	 * double radius： 圆的半径，单位为米-----在函数内部转为英里
	 * 
	 **/
	public static List<Map<String, Object>> queryRealInfoByCycle2(MongoClient conn, String db, List<String> fields,
			double[] polygons, double radius, int skip, int limit) throws DbException {
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		try {

			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(CAR_REAL_INFO);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			//// 2.test cycle query
			BasicDBObject condition = new BasicDBObject();

			// 查询两个时间范围的，用map包装一下
			///// query3.put( "recordDate", new BasicDBObject("$gte",
			// 20180101000000L).append("$lte", 20500101000000L) );

			//// condition.put( "vin", new BasicDBObject("$eq", vin) );
			// if(vin!=null && !vin.trim().equals("") &&
			// !vin.trim().equals("null"))
			// condition.put( "vin", vin);

			/////////// condition.put( "recordDate", new BasicDBObject("$gte",
			/////////// beginDate).append("$lte", endDate) );

			////////// 圆心---------------------------------------
			///// double[] polygons = {100.40111, 39.02222};

			List ls = new ArrayList();
			ls.add(polygons);
			/*
			 * 3963.2英里，是地球半径
			 * 
			 * 2018-10-12 liaoxab add 将米转为英里
			 */
			//// ls.add( radius / 3963.2);
			ls.add(convertMiToYinLi(radius) / 3963.2); /////////////////// radius
														/////////////////// -----单位：英里
														/////////////////// 1英里(mi)=1609.344米(m),
														/////////////////// 1米(m)=0.0006214英里(mi)
			///// ls.add(radius);

			//// $centerSphere 是MongoDB特有的语法,通过指定中心点和弧度半径来表示圆形区域。
			condition.put(F_LOCATION, new BasicDBObject("$geoWithin", new BasicDBObject("$centerSphere", ls)));

			logger.info("condition=" + condition);
			////////////////////////////////////////////
			BasicDBObject fieldDb = new BasicDBObject();

			if (fields != null && fields.size() > 0) {
				for (String field : fields) {
					fieldDb.put(field, 1);
				}

			}
			logger.info("=====>fieldDb=" + fieldDb);

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(condition).skip(skip).limit(limit);
				else
					findIterable = collection.find(condition).projection(fieldDb).skip(skip).limit(limit);

			} else if (skip == 0 && limit > 0) {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(condition).limit(limit);
				else
					findIterable = collection.find(condition).projection(fieldDb).limit(limit);
			} else {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(condition);
				else
					findIterable = collection.find(condition).projection(fieldDb);
			}

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				HashMap<String, Object> map = new HashMap<String, Object>();

				for (String key : keys) {
					map.put(key, d.get(key));
				}

				result.add(map);

			}

			logger.info("count=" + count);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryRealInfoByCycle2", e);

			throw new DbException(2013, "queryRealInfoByCycle2 error", e);
		} finally {

		}

		return result;

	}

	///////////////////////////////////////////////////////////
	/****
	 * 根据开始时间，结束时间，多边形数组 查询表中_id数据（rowkey）
	 * 
	 * 按多边形查历史表，只返回_id(rowkey)
	 * 
	 * 
	 * 
	 **/
	public static List<String> queryCarInfoByPolygon(MongoClient conn, String db, String colName, String beginDate,
			String endDate, List<double[]> polygons, int skip, int limit) throws DbException {
		List<String> result = new ArrayList<String>();
		try {

			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			//// 2.test cycle query
			BasicDBObject condition = new BasicDBObject();

			condition.put(F_RECORDDATE, new BasicDBObject("$gte", beginDate).append("$lte", endDate));

			/////////////////////////////////////////
			List a_polygons = new ArrayList();
			a_polygons.add(polygons);

			condition.put(F_LOCATION, new BasicDBObject("$geoWithin", new BasicDBObject("$geometry",
					new BasicDBObject("type", "Polygon").append("coordinates", a_polygons))));

			logger.info("condition=" + condition);

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				findIterable = collection.find(condition).skip(skip).limit(limit);
			} else if (skip == 0 && limit > 0) {
				findIterable = collection.find(condition).limit(limit);
			} else
				findIterable = collection.find(condition);

			long count = 0;
			for (Document d : findIterable) {

				///// System.out.println("=====>d="+d);
				count++;
				result.add("" + d.get("_id"));
			}
			logger.info("count=" + count);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryCarInfoByPolygon", e);

			throw new DbException(2004, "queryCarInfoByPolygon error", e);
		} finally {

		}

		return result;

	}

	/*****
	 * 根据车id，开始时间，结束时间，多边形数组 查询表中_id数据（rowkey）
	 * 
	 * 
	 **/
	public static List<String> queryCarInfoByPolygon2(MongoClient conn, String db, String colName, String vehicleId,
			String beginDate, String endDate, List<double[]> polygons, int skip, int limit) throws DbException {
		if (conn == null)
			conn = mongoClient;

		List<String> result = new ArrayList<String>();
		try {
			// if (vin == null || vin.trim().equals("") ||
			// vin.trim().equals("null")) {
			// return queryCarInfoByPolygon(conn, db, colName,
			// beginDate, endDate,
			// polygons,
			// skip, limit
			// );
			// }

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			//// 2.test cycle query
			BasicDBObject condition = new BasicDBObject();

			///// condition.put( "vin", new BasicDBObject("$eq", vin) );
			if (vehicleId != null && !vehicleId.trim().equals("") && !vehicleId.trim().equals("null"))
				condition.put(F_VEHICLEID, vehicleId);

			condition.put(F_RECORDDATE, new BasicDBObject("$gte", beginDate).append("$lte", endDate));

			/////////////////////////////////////////
			List a_polygons = new ArrayList();
			a_polygons.add(polygons);

			condition.put(F_LOCATION, new BasicDBObject("$geoWithin", new BasicDBObject("$geometry",
					new BasicDBObject("type", "Polygon").append("coordinates", a_polygons))));

			logger.info("condition=" + condition);

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				findIterable = collection.find(condition).skip(skip).limit(limit);
			} else if (skip == 0 && limit > 0) {
				findIterable = collection.find(condition).limit(limit);
			} else
				findIterable = collection.find(condition);

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				result.add("" + d.get("_id"));
			}

			logger.info("count=" + count);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryCarInfoByPolygon", e);

			throw new DbException(2005, "queryCarInfoByPolygon error", e);
		} finally {

		}

		return result;

	}

	////////////////////////////////////////////////////////////////////////////////////////
	/****
	 * 根据车id， 开始时间，结束时间，多边形数组 ，查询字段列表 查询表中全部数据
	 * 
	 * 
	 * 按多边形查历史表，查询时指定返回字段
	 * 
	 * 
	 * 
	 **/
	public static List<String> queryCarInfoByPolygon3(MongoClient conn, String db, String colName, String vehicleId,
			String beginDate, String endDate, double[][] polygons, String returnField, int skip, int limit)
					throws DbException {
		List<String> result = new ArrayList<String>();
		try {

			if (conn == null)
				conn = mongoClient;

			// if(vin==null || vin.trim().equals("") ||
			// vin.trim().equals("null"))
			// {
			// return queryCarInfoByPolygon( conn, db, colName,
			// beginDate, endDate,
			// polygons,
			// skip, limit
			// );
			// }

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			//// 2.test cycle query
			BasicDBObject condition = new BasicDBObject();

			///// condition.put( "vin", new BasicDBObject("$eq", vin) );

			if (vehicleId != null && !vehicleId.trim().equals("") && !vehicleId.trim().equals("null"))
				condition.put(F_VEHICLEID, vehicleId);

			condition.put(F_RECORDDATE, new BasicDBObject("$gte", beginDate).append("$lte", endDate));

			/////////////////////////////////////////
			List a_polygons = new ArrayList();
			a_polygons.add(polygons);

			condition.put(F_LOCATION, new BasicDBObject("$geoWithin", new BasicDBObject("$geometry",
					new BasicDBObject("type", "Polygon").append("coordinates", a_polygons))));

			logger.info("condition=" + condition);

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				findIterable = collection.find(condition).skip(skip).limit(limit);
			} else if (skip == 0 && limit > 0) {
				findIterable = collection.find(condition).limit(limit);
			} else
				findIterable = collection.find(condition);

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				result.add("" + d.get(returnField));
			}

			logger.info("count=" + count);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryCarInfoByPolygon", e);

			throw new DbException(2005, "queryCarInfoByPolygon error", e);
		} finally {

		}

		return result;

	}

	////////////////////////////////////////////////////////////////////////////////////////

	/****
	 * 根据开始时间，结束时间，多边形数组 查询表中全部数据
	 * 
	 * 
	 * 2018-8-1 liaoxb add
	 *
	 * 根据开始时间，结束时间，多边形区域查询mongo中car_yyyymm功能 查询mongodb中的 car_yyyymm
	 *
	 *
	 **/
	public static List<Map<String, Object>> queryHisByPolygonInMongo(MongoClient conn, String db, String colName,
			List<String> fields, String beginDate, String endDate, List<double[]> polygons, int skip, int limit)
					throws DbException {
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		try {

			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			//// 2.test cycle query
			BasicDBObject condition = new BasicDBObject();

			///// condition.put( "vin", new BasicDBObject("$eq", vin) );
			// condition.put( "vin", vin);
			//
			condition.put("recordDate", new BasicDBObject("$gte", beginDate).append("$lte", endDate));

			/////////////////////////////////////////
			List a_polygons = new ArrayList();
			a_polygons.add(polygons);

			condition.put(F_LOCATION, new BasicDBObject("$geoWithin", new BasicDBObject("$geometry",
					new BasicDBObject("type", "Polygon").append("coordinates", a_polygons))));

			logger.info("condition=" + condition);

			////////////////////////////////////////////
			BasicDBObject fieldDb = new BasicDBObject();

			if (fields != null && fields.size() > 0) {
				for (String field : fields) {
					fieldDb.put(field, 1);
				}

			}
			logger.info("=====>fieldDb=" + fieldDb);
			/////////////////////////////////////////////////////////////

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(condition).skip(skip).limit(limit);
				else
					findIterable = collection.find(condition).projection(fieldDb).skip(skip).limit(limit);
			} else if (skip == 0 && limit > 0) {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(condition).limit(limit);
				else
					findIterable = collection.find(condition).projection(fieldDb).limit(limit);
			} else {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(condition);
				else
					findIterable = collection.find(condition).projection(fieldDb);
			}

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				HashMap<String, Object> map = new HashMap<String, Object>();

				for (String key : keys) {
					map.put(key, d.get(key));
				}

				result.add(map);

			}

			logger.info("count=" + count);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryHisByPolygonInMongo", e);

			throw new DbException(2015, "queryHisByPolygonInMongo error", e);
		} finally {

		}

		return result;

	}

	/**********
	 * 根据多边形求范围内的车辆---读实时表
	 * 
	 * 
	 **/
	public static List<Map<String, Object>> queryRealInfoByPolygon2(MongoClient conn, String db, List<String> fields,
			List<double[]> polygons, int skip, int limit) throws DbException {
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		try {
			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(CAR_REAL_INFO);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			//// 2.test cycle query
			BasicDBObject condition = new BasicDBObject();

			///// condition.put( "vin", new BasicDBObject("$eq", vin) );
			// condition.put( "vin", vin);
			//
			// condition.put( "recordDate", new BasicDBObject("$gte",
			// beginDate).append("$lte", endDate) );

			/////////////////////////////////////////
			List a_polygons = new ArrayList();
			a_polygons.add(polygons);

			condition.put(F_LOCATION, new BasicDBObject("$geoWithin", new BasicDBObject("$geometry",
					new BasicDBObject("type", "Polygon").append("coordinates", a_polygons))));

			logger.info("condition=" + condition);

			////////////////////////////////////////////
			BasicDBObject fieldDb = new BasicDBObject();

			if (fields != null && fields.size() > 0) {
				for (String field : fields) {
					fieldDb.put(field, 1);
				}

			}
			logger.info("=====>fieldDb=" + fieldDb);
			/////////////////////////////////////////////////////////////

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(condition).skip(skip).limit(limit);
				else
					findIterable = collection.find(condition).projection(fieldDb).skip(skip).limit(limit);
			} else if (skip == 0 && limit > 0) {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(condition).limit(limit);
				else
					findIterable = collection.find(condition).projection(fieldDb).limit(limit);
			} else {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(condition);
				else
					findIterable = collection.find(condition).projection(fieldDb);
			}

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				HashMap<String, Object> map = new HashMap<String, Object>();

				for (String key : keys) {
					map.put(key, d.get(key));
				}

				result.add(map);

			}

			logger.info("count=" + count);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryRealInfoByPolygon2", e);

			throw new DbException(2015, "queryRealInfoByPolygon2 error", e);
		} finally {

		}

		return result;

	}
	///////////////////////////////////////////////////////////////////////////////////////

	/******
	 * 2018-5-16 liaoxb add
	 *
	 * 需求1：输入条件，多车vin，查询mongo表car_real_info的车辆状态、经纬度、车速
	 *
	 **/
	public static List<Map<String, Object>> queryRealCarByIds(MongoClient conn, String db, String colName,
			List<String> carIds, List<String> fields, int skip, int limit) throws DbException {
		List<Map<String, Object>> result = new ArrayList<>();

		if (carIds == null || carIds.size() == 0)
			throw new DbException(3001, "queryRealCarBy error", new IllegalArgumentException("carIds is null"));

		try {

			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			String s_query = "";

			for (String carId : carIds) {
				s_query += "{\"" + F_VEHICLEID + "\":\"" + carId + "\"},";
			}
			if (s_query != null && s_query.length() > 0)
				/*
				 * 去掉最后一个
				 */
				s_query = s_query.substring(0, s_query.length() - 1);

			s_query = "{$or:[" + s_query + "]}";

			BasicDBObject queryDb = BasicDBObject.parse(s_query);
			logger.info("=====>queryDb=" + queryDb);

			////////////////////////////////////////////
			BasicDBObject fieldDb = new BasicDBObject();

			if (fields != null && fields.size() > 0) {
				for (String field : fields) {
					fieldDb.put(field, 1);
				}

			}
			logger.info("=====>fieldDb=" + fieldDb);
			////////////////////////////////////////////////////////

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(queryDb).skip(skip).limit(limit);
				else
					findIterable = collection.find(queryDb).projection(fieldDb).skip(skip).limit(limit);
			} else if (skip == 0 && limit > 0) {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(queryDb).limit(limit);
				else
					findIterable = collection.find(queryDb).projection(fieldDb).limit(limit);
			} else {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(queryDb);
				else
					findIterable = collection.find(queryDb).projection(fieldDb);
			}

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				HashMap<String, Object> map = new HashMap<String, Object>();

				for (String key : keys) {
					map.put(key, d.get(key));
				}

				result.add(map);

			}

			logger.info("count=" + count);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryRealCarByIds", e);

			throw new DbException(3002, "queryRealCarByIds error", e);
		} finally {

		}

		return result;
	}

	///////////////////////////////////////////////////////////////////////////////////////

	/******
	 * 2018-10-17 wangmh add
	 *
	 * 需求：查询所有的实时车辆
	 *
	 **/
	public static List<Map<String, Object>> queryAllRealCars(MongoClient conn, String db, String colName,
			List<String> fields) throws DbException {
		List<Map<String, Object>> result = new ArrayList<>();
		try {

			if (conn == null)
				conn = mongoClient;
			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			//// System.out.println("集合选择成功");

			BasicDBObject fieldDb = new BasicDBObject();

			if (fields != null && fields.size() > 0) {
				for (String field : fields) {
					fieldDb.put(field, 1);
				}
			}
			logger.info("=====>fieldDb=" + fieldDb);

			FindIterable<Document> findIterable = null;

			if (fields == null || fields.size() == 0) {
				findIterable = collection.find();
			} else {
				findIterable = collection.find().projection(fieldDb);
			}

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				HashMap<String, Object> map = new HashMap<String, Object>();

				for (String key : keys) {
					map.put(key, d.get(key));
				}
				result.add(map);
			}
			logger.info("count=" + count);

		} catch (Exception e) {
			e.printStackTrace();
			logger.error("in queryAllRealCars", e);
			throw new DbException(3002, "queryAllRealCars error", e);
		} finally {

		}

		return result;
	}

	///////////////////////////////////////////////////////////

	/*****
	 * 2018-5-23 liaoxb add
	 *
	 * 将"position": {"lontitude": -73.97, "lattitude": 40.77}
	 *
	 * 转为mongo识别的地理坐标点格式
	 *
	 * "position": { "type": "Point", "coordinates": [ -73.97, 40.77 ] }
	 *
	 *
	 **/
	public static String convertLocation(String fieldName, String lon, String lat) {

		String res = "\"" + fieldName + "\": { \"type\": \"Point\", \"coordinates\": [ " + lon + ", " + lat + " ] } ";

		return res;

	}

	public static JSONObject getLocation(Double longitude, Double latitude) {
		JSONObject json = new JSONObject();
		json.put("type", "Point");
		JSONArray jsonArray = new JSONArray();
		jsonArray.add(longitude);
		jsonArray.add(latitude);
		json.put("coordinates", jsonArray);
		return json;
	}

	public static JSONObject getHistoryData(Map<String, Object> historyMap) {
		JSONObject json = new JSONObject();
		json.put(MongoUtilsForV2x.F_ID, historyMap.get(MongoUtilsForV2x.F_ID).toString());
		json.put(MongoUtilsForV2x.F_VEHICLEID, historyMap.get(MongoUtilsForV2x.F_VEHICLEID));
		json.put(MongoUtilsForV2x.F_RECORDDATE, historyMap.get(MongoUtilsForV2x.F_RECORDDATE));

		json.put(MongoUtilsForV2x.F_LOCATION, getLocation((Double) historyMap.get(MongoUtilsForV2x.F_LONGITUDE),
				(Double) historyMap.get(MongoUtilsForV2x.F_LATITUDE)));
		return json;
	}

	/////////////////////////////////////////////////////

	/****
	 * 2018-7-18 liaoxb add
	 *
	 * 输入道路的线描述坐标，求相交的区域记录 返回 List<String>, String 为json
	 *
	 **/
	public static List<String> queryZoneByRoad(MongoClient conn, String db, String colName, double[][] roadLines,
			int skip, int limit) throws DbException {
		List<String> result = new ArrayList<String>();
		try {

			if (conn == null)
				conn = mongoClient;

			// if(vin==null || vin.trim().equals("") ||
			// vin.trim().equals("null"))
			// {
			// return queryCarInfoByPolygon( conn, db, colName,
			// beginDate, endDate,
			// polygons,
			// skip, limit
			// );
			// }

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			//// 2.test cycle query
			BasicDBObject condition = new BasicDBObject();

			///// condition.put( "vin", new BasicDBObject("$eq", vin) );
			// condition.put( F_VIN, vin);

			//// condition.put( F_RECORDDATE, new BasicDBObject("$gte",
			//// beginDate).append("$lte", endDate) );

			/////////////////////////////////////////
			// List a_polygons = new ArrayList();
			// a_polygons.add(polygons);

			condition.put(F_LOCATION, new BasicDBObject("$geoIntersects", new BasicDBObject("$geometry",
					new BasicDBObject("type", "LineString").append("coordinates", roadLines))));

			logger.info("-------->condition=" + condition);

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				findIterable = collection.find(condition).skip(skip).limit(limit);
			} else if (skip == 0 && limit > 0) {
				findIterable = collection.find(condition).limit(limit);
			} else
				findIterable = collection.find(condition);

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				///// result.add(""+d.get(returnField));
				result.add(d.toJson());
			}

			logger.info("-------->count=" + count);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryZoneByRoad", e);

			throw new DbException(3001, "queryZoneByRoad error", e);
		} finally {

		}

		return result;

	}

	/*******
	 * 当idType是0时，id为String类型的rowkey idType是1时，id为Long类型的vehicleId
	 * idType是2时，id为ObjectId类型的_id (mongodb内置类型)
	 * 
	 **/
	public static String insertOrUpdateMongo2(MongoClient conn, String db, String colName, String idType, Object id,
			String json) throws DbException {
		String result = null;

		try {

			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);

			Document document = Document.parse(json);

			////// System.out.println("document"+document);

			///// db.a1.update({_id:{$eq:1003}},{$set:{_id:1003,name:'lxb',age:40}},true,false)

			///// collection.insertOne(document );

			Document filter = new Document();
			//// filter.append("_id", id); ////////////2018-9-3 liaoxb add
			//// 因为_id是hbase的rowkey，所以直接当字串传入即可，如果是mongo自己生成的主键，则需要转为filter.append("_id",
			//// new ObjectId(rowkey));

			if (idType == null || idType.trim().equals(""))
				idType = "0";

			if ("0".equals(idType.trim()))
				filter.append("_id", id);
			else if ("1".equals(idType.trim()))
				filter.append("_id", id);
			else if ("2".equals(idType.trim()))
				filter.append("_id", new ObjectId("" + id));

			// 注意update文档里要包含"$set"字段
			Document update = new Document();
			update.append("$set", document);

			/////////// 没有就新增，有就update
			UpdateOptions updateOptions = new UpdateOptions();
			updateOptions.upsert(true);

			UpdateResult res = collection.updateOne(filter, update, updateOptions);
			logger.info("matched count = " + res.getMatchedCount());
			logger.info("mongoDB insert successful.");
		} catch (Exception e) {
			// 
			e.printStackTrace();
			logger.error("in insertOrUpdateMongo2", e);

			throw new DbException(2001, "insertOrUpdateMongo2 error", e);
		}

		return result;

	}
	
	
	public static void insertOrUpdataMongoByRSUStatus(MongoClient conn, String db, String colName, String rsuId,
			int status) {

		if (conn == null) {
			conn = mongoClient;
		}

		MongoDatabase mongoDatabase = conn.getDatabase(db);

		MongoCollection<Document> collection = mongoDatabase.getCollection(colName);

		Document update = new Document();
		update.append("$set", new Document("isOnline", status));

		UpdateOptions updateOptions = new UpdateOptions();
		updateOptions.upsert(true);

		UpdateResult res = collection.updateOne(Filters.eq("rsuId", rsuId), update, updateOptions);
		logger.info("matched count = " + res.getMatchedCount());
		logger.info("mongoDB insert successful.");
		

	}

	public static String updateMongo2(MongoClient conn, String db, String colName, String idType, Object id, String json) throws DbException {
		String result = null;

		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);

			Document document = Document.parse(json);

			////// System.out.println("document"+document);

			///// db.a1.update({_id:{$eq:1003}},{$set:{_id:1003,name:'lxb',age:40}},true,false)

			///// collection.insertOne(document );

			Document filter = new Document();
			//// filter.append("_id", id);////new ObjectId
			//// ////因为_id是hbase的rowkey，所以直接当字串传入即可，如果是mongo自己生成的主键，则需要转为filter.append("_id",
			//// new ObjectId(rowkey));

			if (idType == null || idType.trim().equals(""))
				idType = "0";

			if ("0".equals(idType.trim()))
				filter.append("_id", id);
			else if ("1".equals(idType.trim()))
				filter.append("_id", id);
			else if ("2".equals(idType.trim()))
				filter.append("_id", new ObjectId("" + id));

			// 注意update文档里要包含"$set"字段
			Document update = new Document();
			update.append("$set", document);

			/////////// 没有就新增，有就update
			UpdateOptions updateOptions = new UpdateOptions();
			updateOptions.upsert(true);

			UpdateResult res = collection.updateOne(filter, update, updateOptions);
			logger.info("matched count = " + res.getMatchedCount());

		} catch (Exception e) {
			// 
			e.printStackTrace();
			logger.error("in updateMongo", e);

			throw new DbException(2011, "updateMongo error", e);
		}

		return result;

	}

	/**
	 * add wangmh 圆形条件查询
	 * 
	 * @param conn
	 * @param db
	 * @param colName
	 * @param fields
	 * @param jsonCondition
	 * @param polygons
	 * @param radius
	 * @param skip
	 * @param limit
	 * @return
	 * @throws DbException
	 */
	public static List<Map<String, Object>> queryCarInfoByCycleCond(MongoClient conn, String db, String colName,
			List<String> fields, String jsonCondition, double[] polygons, double radius, int skip, int limit)
					throws DbException {
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		try {
			if (conn == null) {
				conn = mongoClient;
			}
			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);

			// 查询条件
			if(StringUtils.isBlank(jsonCondition)){
				jsonCondition = "{}";
			}
			BasicDBObject condition =  BasicDBObject.parse(jsonCondition);


			// double[] polygons = {100.40111, 39.02222};

			List ls = new ArrayList();
			ls.add(polygons);
			// 3963.2英里，是地球半径
			ls.add(convertMiToYinLi(radius) / 3963.2);
			// ls.add(radius);

			//// $centerSphere 是MongoDB特有的语法,通过指定中心点和弧度半径来表示圆形区域。
			condition.put(F_LOCATION, new BasicDBObject("$geoWithin", new BasicDBObject("$centerSphere", ls)));

			logger.info("condition=" + condition);
			////////////////////////////////////////////
			BasicDBObject fieldDb = new BasicDBObject();

			if (fields != null && fields.size() > 0) {
//				for (String field : fields) {
//					fieldDb.put(field, 1);
//				}
				fields.forEach(field -> fieldDb.put(field, 1));
			}
			logger.info("=====>fieldDb=" + fieldDb);

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(condition).skip(skip).limit(limit);
				else
					findIterable = collection.find(condition).projection(fieldDb).skip(skip).limit(limit);

			} else if (skip == 0 && limit > 0) {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(condition).limit(limit);
				else
					findIterable = collection.find(condition).projection(fieldDb).limit(limit);
			} else {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(condition);
				else
					findIterable = collection.find(condition).projection(fieldDb);
			}

			long count = 0;
			for (Document d : findIterable) {
				count++;
				//Set<String> keys = d.keySet();
				HashMap<String, Object> map = new HashMap<String, Object>();
//				for (String key : keys) {
//					//map.put(key, d.get(key));
//					map.put(key, JSONObject.toJSON(d.get(key)));
//				}
				d.forEach((k, v) -> {
					map.put(k, JSONObject.toJSON(v));
				});
				result.add(map);

			}
			logger.info("count=" + count);

		} catch (Exception e) {
			e.printStackTrace();
			logger.error("in queryCarInfoByCycleCond", e);
			throw new DbException(2013, "queryCarInfoByCycleCond error", e);
		} finally {

		}

		return result;
	}

	////////////////////////////////////////////////////////////
	/******
	 * @author liaoxb
	 * 
	 *         2018-9-6 add
	 * 
	 *         按圆形查询范围内车辆----读历史表
	 * 
	 *         radius----半径：单位： 公里
	 * 
	 * 
	 **/
	public static List<Map<String, Object>> queryCarInfoByCycle4(MongoClient conn, String db, String colName,
			List<String> fields, String vehicleId, String beginDate, String endDate, double[] polygons, double radius,
			int skip, int limit) throws DbException {
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		try {
			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			//// 2.test cycle query
			BasicDBObject condition = new BasicDBObject();

			// 查询两个时间范围的，用map包装一下
			///// query3.put( "recordDate", new BasicDBObject("$gte",
			// 20180101000000L).append("$lte", 20500101000000L) );

			//// condition.put( "vin", new BasicDBObject("$eq", vin) );
			// if(vin!=null && !vin.trim().equals("") &&
			// !vin.trim().equals("null"))
			// condition.put( "vin", vin);

			/////////// condition.put( "recordDate", new BasicDBObject("$gte",
			/////////// beginDate).append("$lte", endDate) );

			if (vehicleId != null && !vehicleId.trim().equals("") && !vehicleId.trim().equals("null"))
				condition.put(F_VEHICLEID, vehicleId);

			if (beginDate != null && !beginDate.trim().equals("") && endDate != null && !endDate.trim().equals(""))
				condition.put(F_RECORDDATE, new BasicDBObject("$gte", beginDate).append("$lte", endDate));

			////////// 圆心---------------------------------------
			///// double[] polygons = {100.40111, 39.02222};

			List ls = new ArrayList();
			ls.add(polygons);
			/*
			 * 3963.2英里，是地球半径
			 */
			ls.add(convertMiToYinLi(radius) / 3963.2);
			///// ls.add(radius);

			//// $centerSphere 是MongoDB特有的语法,通过指定中心点和弧度半径来表示圆形区域。
			condition.put(F_LOCATION, new BasicDBObject("$geoWithin", new BasicDBObject("$centerSphere", ls)));

			logger.info("condition=" + condition);
			////////////////////////////////////////////
			BasicDBObject fieldDb = new BasicDBObject();

			if (fields != null && fields.size() > 0) {
				for (String field : fields) {
					fieldDb.put(field, 1);
				}

			}
			logger.info("=====>fieldDb=" + fieldDb);

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(condition).skip(skip).limit(limit);
				else
					findIterable = collection.find(condition).projection(fieldDb).skip(skip).limit(limit);

			} else if (skip == 0 && limit > 0) {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(condition).limit(limit);
				else
					findIterable = collection.find(condition).projection(fieldDb).limit(limit);
			} else {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(condition);
				else
					findIterable = collection.find(condition).projection(fieldDb);
			}

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				HashMap<String, Object> map = new HashMap<String, Object>();

				for (String key : keys) {
					map.put(key, d.get(key));
				}

				result.add(map);

			}

			logger.info("count=" + count);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryCarInfoByCycle4", e);

			throw new DbException(2013, "queryCarInfoByCycle4 error", e);
		} finally {

		}

		return result;

	}
	
	/**
	 * 多边形条件查询 add wangmh 2019-07-26
	 * 
	 * @param conn
	 * @param db
	 * @param colName
	 * @param fields
	 * @param jsonCondition
	 * @param polygons
	 * @param skip
	 * @param limit
	 * @return
	 * @throws DbException
	 */
	public static List<Map<String, Object>> queryCarInfoByPolygonCond(MongoClient conn, String db, String colName,
			List<String> fields,String jsonCondition, List<double[]> polygons, int skip,
			int limit) throws DbException {
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		
		try {
			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);

			// 查询条件
			if(StringUtils.isBlank(jsonCondition)){
				jsonCondition = "{}";
			}
			BasicDBObject condition =  BasicDBObject.parse(jsonCondition);

			List a_polygons = new ArrayList();
			a_polygons.add(polygons);

			condition.put(F_LOCATION, new BasicDBObject("$geoWithin", new BasicDBObject("$geometry",
					new BasicDBObject("type", "Polygon").append("coordinates", a_polygons))));

			logger.info("condition=" + condition);

			////////////////////////////////////////////
			BasicDBObject fieldDb = new BasicDBObject();

			if (fields != null && fields.size() > 0) {
//				for (String field : fields) {
//					fieldDb.put(field, 1);
//				}
				fields.forEach(field -> fieldDb.put(field, 1));
			}
			logger.info("=====>fieldDb=" + fieldDb);
			////////////////////////////////////////////////////////

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(condition).skip(skip).limit(limit);
				else
					findIterable = collection.find(condition).projection(fieldDb).skip(skip).limit(limit);
			} else if (skip == 0 && limit > 0) {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(condition).limit(limit);
				else
					findIterable = collection.find(condition).projection(fieldDb).limit(limit);
			} else {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(condition);
				else
					findIterable = collection.find(condition).projection(fieldDb);
			}

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//Set<String> keys = d.keySet();
				HashMap<String, Object> map = new HashMap<String, Object>();
//				for (String key : keys) {
//					map.put(key, d.get(key));
//				}
				d.forEach((k,v)->{map.put(k, JSONObject.toJSON(v));});
				result.add(map);
			}

			logger.info("------>count=" + count);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryCarInfoByPolygonCond", e);

			throw new DbException(2025, "queryCarInfoByPolygonCond error", e);
		} finally {

		}

		return result;
	}

	/*******
	 * 2018-9-5 liaoxb add
	 * 
	 * 按照vin， 开始时间，结束时间， 多边形， 返回字段，查询多边形范围内的车辆数据---读历史表
	 * 
	 * 当vin为空，跳过vin条件；当开始时间，结束时间为空时，会跳过这2条件
	 * 
	 * 多边形数组不能为空
	 * 
	 * 
	 **/
	public static List<Map<String, Object>> queryCarInfoByPolygon4(MongoClient conn, String db, String colName,
			List<String> fields, String vehicleId, String beginDate, String endDate, List<double[]> polygons, int skip,
			int limit) throws DbException {
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		try {
			if (conn == null)
				conn = mongoClient;

			// if(vin==null || vin.trim().equals("") ||
			// vin.trim().equals("null"))
			// {
			// return queryCarInfoByPolygon( conn, db, colName,
			// beginDate, endDate,
			// polygons,
			// skip, limit
			// );
			// }

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			//// 2.test cycle query
			BasicDBObject condition = new BasicDBObject();

			///// condition.put( "vin", new BasicDBObject("$eq", vin) );

			if (vehicleId != null && !vehicleId.trim().equals("") && !vehicleId.trim().equals("null"))
				condition.put(F_VEHICLEID, vehicleId);

			if (beginDate != null && !beginDate.trim().equals("") && endDate != null && !endDate.trim().equals(""))
				condition.put(F_RECORDDATE, new BasicDBObject("$gte", beginDate).append("$lte", endDate));

			/////////////////////////////////////////
			List a_polygons = new ArrayList();
			a_polygons.add(polygons);

			condition.put(F_LOCATION, new BasicDBObject("$geoWithin", new BasicDBObject("$geometry",
					new BasicDBObject("type", "Polygon").append("coordinates", a_polygons))));

			logger.info("condition=" + condition);

			////////////////////////////////////////////
			BasicDBObject fieldDb = new BasicDBObject();

			if (fields != null && fields.size() > 0) {
				for (String field : fields) {
					fieldDb.put(field, 1);
				}
			}
			logger.info("=====>fieldDb=" + fieldDb);
			////////////////////////////////////////////////////////

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(condition).skip(skip).limit(limit);
				else
					findIterable = collection.find(condition).projection(fieldDb).skip(skip).limit(limit);
			} else if (skip == 0 && limit > 0) {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(condition).limit(limit);
				else
					findIterable = collection.find(condition).projection(fieldDb).limit(limit);
			} else {
				if (fields == null || fields.size() == 0)
					findIterable = collection.find(condition);
				else
					findIterable = collection.find(condition).projection(fieldDb);
			}

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				HashMap<String, Object> map = new HashMap<String, Object>();

				for (String key : keys) {
					map.put(key, d.get(key));
				}

				result.add(map);
			}

			logger.info("------>count=" + count);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryCarInfoByPolygon4", e);

			throw new DbException(2025, "queryCarInfoByPolygon4 error", e);
		} finally {

		}

		return result;

	}

	/******
	 * 2018-9-28 根据事件点经纬度，得到距离最近的mec——id
	 * 
	 * @author liaoxiaobo
	 * 
	 *         double[0]----lon double[1]----lat
	 * 
	 **/
	public static HashMap<String, Object> getMecInfoByPoint(String db, double[] points, double maxDistance)
			throws DbException {
		HashMap<String, Object> map = new HashMap<String, Object>();

		try {

			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection("mec_info");
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////

			//// 2.test cycle query
			BasicDBObject condition = new BasicDBObject();

			// condition.put(F_LOCATION, new BasicDBObject("$near",
			// new BasicDBObject("$geometry",
			// new BasicDBObject("type","Point")
			// .append("coordinates", points))));

			condition.put(F_LOCATION,
					new BasicDBObject("$near",
							new BasicDBObject("$geometry",
									new BasicDBObject("type", "Point").append("coordinates", points))
											.append("$maxDistance", maxDistance))

			);

			logger.info("condition=" + condition);

			FindIterable<Document> findIterable = collection.find(condition).limit(1);

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				for (String key : keys) {
					map.put(key, d.get(key));
				}

			}

			logger.info("count=" + count);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in getMecInfoByPoint", e);

			throw new DbException(3002, "getMecInfoByPoint error", e);
		} finally {

		}

		return map;
	}

	/******
	 * 2018-9-28 根据事件点经纬度，得到最近的rsu——id
	 * 
	 * @author liaoxiaobo
	 * 
	 *         double[0]----lon double[1]----lat
	 * 
	 *         limit:表示限制返回后多少条记录
	 * 
	 *         返回的记录时按离事件坐标点的距离从小到大排序
	 * 
	 **/
	public static List<HashMap<String, Object>> getRsuInfoByPoint(String db, String mec_id, double[] points,
			double maxDistance, int limit) throws DbException {
		List<HashMap<String, Object>> res = new ArrayList<HashMap<String, Object>>();

		try {
			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection("rsu_info");
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////

			//// 2.test cycle query
			BasicDBObject condition = new BasicDBObject();

			if (mec_id != null && !mec_id.trim().equals("") && !mec_id.trim().equals("null"))
				condition.put("mec_id", mec_id);

			///// double miles=convertMiToYinLi(maxDistance); ///////将米转英里

			/******
			 * 
			 * 
			 * db.cars.find( { location: { $near: { $geometry: { type: "Point",
			 * coordinates: [ -73.9667, 40.78 ] }, $minDistance: 0,
			 * $maxDistance: 5000 } } } )
			 * 
			 **/
			condition.put(F_LOCATION,
					new BasicDBObject("$near",
							new BasicDBObject("$geometry",
									new BasicDBObject("type", "Point").append("coordinates", points))
											.append("$maxDistance", maxDistance))

			);

			logger.info("-------------condition=" + condition);

			FindIterable<Document> findIterable = collection.find(condition);

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				HashMap<String, Object> map = new HashMap<String, Object>();

				for (String key : keys) {
					map.put(key, d.get(key));
				}

				res.add(map);

			}

			logger.info("----------------------------count=" + count);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in getRsuInfoByPoint", e);

			throw new DbException(3002, "getRsuInfoByPoint error", e);
		} finally {

		}

		return res;
	}

	public static List<HashMap<String, Object>> getRsuInfoByPoint2(String db, double[] points, double maxDistance,
			int limit) throws DbException {
		List<HashMap<String, Object>> res = new ArrayList<HashMap<String, Object>>();

		try {
			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection("rsu_info");
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////

			//// 2.test cycle query
			BasicDBObject condition = new BasicDBObject();

			// if(mec_id!=null && !mec_id.trim().equals("") &&
			// !mec_id.trim().equals("null"))
			// condition.put("mec_id", mec_id);

			///// double miles=convertMiToYinLi(maxDistance); ///////将米转英里

			/******
			 * 
			 * 
			 * db.cars.find( { location: { $near: { $geometry: { type: "Point",
			 * coordinates: [ -73.9667, 40.78 ] }, $minDistance: 0,
			 * $maxDistance: 5000 } } } )
			 * 
			 **/
			condition.put(F_LOCATION,
					new BasicDBObject("$near",
							new BasicDBObject("$geometry",
									new BasicDBObject("type", "Point").append("coordinates", points))
											.append("$maxDistance", maxDistance))

			);

			logger.info("-------------condition=" + condition);

			FindIterable<Document> findIterable = collection.find(condition).limit(limit);

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				HashMap<String, Object> map = new HashMap<String, Object>();

				for (String key : keys) {
					map.put(key, d.get(key));
				}

				res.add(map);

			}

			logger.info("----------------------------count=" + count);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in getRsuInfoByPoint2", e);

			throw new DbException(3022, "getRsuInfoByPoint2 error", e);
		} finally {

		}

		return res;
	}

	/**********
	 * 2018-10-19 组装报文：RC0103_非实时类事件通知推送
	 * 
	 * @author liaoxiaobo
	 * 
	 * 
	 *         返回 countTodo 待处理事件数 INT M countInprogress 处理中事件数 INT M countDone
	 *         已处理事件数 INT
	 * 
	 **/
	public static long[] generateRc0103(String db, String tableName, int regionId, int eventType) {
		long[] res = new long[3];

		try {
			MongoDatabase mongoDatabase = mongoClient.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

			/******
			 * ENUM_RCEVENT_STATUS（资源协同事件状态） 值代码说明 0 RCEVENT_STATUS_UNKNOWN 未知 1
			 * RCEVENT_STATUS_TODO 待处理 2 RCEVENT_STATUS_INPROGRESS 处理中 3
			 * RCEVENT_STATUS_DONE 已处理
			 ******/

			String json = "";
			Document condition = null;
			json = "{\"regionId\":" + regionId + ", \"eventType\":" + eventType + ", \"status\":1}";
			condition = Document.parse(json);

			res[0] = collection.count(condition);
			//////////////////////////////////////////////////////////
			json = "{\"regionId\":" + regionId + ", \"eventType\":" + eventType + ", \"status\":2}";
			condition = Document.parse(json);

			res[1] = collection.count(condition);
			/////////////////////////////////////////////////////////
			json = "{\"regionId\":" + regionId + ", \"eventType\":" + eventType + ", \"status\":3}";
			condition = Document.parse(json);

			res[2] = collection.count(condition);

			logger.info("res[0]=" + res[0] + ", res[1]=" + res[1] + ", res[2]=" + res[2]);

		} catch (Exception e) {
			// 
			e.printStackTrace();
			logger.error("in generateRc0103", e);

		}

		return res;
	}

	/*****
	 * 
	 * 2018-10-19 组装报文：RC0103_非实时类事件通知推送
	 * 
	 * @author sharphero
	 * 
	 */
	public static String generateRc0103Json(String db, String tableName, int regionId, int eventType) {
		String res = "";

		long[] arr = generateRc0103(db, tableName, regionId, eventType);

		long timestamp = (new java.util.Date()).getTime();

		res = "[ { \"regionId\":" + regionId + ",\"eventType\":" + eventType + ",\"countTodo\":" + arr[0]
				+ ",\"countInprogress\":" + arr[1] + ",\"countDone\":" + arr[2] + ",\"timestamp\":" + timestamp + "}]";

		return res;
	}

	///////////////////////////////////////////////////////////

	/****
	 * 
	 * 写一条记录到表中 throws DbException
	 * 
	 **/
	public static String insertDoc(MongoClient conn, String db, String colName, Document document) {
		String result = null;

		try {

			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);

			collection.insertOne(document);
			/////// logger.info("mongoDB insert successful.");

		} catch (Exception e) {
			// 
			e.printStackTrace();
			logger.error("in insertDoc:" + document, e);

			/////// throw new DbException(4013, "insertDoc", e);
		}

		return result;

	}

	/****
	 * 
	 * 写或更新一条记录 throws DbException
	 * 
	 **/
	public static String insertOrUpdateDoc(MongoClient conn, String db, String colName, String rowkey, Document doc) {
		String result = null;

		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);

			Document filter = new Document();
			// filter.append("_id", new ObjectId(rowkey)); ////////////2018-9-3
			// liaoxb add
			/*
			 * 2018-9-3 liaoxb add
			 */
			filter.append("_id", rowkey);

			// 注意update文档里要包含"$set"字段
			Document update = new Document();
			update.append("$set", doc);

			/////////// 没有就新增，有就update
			UpdateOptions updateOptions = new UpdateOptions();
			updateOptions.upsert(true);

			UpdateResult res = collection.updateOne(filter, update, updateOptions);
			//// logger.info("matched count = " + res.getMatchedCount());
			//// logger.info("mongoDB insertOrUpdateDoc successful.");
		} catch (Exception e) {
			// 
			e.printStackTrace();
			logger.error("in insertOrUpdateDoc:" + doc, e);

			/////// throw new DbException(4014, "insertOrUpdateDoc error",e);
		}

		return result;

	}

	/*******
	 * 读任务进度
	 * 
	 * 
	 **/
	public static Long getEndTime(MongoClient conn, String db, String tableName) throws DbException {
		Long endTime = null;

		try {

			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////

			//// 2.test cycle query
			BasicDBObject condition = new BasicDBObject();

			condition.put("_id", "1");

			logger.info("condition=" + condition);

			FindIterable<Document> findIterable = collection.find(condition).limit(1);

			for (Document d : findIterable) {
				endTime = d.getLong("endTime");
			}

			logger.info("endTime=" + endTime);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in getEndTime", e);

			throw new DbException(4012, "getEndTime error", e);
		} finally {

		}

		return endTime;

	}

	/*******
	 * 读上次任务的最后一条记录
	 * 
	 * 
	 **/
	public static Document getLastDoc(MongoClient conn, String db, String tableName) throws DbException {
		Document doc = null;

		try {
			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(db);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////

			//// 2.test cycle query
			BasicDBObject condition = new BasicDBObject();

			condition.put("_id", "2");

			logger.info("condition=" + condition);

			FindIterable<Document> findIterable = collection.find(condition).limit(1);

			for (Document d : findIterable) {
				doc = d;
			}

			if (doc != null) {
				Object tmpxid = doc.get("tmpxid");

				doc.put("_id", tmpxid);
				doc.remove("tmpxid");
			}

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in getLastDoc", e);

			throw new DbException(4012, "getLastDoc error", e);
		} finally {

		}

		return doc;

	}

	/****
	 * 更新剔重任务进度
	 * 
	 * 
	 * 
	 **/
	public static void updateEndTime(MongoClient conn, String db, String tableName, Long endTime) throws DbException {
		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

			java.util.Date date = new java.util.Date();
			long updateTime = date.getTime();
			String valJson = "{ \"endTime\":" + endTime + ", \"updateTime\":" + updateTime + "}";
			logger.info("valJson" + valJson);
			Document document = Document.parse(valJson);

			////// System.out.println("document"+document);

			///// db.a1.update({_id:{$eq:1003}},{$set:{_id:1003,name:'lxb',age:40}},true,false)

			///// collection.insertOne(document );

			String whereJson = "{\"_id\":\"1\"}";
			Document filter = Document.parse(whereJson);

			// 注意update文档里要包含"$set"字段
			Document update = new Document();
			update.append("$set", document);

			/////////// 没有就新增，有就update
			UpdateOptions updateOptions = new UpdateOptions();
			updateOptions.upsert(true);

			UpdateResult res = collection.updateMany(filter, update, updateOptions);

			////// logger.info("matched count = " + res.getMatchedCount());

		} catch (Exception e) {
			// 
			e.printStackTrace();
			logger.error("in updateEndTime", e);

			throw new DbException(4011, "updateEndTime error", e);
		}

	}

//	/*********
//	 * 2018-11-27 liaoxb 增加剔重功能
//	 * 
//	 * 按event, hvid, rvid, 判断2条记录gpstime<5s的则为重复记录
//	 * 
//	 * offset: 5s
//	 * 
//	 * 
//	 * db.u_obu_event_dup.remove({}); db.u_event_dup_process.remove({});
//	 * 
//	 * endTime: 传入当前系统时间, 暂不使用此参数
//	 * 
//	 * 重复处理的记录写不进表dup
//	 * 
//	 * 
//	 * 缺点:每次扫描，本次扫描记录的最后1条需要等到下次扫描时才可以被处理
//	 * 
//	 * 避免最后一条延时也可以这样做： 最后1条暂时写入dup表，做好标记(记录_id写到process表中)，进度不变
//	 * 下次扫描时先删除标记的记录，重新把上次未处理的最后1条记录和新纪录重新扫描处理。
//	 * 
//	 * 
//	 **/
//	public static void doDup(MongoClient conn, String db, String tableName, Long endTime, int offset)
//			throws DbException {
//
//		try {
//
//			if (conn == null)
//				conn = mongoClient;
//
//			MongoDatabase mongoDatabase = conn.getDatabase(db);
//
//			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);
//
//			BasicDBObject condition = new BasicDBObject();
//
//			String processTable = "u_event_dup_process";
//			String eventTable = "u_obu_event_dup";
//
//			////// 读出进度 --------------------------------------------------
//			Long beginTime = getEndTime(conn, db, processTable);
//
//			////// 读上一次没有处理的最后一条记录
//			// Document last_doc=getLastDoc( conn, db, "u_event_dup_process");
//			//
//			// logger.info("----------->beginTime="+beginTime);
//			// logger.info("----------->endTime="+endTime);
//			// logger.info("----------->last_doc="+last_doc);
//
//			// if(beginTime!=null && beginTime!=0 && endTime!=null &&
//			// endTime!=0)
//			// condition.put("gpstime", new BasicDBObject("$gte",
//			// beginTime).append("$lt", endTime)); ///// beginTime=< gpstime <
//			// endTime
//			// else if(beginTime==null || beginTime==0 )
//			// condition.put("gpstime", new BasicDBObject("$lt", endTime));
//
//			if (beginTime != null && beginTime != 0 && endTime != null && endTime != 0)
//				condition.put("gpstime", new BasicDBObject("$gte", beginTime)); ///// beginTime=<
//																				///// gpstime
//
//			logger.info("-------------condition=" + condition);
//
//			String orderJson = "{\"event\":1, \"hvid\":1, \"rvid\":1, \"gpstime\":1 }";
//			BasicDBObject orderDb = BasicDBObject.parse(orderJson);
//			logger.info("=====>orderDb=" + orderDb);
//
//			FindIterable<Document> findIterable = collection.find(condition).sort(orderDb);
//
//			long count = 0;
//
//			List<Document> ls = new ArrayList<Document>();
//
//			// if(last_doc!=null)
//			// ls.add(last_doc); ////////////加入上次没处理的最后一条记录，进行重复判断
//
//			for (Document d : findIterable) {
//
//				count++;
//
//				ls.add(d);
//			}
//			logger.info("--------------------------------->ls=" + ls.size() + ", count=" + count);
//
//			//////////////////////////////////////////////
//
//			// if(ls!=null && ls.size()>0)
//			// {
//			//
//			// long bTime =0L;
//			// long eTime =0L;
//			//
//			//
//			// for(int i=0;i<ls.size()-1;i++)
//			// {
//			// Document tmp_map=ls.get(i);
//			//
//			// Document tmp_map2=ls.get(i+1);
//			//
//			//
//			// int event1 =(Integer)tmp_map.get("event");
//			// String hvid1 =""+tmp_map.get("hvid");
//			// String rvid1 =""+tmp_map.get("rvid");
//			// long gpstime1=(Long)tmp_map.get("gpstime");
//			//
//			// if(i==0)
//			// bTime=gpstime1;
//			//
//			// ///////////////////////////////////////////////////////
//			// int event2 =(Integer)tmp_map2.get("event");
//			// String hvid2 =""+tmp_map2.get("hvid");
//			// String rvid2 =""+tmp_map2.get("rvid");
//			// long gpstime2=(Long)tmp_map2.get("gpstime");
//			//
//			// if(event1==event2 && hvid1.equals(hvid2) && rvid1.equals(rvid2) )
//			// //////事件类型，hvid, rvid相同
//			// {
//			// if((gpstime2-gpstime1)<offset) //////////小于5s
//			// {
//			// logger.info("==================>skip line="+i);
//			// }
//			// else
//			// {
//			//
//			// ////logger.info("======>line["+i+"]="+tmp_map);
//			// tmp_map.put("beginTime", bTime);
//			// ///////////////////写相同区间记录的第一条记录的gpstime
//			// tmp_map.put("endTime", gpstime1);
//			// ///////////////////写相同区间记录的最后一条记录的gpstime
//			//
//			// insertDoc( conn, db, "u_obu_event_dup", tmp_map);
//			//
//			// bTime=gpstime2; ///////////开始时间设置为后1条的gpstime
//			// }
//			//
//			// }
//			// else /////本条和下一条 event,hvid, rvid,有不同，本条可以入库
//			// {
//			//
//			// tmp_map.put("beginTime", bTime);
//			// ///////////////////写相同区间记录的第一条记录的gpstime
//			// tmp_map.put("endTime", gpstime1);
//			// ///////////////////写相同区间记录的最后一条记录的gpstime
//			//
//			// insertDoc( conn, db, "u_obu_event_dup", tmp_map);
//			//
//			// bTime=gpstime2; ///////////开始时间设置为后1条的gpstime
//			//
//			// //////logger.info("======>line["+i+"]="+tmp_map);
//			// }
//			//
//			// }
//			//
//			//
//			// last_doc=ls.get(ls.size()-1);///////////取出最后一条的记录，存在mongo中，参与下次排序
//			// last_doc.put("tmpxid", last_doc.get("_id"));
//			// last_doc.put("_id", "2");
//			//
//			//
//			// if(count>0)
//			// insertOrUpdateDoc( conn, db, "u_event_dup_process", "2",
//			// last_doc);/////最后一条写入进度表,没查询到记录，则不更新
//			//
//			//
//			//
//			// ////////////////////更新进度
//			// -------------------没有查询到记录，则不更新进度表中_id=1,2的记录
//			// if(count>0)
//			// updateEndTime( conn, db, "u_event_dup_process", endTime);
//			//
//			// }
//
//			List<EventCol> ecs = new ArrayList<EventCol>();
//			EventCol ec = new EventCol();
//
//			if (ls != null && ls.size() > 0) {
//
//				for (int i = 0; i < ls.size() - 1; i++) {
//					Document tmp_map = ls.get(i);
//
//					Document tmp_map2 = ls.get(i + 1);
//
//					int event1 = (Integer) tmp_map.get("event");
//					String hvid1 = "" + tmp_map.get("hvid");
//					String rvid1 = "" + tmp_map.get("rvid");
//					long gpstime1 = Long.parseLong("" + tmp_map.get("gpstime"));
//
//					if (i == 0) {
//						ec = new EventCol();
//						ec.setEvent(event1);
//						ec.setHvid(hvid1);
//						ec.setRvid(rvid1);
//						ec.setBeginTime(gpstime1);
//					}
//
//					///////////////////////////////////////////////////////
//					int event2 = (Integer) tmp_map2.get("event");
//					String hvid2 = "" + tmp_map2.get("hvid");
//					String rvid2 = "" + tmp_map2.get("rvid");
//
//					// String xxx=""+tmp_map2.get("gpstime");
//					// logger.info("-i=["+i+"]------------->xxxx="+xxx);
//
//					long gpstime2 = Long.parseLong("" + tmp_map2.get("gpstime"));
//
//					if (event1 == event2 && hvid1.equals(hvid2) && rvid1.equals(rvid2)) ////// 事件类型，hvid,
//																						////// rvid相同
//					{
//						if ((gpstime2 - gpstime1) < offset) ////////// 小于5s
//						{
//							////// logger.info("==================>skip
//							////// line="+i);
//							ec.add(tmp_map);
//						} else {
//							ec.setEndTime(gpstime1);
//							ec.add(tmp_map);
//
//							ecs.add(ec);
//
//							ec = new EventCol();
//							ec.setEvent(event2);
//							ec.setHvid(hvid2);
//							ec.setRvid(rvid2);
//							ec.setBeginTime(gpstime2);
//						}
//
//					} else ///// 本条和下一条 event,hvid, rvid,有不同，本条可以入库
//					{
//
//						ec.setEndTime(gpstime1);
//						ec.add(tmp_map);
//
//						ecs.add(ec);
//
//						ec = new EventCol();
//						ec.setEvent(event2);
//						ec.setHvid(hvid2);
//						ec.setRvid(rvid2);
//						ec.setBeginTime(gpstime2);
//					}
//
//				}
//
//				//
//				// last_doc=ls.get(ls.size()-1);///////////取出最后一条的记录，存在mongo中，参与下次排序
//				// last_doc.put("tmpxid", last_doc.get("_id"));
//				// last_doc.put("_id", "2");
//				// last_doc.put("beginTime", ec.getEndTime().longValue());
//				//
//				//
//				// if(count>0)
//				// insertOrUpdateDoc( conn, db, "u_event_dup_process", "2",
//				// last_doc);/////最后一条写入进度表,没查询到记录，则不更新
//
//				for (EventCol tmpVo : ecs) {
//
//					Document tmpDoc = tmpVo.rss.get(0);
//
//					tmpDoc.put("beginTime", tmpVo.getBeginTime().longValue());
//					tmpDoc.put("endTime", tmpVo.getEndTime().longValue());
//
//					logger.info("----->" + tmpVo.toString());
//
//					insertDoc(conn, db, eventTable, tmpDoc);
//
//				}
//				logger.info("=======>ecs.size=" + ecs.size());
//
//				//////////////////// 更新进度 -------------------更新进度表中为最后
//				if (ecs != null && ecs.size() > 0)
//					updateEndTime(conn, db, processTable, ecs.get(ecs.size() - 1).getEndTime().longValue());
//
//			}
//
//		} catch (Exception e) {
//			// 
//			e.printStackTrace();
//
//			logger.error("in doDup", e);
//
//		}
//
//	}

	/*****
	 * 2019-3-13 wangmh add
	 * 
	 * 按车id，开始时间，结束时间查询视频文件
	 * 
	 * dbName: qdyktest tableName: t_video
	 * 
	 * 
	 **/
	public static MongoResultVo doQueryVideo(MongoClient conn, String dbName, String tableName, String fileId,
			String vid, String roadname, String roadpointname, String camId, String source, String beginTime,
			String endTime, String status, String plateNo, String camtype, String orderJson, int skip, int limit)
					throws DbException {

		MongoResultVo resultVo = new MongoResultVo();

		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		long total = 0L;
		try {

			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(dbName);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			////// BasicDBObject condition = new BasicDBObject();

			// 查询两个时间范围的，用map包装一下
			///// query3.put( "recordDate", new BasicDBObject("$gte",
			// 20180101000000L).append("$lte", 20500101000000L) );

			//// condition.put( "vin", new BasicDBObject("$eq", vin) );
			// if(vin!=null && !vin.trim().equals("") &&
			// !vin.trim().equals("null"))
			// condition.put( "vin", vin);

			/////////// condition.put( "recordDate", new BasicDBObject("$gte",
			/////////// beginDate).append("$lte", endDate) );
			String jsonCondition = "";

			BasicDBObject cond = new BasicDBObject();

			///////////////////////////////////////////
			if (beginTime == null || beginTime.trim().equals("") || beginTime.trim().equals("null"))
				beginTime = "20190101000000";

			if (endTime == null || endTime.trim().equals("") || endTime.trim().equals("null"))
				endTime = "20500308122100";

			jsonCondition = "{ " + " $or: [ " + " {$and:[{\"beginTime\":{\"$gte\":\"" + beginTime + "\",\"$lte\":\""
					+ endTime + "\"}},{\"endTime\":{\"$gte\":\"" + beginTime + "\", \"$lte\":\"" + endTime + "\"}}]},  "
					+ " {$and:[{\"beginTime\":{\"$gte\":\"" + beginTime + "\",\"$lte\":\"" + endTime
					+ "\"}},{\"endTime\":{\"$gte\":\"" + beginTime + "\", \"$gte\":\"" + endTime + "\"}}]},  "
					+ " {$and:[{\"beginTime\":{\"$lte\":\"" + beginTime + "\",\"$lte\":\"" + endTime
					+ "\"}},{\"endTime\":{\"$gte\":\"" + beginTime + "\", \"$lte\":\"" + endTime + "\"}}]},  "
					+ " {$and:[{\"beginTime\":{\"$lte\":\"" + beginTime + "\",\"$lte\":\"" + endTime
					+ "\"}},{\"endTime\":{\"$gte\":\"" + beginTime + "\", \"$gte\":\"" + endTime + "\"}}]}   " + " ] }";

			cond = BasicDBObject.parse(jsonCondition);

			if (fileId != null && !fileId.trim().equals("") && !fileId.trim().equals("null")) {
				Pattern pattern = Pattern.compile("^.*" + fileId + ".*$", Pattern.CASE_INSENSITIVE);

				cond.put("fileid", pattern);
			}

			if (vid != null && !vid.trim().equals("") && !vid.trim().equals("null")) {
				Pattern pattern = Pattern.compile("^.*" + vid + ".*$", Pattern.CASE_INSENSITIVE);

				cond.put("vid", pattern);
			}
			if (roadname != null && !roadname.trim().equals("") && !roadname.trim().equals("null")) {
				Pattern pattern = Pattern.compile("^.*" + roadname + ".*$", Pattern.CASE_INSENSITIVE);

				cond.put("roadname", pattern);
			}
			if (roadpointname != null && !roadpointname.trim().equals("") && !roadpointname.trim().equals("null")) {
				Pattern pattern = Pattern.compile("^.*" + roadpointname + ".*$", Pattern.CASE_INSENSITIVE);

				cond.put("roadpointname", pattern);
			}

			if (camId != null && !camId.trim().equals("") && !camId.trim().equals("null")) {
				Pattern pattern = Pattern.compile("^.*" + camId + ".*$", Pattern.CASE_INSENSITIVE);

				cond.put("camid", pattern);
			}

			if (source != null && !source.trim().equals("") && !source.trim().equals("null")) {
				cond.put("source", source);
			}

			if (status != null && !status.trim().equals("") && !status.trim().equals("null")) {
				cond.put("status", status);
			}
			if (camtype != null && !camtype.trim().equals("") && !camtype.trim().equals("null")) {
				cond.put("camtype", camtype);
			}

			if (plateNo != null && !plateNo.trim().equals("") && !plateNo.trim().equals("null")) {
				Pattern pattern = Pattern.compile("^.*" + plateNo + ".*$", Pattern.CASE_INSENSITIVE);

				cond.put("plateNo", pattern);
			}

			///////////////////////////////////////////
			// if(fileId!=null && !fileId.trim().equals("") &&
			// !fileId.trim().equals("null"))
			// jsonConditionBuff.append("\"fileid\":{$regex:/"+fileId+"/},");
			//
			// if(vid!=null && !vid.trim().equals("") &&
			// !vid.trim().equals("null"))
			// jsonConditionBuff.append("\"vid\":{$regex:/"+vid+"/},");
			//
			// if(camId!=null && !camId.trim().equals("") &&
			// !camId.trim().equals("null"))
			// jsonConditionBuff.append("\"camid\":{$regex:/"+camId+"/},");
			//
			// if(source!=null && !source.trim().equals("") &&
			// !source.trim().equals("null"))
			// jsonConditionBuff.append("\"source\": "+source+",");

			logger.info("====111111111=>jsonCondition=" + jsonCondition);

			//////// 查询条件
			BasicDBObject queryDb = cond;
			//// BasicDBObject queryDb = BasicDBObject.parse(jsonCondition);
			logger.info("------>in doQueryVideo, queryDb=" + queryDb);

			/////////////// liaoxb add
			if (orderJson == null || orderJson.trim().equals("") || orderJson.trim().equals("null")) {
				orderJson = "{\"createDate\": 1}";
				//// orderJson="{}";
			}
			logger.info("=====>orderJson=" + orderJson);
			BasicDBObject orderDb = BasicDBObject.parse(orderJson);
			logger.info("=====>orderDb=" + orderDb);

			////////////////////////////////////////////////////////

			total = collection.count(queryDb);
			logger.info("total=" + total);

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				findIterable = collection.find(queryDb).sort(orderDb).skip(skip).limit(limit);
			} else if (skip == 0 && limit > 0) {
				findIterable = collection.find(queryDb).sort(orderDb).limit(limit);
			} else {
				findIterable = collection.find(queryDb).sort(orderDb);
			}

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				HashMap<String, Object> map = new HashMap<String, Object>();

				for (String key : keys) {
					map.put(key, d.get(key));
				}

				result.add(map);

			}

			logger.info("count=" + count);
			resultVo.setRes(result);
			resultVo.setTotal(total);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in doQueryVideo", e);

			throw new DbException(3012, "doQueryVideo error", e);
		} finally {

		}

		return resultVo;

	}

	/*****
	 * 2019-3-13 wangmh add
	 * 
	 * 按车id，开始时间，结束时间查询视频文件
	 * 
	 * dbName: qdyktest tableName: t_video
	 * 
	 * 
	 **/
	public static MongoResultVo doQueryVideo(MongoClient conn, String dbName, String tableName, String fileId,
			String filename, String vid, String roadname, String roadpointname, String camId, String source,
			String startBeginTime, String startEndTime, String stopBeginTime, String stopEndTime, String status,
			String plateNo, String camCode, String camtype, String type, String orderJson, int skip, int limit)
					throws DbException {

		MongoResultVo resultVo = new MongoResultVo();

		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		long total = 0L;
		try {
			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(dbName);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			////// BasicDBObject condition = new BasicDBObject();

			// 查询两个时间范围的，用map包装一下
			///// query3.put( "recordDate", new BasicDBObject("$gte",
			// 20180101000000L).append("$lte", 20500101000000L) );

			//// condition.put( "vin", new BasicDBObject("$eq", vin) );
			// if(vin!=null && !vin.trim().equals("") &&
			// !vin.trim().equals("null"))
			// condition.put( "vin", vin);

			/////////// condition.put( "recordDate", new BasicDBObject("$gte",
			/////////// beginDate).append("$lte", endDate) );
			String jsonCondition = "";

			BasicDBObject cond = new BasicDBObject();

			///////////////////////////////////////////
			if (startBeginTime == null || startBeginTime.trim().equals("") || startBeginTime.trim().equals("null"))
				startBeginTime = "20190101000000";

			if (startEndTime == null || startEndTime.trim().equals("") || startEndTime.trim().equals("null"))
				startEndTime = "20500308122100";

			if (stopBeginTime == null || stopBeginTime.trim().equals("") || stopBeginTime.trim().equals("null"))
				stopBeginTime = "20190101000000";

			if (stopEndTime == null || stopEndTime.trim().equals("") || stopEndTime.trim().equals("null"))
				stopEndTime = "20500308122100";

			jsonCondition = "{\"beginTime\":{\"$gte\":\"" + startBeginTime + "\",\"$lte\":\"" + startEndTime
					+ "\"},\"endTime\":{\"$gte\":\"" + stopBeginTime + "\", \"$lte\":\"" + stopEndTime + "\"}}";

			cond = BasicDBObject.parse(jsonCondition);

			if (fileId != null && !fileId.trim().equals("") && !fileId.trim().equals("null")) {
				Pattern pattern = Pattern.compile("^.*" + fileId + ".*$", Pattern.CASE_INSENSITIVE);

				cond.put("fileid", pattern);
			}

			if (filename != null && !filename.trim().equals("") && !filename.trim().equals("null")) {
				Pattern pattern = Pattern.compile("^.*" + filename + ".*$", Pattern.CASE_INSENSITIVE);

				cond.put("filename", pattern);
			}

			if (vid != null && !vid.trim().equals("") && !vid.trim().equals("null")) {
				Pattern pattern = Pattern.compile("^.*" + vid + ".*$", Pattern.CASE_INSENSITIVE);

				cond.put("vid", pattern);
			}
			if (roadname != null && !roadname.trim().equals("") && !roadname.trim().equals("null")) {
				Pattern pattern = Pattern.compile("^.*" + roadname + ".*$", Pattern.CASE_INSENSITIVE);

				cond.put("roadname", pattern);
			}
			if (roadpointname != null && !roadpointname.trim().equals("") && !roadpointname.trim().equals("null")) {
				Pattern pattern = Pattern.compile("^.*" + roadpointname + ".*$", Pattern.CASE_INSENSITIVE);

				cond.put("roadpointname", pattern);
			}

			if (camId != null && !camId.trim().equals("") && !camId.trim().equals("null")) {
				Pattern pattern = Pattern.compile("^.*" + camId + ".*$", Pattern.CASE_INSENSITIVE);

				cond.put("camid", pattern);
			}

			if (camCode != null && !camCode.trim().equals("") && !camCode.trim().equals("null")) {
				Pattern pattern = Pattern.compile("^.*" + camCode + ".*$", Pattern.CASE_INSENSITIVE);

				cond.put("camCode", pattern);
			}

			if (source != null && !source.trim().equals("") && !source.trim().equals("null")) {
				cond.put("source", source);
			}

			if (status != null && !status.trim().equals("") && !status.trim().equals("null")) {
				cond.put("status", status);
			}
			if (camtype != null && !camtype.trim().equals("") && !camtype.trim().equals("null")) {
				// cond.put("camtype", camtype);
				String[] tmp_arr = camtype.split(",");
				if (tmp_arr != null && tmp_arr.length > 1) {
					List<String> tmp_list = new ArrayList<String>();
					for (String tmpStr : tmp_arr) {
						tmp_list.add(tmpStr);
					}
					cond.put("camtype", new BasicDBObject("$in", tmp_list));
				} else {
					cond.put("camtype", camtype);
				}

			}
			if (type != null && !type.trim().equals("") && !type.trim().equals("null")) {
				String[] tmp_arr = type.split(",");
				if (tmp_arr != null && tmp_arr.length > 1) {
					List<String> tmp_list = new ArrayList<String>();
					for (String tmpStr : tmp_arr) {
						tmp_list.add(tmpStr);
					}
					cond.put("type", new BasicDBObject("$in", tmp_list));
				} else {
					cond.put("type", camtype);
				}

			}

			if (plateNo != null && !plateNo.trim().equals("") && !plateNo.trim().equals("null")) {
				Pattern pattern = Pattern.compile("^.*" + plateNo + ".*$", Pattern.CASE_INSENSITIVE);

				cond.put("plateNo", pattern);
			}

			///////////////////////////////////////////
			// if(fileId!=null && !fileId.trim().equals("") &&
			// !fileId.trim().equals("null"))
			// jsonConditionBuff.append("\"fileid\":{$regex:/"+fileId+"/},");
			//
			// if(vid!=null && !vid.trim().equals("") &&
			// !vid.trim().equals("null"))
			// jsonConditionBuff.append("\"vid\":{$regex:/"+vid+"/},");
			//
			// if(camId!=null && !camId.trim().equals("") &&
			// !camId.trim().equals("null"))
			// jsonConditionBuff.append("\"camid\":{$regex:/"+camId+"/},");
			//
			// if(source!=null && !source.trim().equals("") &&
			// !source.trim().equals("null"))
			// jsonConditionBuff.append("\"source\": "+source+",");

			logger.info("====111111111=>jsonCondition=" + jsonCondition);

			//////// 查询条件
			BasicDBObject queryDb = cond;
			//// BasicDBObject queryDb = BasicDBObject.parse(jsonCondition);
			logger.info("------>in doQueryVideo, queryDb=" + queryDb);

			/////////////// liaoxb add
			if (orderJson == null || orderJson.trim().equals("") || orderJson.trim().equals("null")) {
				orderJson = "{\"createDate\": 1}";
				//// orderJson="{}";
			}
			logger.info("=====>orderJson=" + orderJson);
			BasicDBObject orderDb = BasicDBObject.parse(orderJson);
			logger.info("=====>orderDb=" + orderDb);

			////////////////////////////////////////////////////////

			total = collection.count(queryDb);
			logger.info("total=" + total);

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				findIterable = collection.find(queryDb).sort(orderDb).skip(skip).limit(limit);
			} else if (skip == 0 && limit > 0) {
				findIterable = collection.find(queryDb).sort(orderDb).limit(limit);
			} else {
				findIterable = collection.find(queryDb).sort(orderDb);
			}

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				HashMap<String, Object> map = new HashMap<String, Object>();

				for (String key : keys) {
					map.put(key, d.get(key));
				}

				result.add(map);

			}

			logger.info("count=" + count);
			resultVo.setRes(result);
			resultVo.setTotal(total);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in doQueryVideo", e);

			throw new DbException(3012, "doQueryVideo error", e);
		} finally {

		}

		return resultVo;

	}

	public static List<Map<String, Object>> queryMatchDate(MongoClient conn, String dbName, String tableName,
			String camId, String date, int skip, int limit) throws DbException {
		MongoResultVo resultVo = new MongoResultVo();
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		try {
			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(dbName);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);
			String jsonCondition = "{\"camid\":\"" + camId + "\",\"camtype\":\"10\"}";
			BasicDBObject queryDb = new BasicDBObject();
			queryDb = BasicDBObject.parse(jsonCondition);
			if (date != null && !date.trim().equals("") && !date.trim().equals("null")) {
				Pattern pattern = Pattern.compile("^" + date + ".*$", Pattern.CASE_INSENSITIVE);
				queryDb.put("createDate", pattern);
			}
			// 查询条件
			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				findIterable = collection.find(queryDb).skip(skip).limit(limit);
			} else if (skip == 0 && limit > 0) {
				findIterable = collection.find(queryDb).limit(limit);
			} else {
				findIterable = collection.find(queryDb);
			}

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				HashMap<String, Object> map = new HashMap<String, Object>();

				for (String key : keys) {
					map.put(key, d.get(key));
				}

				result.add(map);
			}
			logger.info("count=" + count);
			resultVo.setRes(result);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	/*****
	 * 2019-1-8 liaoxb add
	 * 
	 * 按车id，开始时间，结束时间查询视频文件
	 * 
	 * dbName: qdyktest tableName: t_video
	 * 
	 * 
	 **/
	public static List<Map<String, Object>> doQueryVideo(MongoClient conn, String dbName, String tableName, String vid,
			Long beginTime, Long endTime, String orderJson, int skip, int limit) throws DbException {
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

		try {

			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(dbName);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			////// BasicDBObject condition = new BasicDBObject();

			// 查询两个时间范围的，用map包装一下
			///// query3.put( "recordDate", new BasicDBObject("$gte",
			// 20180101000000L).append("$lte", 20500101000000L) );

			//// condition.put( "vin", new BasicDBObject("$eq", vin) );
			// if(vin!=null && !vin.trim().equals("") &&
			// !vin.trim().equals("null"))
			// condition.put( "vin", vin);

			/////////// condition.put( "recordDate", new BasicDBObject("$gte",
			/////////// beginDate).append("$lte", endDate) );
			String jsonCondition = "";

			if (vid != null && !vid.trim().equals("") && !vid.trim().equals("null")) {
				jsonCondition = "{\"vid\":\"" + vid + "\"," + " $or: [ " + " {$and:[{\"begin_time\":{\"$gte\":"
						+ beginTime + ",\"$lte\":" + endTime + "}},{\"end_time\":{\"$gte\":" + beginTime + ", \"$lte\":"
						+ endTime + "}}]},  " + " {$and:[{\"begin_time\":{\"$gte\":" + beginTime + ",\"$lte\":"
						+ endTime + "}},{\"end_time\":{\"$gte\":" + beginTime + ", \"$gte\":" + endTime + "}}]},  "
						+ " {$and:[{\"begin_time\":{\"$lte\":" + beginTime + ",\"$lte\":" + endTime
						+ "}},{\"end_time\":{\"$gte\":" + beginTime + ", \"$lte\":" + endTime + "}}]},  "
						+ " {$and:[{\"begin_time\":{\"$lte\":" + beginTime + ",\"$lte\":" + endTime
						+ "}},{\"end_time\":{\"$gte\":" + beginTime + ", \"$gte\":" + endTime + "}}]}   " + " ] }";

			} else {
				jsonCondition = "{ " + " $or: [ " + " {$and:[{\"begin_time\":{\"$gte\":" + beginTime + ",\"$lte\":"
						+ endTime + "}},{\"end_time\":{\"$gte\":" + beginTime + ", \"$lte\":" + endTime + "}}]},  "
						+ " {$and:[{\"begin_time\":{\"$gte\":" + beginTime + ",\"$lte\":" + endTime
						+ "}},{\"end_time\":{\"$gte\":" + beginTime + ", \"$gte\":" + endTime + "}}]},  "
						+ " {$and:[{\"begin_time\":{\"$lte\":" + beginTime + ",\"$lte\":" + endTime
						+ "}},{\"end_time\":{\"$gte\":" + beginTime + ", \"$lte\":" + endTime + "}}]},  "
						+ " {$and:[{\"begin_time\":{\"$lte\":" + beginTime + ",\"$lte\":" + endTime
						+ "}},{\"end_time\":{\"$gte\":" + beginTime + ", \"$gte\":" + endTime + "}}]}   " + " ] }";
			}

			// if(vid!=null && !vid.trim().equals("") &&
			// !vid.trim().equals("null"))
			// {
			// jsonCondition="{\"vid\":\""+vid+"\","+
			// " $or: [ "+
			// "
			// {$and:[{\"begin_time\":{\"$gte\":"+beginTime+"}},{\"end_time\":{\"$lte\":"+endTime+"}}]},
			// "+
			// "
			// {$and:[{\"begin_time\":{\"$lte\":"+beginTime+"}},{\"end_time\":{\"$gte\":"+endTime+"}}]}
			// "+
			// "
			// {$and:[{\"begin_time\":{\"$gte\":"+beginTime+",\"$lte\":"+endTime+"}},{\"end_time\":{\"$gte\":"+endTime+"}}]},
			// "+
			// "
			// {$and:[{\"begin_time\":{\"$lte\":"+beginTime+"}},{\"end_time\":{\"$gte\":"+beginTime+",\"$lte\":"+endTime+"}}]},
			// "+
			// " ] }";
			//
			// }
			// else
			// {
			// jsonCondition="{ "+
			// " $or: [ "+
			// "
			// {$and:[{\"begin_time\":{\"$gte\":"+beginTime+",\"$lte\":"+endTime+"}},{\"end_time\":{\"$gte\":"+beginTime+",
			// \"$lte\":"+endTime+"}}]}, "+
			// "
			// {$and:[{\"begin_time\":{\"$gte\":"+beginTime+",\"$lte\":"+endTime+"}},{\"end_time\":{\"$gte\":"+beginTime+",
			// \"$gte\":"+endTime+"}}]}, "+
			// "
			// {$and:[{\"begin_time\":{\"$lte\":"+beginTime+",\"$lte\":"+endTime+"}},{\"end_time\":{\"$gte\":"+beginTime+",
			// \"$lte\":"+endTime+"}}]}, "+
			// "
			// {$and:[{\"begin_time\":{\"$lte\":"+beginTime+",\"$lte\":"+endTime+"}},{\"end_time\":{\"$gte\":"+beginTime+",
			// \"$gte\":"+endTime+"}}]} "+
			// " ] }";
			// }

			logger.info("=====>jsonCondition=" + jsonCondition);

			//////// 查询条件
			BasicDBObject queryDb = BasicDBObject.parse(jsonCondition);
			logger.info("------>in doQueryVideo, queryDb=" + queryDb);

			/////////////// liaoxb add
			if (orderJson == null || orderJson.trim().equals("") || orderJson.trim().equals("null")) {
				orderJson = "{\"create_time\": 1}";
				//// orderJson="{}";
			}
			logger.info("=====>orderJson=" + orderJson);
			BasicDBObject orderDb = BasicDBObject.parse(orderJson);
			logger.info("=====>orderDb=" + orderDb);

			////////////////////////////////////////////////////////

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				findIterable = collection.find(queryDb).sort(orderDb).skip(skip).limit(limit);
			} else if (skip == 0 && limit > 0) {
				findIterable = collection.find(queryDb).sort(orderDb).limit(limit);
			} else {
				findIterable = collection.find(queryDb).sort(orderDb);
			}

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				HashMap<String, Object> map = new HashMap<String, Object>();

				for (String key : keys) {
					map.put(key, d.get(key));
				}

				result.add(map);

			}

			logger.info("count=" + count);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in doQueryVideo", e);

			throw new DbException(3012, "doQueryVideo error", e);
		} finally {

		}

		return result;

	}
	///////////////////////////////////////////////////////////

	// /******
	// * 批量提交回调
	// *
	// * **/

	/********
	 * 
	 * 2019-3-13 liaoxb add mongodb bulkWrite 批量提交
	 * 
	 */
	public static void bulkWrite(MongoClient conn, String db, String tableName,
			List<WriteModel<Document>> writeModelList) {

		if (conn == null)
			conn = mongoClient;

		MongoDatabase mongoDatabase = conn.getDatabase(db);

		MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

		collection.bulkWrite(writeModelList);

	}
	
	public static void bulkWrite(MongoClient conn, String db, String tableName,
			List<WriteModel<Document>> writeModelList, boolean ordered) {

		if (conn == null)
			conn = mongoClient;

		MongoDatabase mongoDatabase = conn.getDatabase(db);

		MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

		collection.bulkWrite(writeModelList, (new BulkWriteOptions()).ordered(ordered));

	}

	/******
	 * 
	 * String [0] -------过滤条件json String [1] -------值json
	 * 
	 **/
	public static void bulkReplace(MongoClient conn, String db, String tableName, List<String[]> docs) {

		if (conn == null)
			conn = mongoClient;

		MongoDatabase mongoDatabase = conn.getDatabase(db);

		MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

		List<WriteModel<Document>> writeModelList = new ArrayList<WriteModel<Document>>();

		if (docs != null) {
			for (String[] arr : docs) {

				Document filter = Document.parse(arr[0]);
				Document document = Document.parse(arr[1]);

				UpdateOptions op = new UpdateOptions();
				op.upsert(true); /// 无匹配时为新增

				com.mongodb.client.model.ReplaceOneModel vo = new com.mongodb.client.model.ReplaceOneModel(filter,
						document, op);

				writeModelList.add(vo);
			}

			collection.bulkWrite(writeModelList);
		}

	}

//	////////////////////////////////////////////////////////////////////////////////////////////////
//	/******
//	 * 统计某个车1个月的按天分组的，驾驶时间总和 ------real_route_info
//	 * 
//	 * 
//	 **/
//	public static List<DurationVo> aggrateVehicleRouteByDay(MongoClient conn, String db, String tableName,
//			String vehicleId, String beginDay, String endDay) throws DbException {
//		List<DurationVo> result = new ArrayList<DurationVo>();
//
//		try {
//			if (conn == null)
//				conn = mongoClient;
//
//			MongoDatabase mongoDatabase = conn.getDatabase(db);
//
//			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);
//
//			List<BasicDBObject> pipeline = new ArrayList<>();
//
//			if (beginDay == null || beginDay.trim().equals("")) {
//				beginDay = "20010101";
//			}
//
//			if (endDay == null || endDay.trim().equals("")) {
//				endDay = "20500101";
//			}
//
//			// String match=" {" +
//			// " $match: {" +
//			// " rundate: {$gte: \""+beginDay+"\",$lte:\""+endDay+"\"}" +
//			// " }" +
//			// " }";
//
//			// match(相当于 WHERE 或者 HAVING )
//			BasicDBObject query = new BasicDBObject();
//			BasicDBObject[] array = { new BasicDBObject("vehicleId", vehicleId),
//					new BasicDBObject("runDate", new BasicDBObject("$gte", beginDay)),
//					new BasicDBObject("runDate", new BasicDBObject("$lte", endDay)) };
//			query.append("$and", array);
//			BasicDBObject match = new BasicDBObject("$match", query);
//
//			logger.info("======>query=" + query.toJson());
//
//			// group（相当于 GROUP BY）
//			BasicDBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "$runDate").append("totalNum",
//					new BasicDBObject("$sum", "$durationTime")));
//
//			pipeline.add(match);
//			pipeline.add(group);
//			// queryList .add(sort);
//			// queryList .add(skip);
//			// queryList .add(limit);
//
//			AggregateIterable<Document> iterable = collection.aggregate(pipeline);
//
//			MongoCursor<Document> its = null;
//
//			if (iterable != null) {
//				its = iterable.iterator();
//
//				while (its.hasNext()) {
//					Document doc = its.next();
//
//					DurationVo vo = new DurationVo();
//
//					vo.setDay(doc.getString("_id"));
//					
//					 Long tmp_val=getDurationFromDoc(doc, "totalNum");
//					 
//					 vo.setDuration(tmp_val);
//					
//					////vo.setDuration(doc.getInteger("totalNum")/(60*1000));  ////驾驶时间---目前单位是ms，转为分钟
//
//					result.add(vo);
//				}
//			}
//
//		} catch (Exception e) {
//			e.printStackTrace();
//
//			logger.error("in aggrateVehicleRouteByDay", e);
//
//			throw new DbException(5001, "aggrateVehicleRouteByDay error", e);
//		} finally {
//
//		}
//
//		return result;
//
//	}
//	
//	/***
//	 * 求某个车总里程--千米，总驾驶时长--分钟，驾驶总次数---real_route_info
//	 * 表中单位
//	 * mileage	double	行驶里程	单位：m
//       durationTime	long	行程持续时间	单位：ms
//	 * 
//	 * **/
//	public static DriverTotalVo queryMileageAndDurationById(MongoClient conn, String db, String tableName,
//			String vehicleId) throws DbException 
//	{
//		DriverTotalVo vo=new DriverTotalVo();
//		
//		try {
//			if (conn == null)
//				conn = mongoClient;
//
//			MongoDatabase mongoDatabase = conn.getDatabase(db);
//
//			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);
//
//			List<BasicDBObject> pipeline = new ArrayList<>();
//
//			// match(相当于 WHERE 或者 HAVING )
//			BasicDBObject query = new BasicDBObject("vehicleId", vehicleId);
//
//			BasicDBObject match = new BasicDBObject("$match", query);
//
//			logger.info("======>query=" + query.toJson());
//
//			// group（相当于 GROUP BY）
//			BasicDBObject group = new BasicDBObject("$group",
//					new BasicDBObject("_id", "$vehicleId")
//							.append("total_duration", new BasicDBObject("$sum", "$durationTime")) //// 驾驶总时长
//							.append("total_mileage",  new BasicDBObject("$sum", "$mileage")) //// 驾驶总里程
//							.append("total_count",    new BasicDBObject("$sum", 1)) //// 驾驶总次数
//							);
//
//			logger.info("=======>group=" + group.toJson());
//
//			pipeline.add(match);
//			pipeline.add(group);
//			// queryList .add(sort);
//			// queryList .add(skip);
//			// queryList .add(limit);
//
//			AggregateIterable<Document> iterable = collection.aggregate(pipeline);
//
//			MongoCursor<Document> its = null;
//
//			if (iterable != null) {
//				its = iterable.iterator();
//
//				while (its.hasNext()) {
//					Document doc = its.next();
//
//
//					vo.setVehicleId(doc.getString("_id"));
//					//vo.setDuration(doc.getInteger("total_duration"));
//					
//					////Object d_duration=doc.get("total_duration");
//					
//					
//					long l_duration=getDurationFromDoc(doc, "total_duration");
//					vo.setDuration(l_duration);
//					
//					
////					if(d_duration instanceof Integer)
////					{
////						vo.setDuration(doc.getInteger("total_duration")/(60*1000)); ////驾驶时长单位由ms转分钟 
////					}
////					else if(d_duration instanceof Long)
////					{
////						vo.setDuration(doc.getLong("total_duration")/(60*1000)); ////驾驶时长单位由ms转分钟 
////					}
////					else if(d_duration instanceof Double)
////					{
////						Double tmp_val=doc.getDouble("total_duration")/(60*1000);
////						vo.setDuration(tmp_val.longValue()); ////驾驶时长单位由ms转分钟 
////					}
// 
//					////vo.setDuration(Long.parseLong(String.valueOf(doc.get("total_duration")))/(60*1000)); ////驾驶时长单位由ms转分钟 
//					vo.setMileage(doc.getDouble("total_mileage")/1000);  /////里程，单位 千米
//					//vo.setCount(doc.getInteger("total_count"));
//					vo.setCount(Long.parseLong(String.valueOf(doc.get("total_count"))));
//
//					break;
//				}
//			}
//
//		} catch (Exception e) {
//			e.printStackTrace();
//
//			logger.error("in queryMileageAndDurationById", e);
//
//			throw new DbException(5001, "queryMileageAndDurationById error", e);
//		} finally {
//
//		}
//		
//		return vo;
//	}
//	
//	
//	public static Long getDurationFromDoc(Document doc, String fieldName)
//	{
//		Long val=0L;
//		
//		Object d_duration=doc.get(fieldName);
//		
//		if(d_duration instanceof Integer)
//		{
//			String s_val=""+ (Integer)d_duration/(60*1000); ////驾驶时长单位由ms转分钟 
//			
//			if(s_val==null || s_val.trim().equals("null"))
//				val=0L;
//			else
//				val=Long.parseLong(s_val);
//		}
//		else if(d_duration instanceof Long)
//		{
//			val=(Long)d_duration/(60*1000); ////驾驶时长单位由ms转分钟 
//		}
//		else if(d_duration instanceof Double)
//		{
//			Double tmp_val=((Double)d_duration)/(60*1000);
//			val=tmp_val.longValue(); ////驾驶时长单位由ms转分钟 
//		}
//		
//		return val;
//	}
//	
//	

//	/******
//	 * 统计route_analyze表中。某个车总的驾驶总时长,驾驶总时长,驾驶总次数,急刹车，急加速，急转弯，最大速度
//	 * ----固定读route_analyze
//	 * 
//	 * 
//	 **/
//	public static List<DriverTotalVo> aggrateVehicleRouteTotal(MongoClient conn, String db, String tableName,
//			String vehicleId) throws DbException {
//		List<DriverTotalVo> result = new ArrayList<DriverTotalVo>();
//
//		try {
//			if (conn == null)
//				conn = mongoClient;
//			
//			/////////////////////////////////////
//			DriverTotalVo vo2=queryMileageAndDurationById(  conn,   db, "real_route_info",  vehicleId);
//			////////////////////////////////////
//
//			MongoDatabase mongoDatabase = conn.getDatabase(db);
//
//			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);
//
//			List<BasicDBObject> pipeline = new ArrayList<>();
//
//			// match(相当于 WHERE 或者 HAVING )
//			BasicDBObject query = new BasicDBObject("vehicleId", vehicleId);
//
//			BasicDBObject match = new BasicDBObject("$match", query);
//
//			logger.info("======>query=" + query.toJson());
//
//			// group（相当于 GROUP BY）
//			BasicDBObject group = new BasicDBObject("$group",
//					new BasicDBObject("_id", "$vehicleId")
//							.append("total_duration", new BasicDBObject("$sum", "$durationTime")) //// 驾驶总时长
//							.append("total_mileage", new BasicDBObject("$sum", "$mileage")) //// 驾驶总里程
//							.append("total_count", new BasicDBObject("$sum", 1)) //// 驾驶总次数
//							.append("total_rapidBrakeNum", new BasicDBObject("$sum", "$rapidBrakeNum"))
//							.append("total_rapidAccelerateNum", new BasicDBObject("$sum", "$rapidAccelerateNum"))
//							.append("total_rapidTurnNum", new BasicDBObject("$sum", "$rapidTurnNum"))
//							.append("maxSpeed", new BasicDBObject("$max", "$maxSpeed")));
//
//			logger.info("=======>group=" + group.toJson());
//
//			pipeline.add(match);
//			pipeline.add(group);
//			// queryList .add(sort);
//			// queryList .add(skip);
//			// queryList .add(limit);
//
//			AggregateIterable<Document> iterable = collection.aggregate(pipeline);
//
//			MongoCursor<Document> its = null;
//
//			if (iterable != null) {
//				its = iterable.iterator();
//
//				while (its.hasNext()) {
//					Document doc = its.next();
//
//					DriverTotalVo vo = new DriverTotalVo();
//
//					vo.setVehicleId(doc.getString("_id"));
//					//vo.setDuration(doc.getInteger("total_duration"));
//					
//					
//					long l_duration=getDurationFromDoc(doc, "total_duration");
//					
//					vo.setDuration(l_duration);
//					
//					////vo.setDuration(Long.parseLong(String.valueOf(doc.get("total_duration")))/(60*1000)); ////驾驶时长单位由ms转分钟 
//					vo.setMileage(doc.getDouble("total_mileage"));  /////里程，单位 千米
//					//vo.setCount(doc.getInteger("total_count"));
//					vo.setCount(Long.parseLong(String.valueOf(doc.get("total_count"))));
//					//vo.setRapidBrakeNum(doc.getInteger("total_rapidBrakeNum"));
//					vo.setRapidBrakeNum(Long.parseLong(String.valueOf(doc.get("total_rapidBrakeNum"))));
//					//vo.setRapidAccelerateNum(doc.getInteger("total_rapidAccelerateNum"));
//					vo.setRapidAccelerateNum(Long.parseLong(String.valueOf(doc.get("total_rapidAccelerateNum"))));
//					//vo.setRapidTurnNum(doc.getInteger("total_rapidTurnNum"));
//					vo.setRapidTurnNum(Long.parseLong(String.valueOf(doc.get("total_rapidTurnNum"))));
//					vo.setMaxSpeed(doc.getDouble("maxSpeed"));
//
//					///////////////从real_route_info中读出这个数据，而不是取route_analyze中的数据
//					vo.setDuration(vo2.getDuration());
//					vo.setMileage(vo2.getMileage());
//					vo.setCount(vo2.getCount());
//					///////////////
//					
//					result.add(vo);
//				}
//			}
//
//			if(result.size()==0){
//				DriverTotalVo vo = new DriverTotalVo();
//				vo.setVehicleId(vehicleId.trim());
//				
//				if(vo2!=null)
//				{
//					vo.setDuration(vo2.getDuration());
//					vo.setMileage(vo2.getMileage());
//					vo.setCount(vo2.getCount());
//				}
//				else
//				{
//					vo.setDuration(0L);
//					vo.setMileage(0D);
//					vo.setCount(0L);
//				}
//				
//				result.add(vo);
//			}
//			
//		} catch (Exception e) {
//			e.printStackTrace();
//
//			logger.error("in aggrateVehicleRouteTotal", e);
//
//			throw new DbException(5001, "aggrateVehicleRouteTotal error", e);
//		} finally {
//
//		}
//
//		return result;
//
//	}
//
//	/**
//	 * 驾驶距离top5 近30天，车辆总驾驶距离top5，返回字段包括车辆id、车辆总行驶距离(km) ------real_route_info
//	 * 
//	 * @param conn
//	 * @param db
//	 * @param tableName
//	 * @param beginDay
//	 * @param endDay
//	 * @param count
//	 * @return
//	 * @throws DbException
//	 */
//	public static List<DriverTotalVo> aggrateVehicleRouteMileage(MongoClient conn, String db, String tableName,
//			String beginDay, String endDay, int count) throws DbException {
//		List<DriverTotalVo> result = new ArrayList<>();
//		try {
//			if (conn == null)
//				conn = mongoClient;
//
//			MongoDatabase mongoDatabase = conn.getDatabase(db);
//
//			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);
//
//			List<BasicDBObject> pipeline = new ArrayList<>();
//
//			if (beginDay == null || beginDay.trim().equals("")) {
//				beginDay = "20010101";
//			}
//
//			if (endDay == null || endDay.trim().equals("")) {
//				endDay = "20500101";
//			}
//
//			// 添加时间查询条件
//			BasicDBObject query = new BasicDBObject();
//
//			query.put("$gte", beginDay);
//			query.put("$lte", endDay);
//
//			BasicDBObject match = new BasicDBObject("$match", new BasicDBObject("runDate", query));
//
//			// 按车辆分组,对行程距离求和
//			BasicDBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "$vehicleId")
//					.append("total_mileage", new BasicDBObject("$sum", "$mileage")));
//
//			// 限制返回的文档数
//			BasicDBObject limit = new BasicDBObject("$limit", count);
//
//			// 按行程距离倒序排序
//			BasicDBObject sort = new BasicDBObject("$sort", new BasicDBObject("total_mileage", -1));
//
//			pipeline.add(match);
//			pipeline.add(group);
//			pipeline.add(sort);
//			pipeline.add(limit);
//
//			logger.info("the aggregate pipeline ：" + pipeline);
//			AggregateIterable<Document> iterable = collection.aggregate(pipeline);
//			MongoCursor<Document> its = null;
//
//			if (iterable != null) {
//				its = iterable.iterator();
//				while (its.hasNext()) {
//					Document doc = its.next();
//
//					DriverTotalVo vo = new DriverTotalVo();
//
//					vo.setVehicleId(doc.getString("_id"));
//					vo.setMileage(doc.getDouble("total_mileage")/1000);  /////里程单位由米转千米
//					result.add(vo);
//				}
//			}
//
//		} catch (Exception e) {
//			e.printStackTrace();
//
//			logger.error("in aggrateVehicleRouteMileageByTime", e);
//
//			throw new DbException(5001, "aggrateVehicleRouteMileageByTime error", e);
//		}
//		return result;
//	}
//
//	/**
//	 * 2、驾驶时长top5 近30天，车辆总驾驶时长top5，返回字段包括车辆id、车辆总行驶时长(分钟) ------real_route_info
//	 * 
//	 * @param conn
//	 * @param db
//	 * @param tableName
//	 * @param beginDay
//	 * @param endDay
//	 * @param count
//	 * @return
//	 * @throws DbException
//	 */
//	public static List<DriverTotalVo> aggrateVehicleRouteDuration(MongoClient conn, String db, String tableName,
//			String beginDay, String endDay, int count) throws DbException {
//		List<DriverTotalVo> result = new ArrayList<>();
//		try {
//			if (conn == null)
//				conn = mongoClient;
//
//			MongoDatabase mongoDatabase = conn.getDatabase(db);
//
//			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);
//
//			List<BasicDBObject> pipeline = new ArrayList<>();
//
//			if (beginDay == null || beginDay.trim().equals("")) {
//				beginDay = "20010101";
//			}
//
//			if (endDay == null || endDay.trim().equals("")) {
//				endDay = "20500101";
//			}
//
//			// 添加时间查询条件
//			BasicDBObject query = new BasicDBObject();
//
//			query.put("$gte", beginDay);
//			query.put("$lte", endDay);
//
//			BasicDBObject match = new BasicDBObject("$match", new BasicDBObject("runDate", query));
//
//			// 按车辆分组,对行程时长求和
//			BasicDBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "$vehicleId")
//					.append("total_duration", new BasicDBObject("$sum", "$durationTime")));
//
//			// 限制返回的文档数
//			BasicDBObject limit = new BasicDBObject("$limit", count);
//
//			// 按行程时长倒序排序
//			BasicDBObject sort = new BasicDBObject("$sort", new BasicDBObject("total_duration", -1));
//
//			pipeline.add(match);
//			pipeline.add(group);
//			pipeline.add(sort);
//			pipeline.add(limit);
//
//			logger.info("the aggregate pipeline ：" + pipeline);
//			AggregateIterable<Document> iterable = collection.aggregate(pipeline);
//			MongoCursor<Document> its = null;
//
//			if (iterable != null) {
//				its = iterable.iterator();
//				while (its.hasNext()) {
//					Document doc = its.next();
//
//					DriverTotalVo vo = new DriverTotalVo();
//
//					vo.setVehicleId(doc.getString("_id"));
//
//					///Object val = doc.get("total_duration");
//					
//					long l_duration=getDurationFromDoc(doc, "total_duration");
//					
//					vo.setDuration(l_duration);
//					
//	 
//					// vo.setDuration(doc.get("total_duration",Long.class));
//					result.add(vo);
//				}
//			}
//
//		} catch (Exception e) {
//			e.printStackTrace();
//
//			logger.error("in aggrateVehicleRouteDuration", e);
//
//			throw new DbException(5001, "aggrateVehicleRouteDuration error", e);
//		}
//		return result;
//	}

	/**
	 * 1、行程距离分布 近30天，车辆行程距离分布数据汇总。返回字段包括单个行程距离分布维度(km)、对应分布维度的单个行程总数。
	 * ------real_route_info
	 * 
	 * @param conn
	 * @param db
	 * @param tableName
	 * @param beginDay
	 * @param endDay
	 * @param range
	 *            "0-20", "20-30","30-40"
	 * @param endValue
	 *            40
	 * @return
	 * @throws DbException
	 */
	public static LinkedHashMap<String, Long> vehicleRouteMileageDimensionBak(MongoClient conn, String db,
			String tableName, String beginDay, String endDay, String[] range, long endValue) throws DbException {
		LinkedHashMap<String, Long> result = new LinkedHashMap<>();
		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

			if (beginDay == null || beginDay.trim().equals("")) {
				beginDay = "20010101";
			}

			if (endDay == null || endDay.trim().equals("")) {
				endDay = "20500101";
			}

			// 添加时间查询条件
			BasicDBObject tiemQuery = new BasicDBObject();

			tiemQuery.put("$gte", beginDay);
			tiemQuery.put("$lte", endDay);

			BasicDBObject filter = new BasicDBObject("runDate", tiemQuery);

			FindIterable<Document> findIterable = collection.find(filter);

			long[] count = new long[range.length + 1];

			// 数据遍历
			for (Document document : findIterable) {
				double mileage = document.getDouble("mileage");

				if (mileage >= endValue) {
					count[range.length] += 1;
				} else {
					for (int i = 0; i < range.length; i++) {
						String[] values = range[i].split("-");
						long minVal = Long.parseLong(values[0]);
						long maxVal = Long.parseLong(values[1]);
						if (mileage >= minVal && mileage < maxVal) {
							count[i] += 1;
							break;
						}
					}
				}
			}

			// 返回值
			for (int i = 0; i < range.length; i++) {
				result.put(range[i], count[i]);
			}
			result.put(endValue + "", count[range.length]);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in vehicleRouteMileageDimension", e);

			throw new DbException(5001, "vehicleRouteMileageDimension error", e);
		}
		return result;
	}

	/**
	 * 1、行程距离分布 近30天，车辆行程距离分布数据汇总。返回字段包括单个行程距离分布维度(km)、对应分布维度的单个行程总数。
	 * 
	 * @param conn
	 * @param db
	 * @param tableName
	 * @param beginDay
	 * @param endDay
	 * @param range
	 *            "0-20", "20-30","30-40"
	 * @param endValue
	 *            40
	 * @return
	 * @throws DbException
	 */
	public static LinkedHashMap<String, Long> vehicleRouteMileageDimension(MongoClient conn, String db,
			String tableName, String beginDay, String endDay, String[] range, long endValue) throws DbException {
		LinkedHashMap<String, Long> result = new LinkedHashMap<>();
		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

			List<BasicDBObject> pipeline = new ArrayList<>();

			if (beginDay == null || beginDay.trim().equals("")) {
				beginDay = "20010101";
			}

			if (endDay == null || endDay.trim().equals("")) {
				endDay = "20500101";
			}

			// 添加时间查询条件
			BasicDBObject query = new BasicDBObject();

			query.put("$gte", beginDay);
			query.put("$lte", endDay);

			BasicDBObject match = new BasicDBObject("$match", new BasicDBObject("runDate", query));

			BasicDBObject groupContent = new BasicDBObject("_id", null);

			for (String str : range) {

				String[] values = str.split("-");
				if (values.length == 2) {
					long minVal = Long.parseLong(values[0])*1000;  /////mongo表中单位是米，外面传进来是千米，所以要乘以1000
					long maxVal = Long.parseLong(values[1])*1000;

					BasicDBObject cond = new BasicDBObject();

					BasicDBObject[] andExpression = { new BasicDBObject("$gte", Arrays.asList("$mileage", minVal)),
							new BasicDBObject("$lt", Arrays.asList("$mileage", maxVal)) };
					BasicDBObject ifExpression = new BasicDBObject();
					ifExpression.put("$and", andExpression);
					cond.put("if", ifExpression);
					cond.put("then", 1);
					cond.put("else", 0);
					groupContent.put(str, new BasicDBObject("$sum", new BasicDBObject("$cond", cond)));
				}

			}
			BasicDBObject cond = new BasicDBObject();
			cond.put("if", new BasicDBObject("$gte", Arrays.asList("$mileage", endValue*1000)));
			cond.put("then", 1);
			cond.put("else", 0);

			groupContent.put(endValue + "", new BasicDBObject("$sum", new BasicDBObject("$cond", cond)));

			BasicDBObject group = new BasicDBObject("$group", groupContent);
			pipeline.add(match);
			pipeline.add(group);

			logger.info("the aggregate pipeline ：" + pipeline);

			AggregateIterable<Document> iterable = collection.aggregate(pipeline);
			MongoCursor<Document> its = null;

			if (iterable != null) {
				its = iterable.iterator();
				while (its.hasNext()) {
					Document doc = its.next();

					for (String str : range) {

						result.put(str, Long.parseLong(String.valueOf(doc.get(str))));
					}
					result.put(endValue + "", Long.parseLong(String.valueOf(doc.get(endValue + ""))));
				}
			}

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in vehicleRouteMileageDimension", e);

			throw new DbException(5001, "vehicleRouteMileageDimension error", e);
		}
		return result;
	}

	/**
	 * 2、行程时长分布 近30天，车辆行程时长分布数据汇总。返回字段包括单个行程时长分布维度(小时)、对应分布维度的单个行程总数
	 * ------real_route_info
	 * 
	 * @param conn
	 * @param db
	 * @param tableName
	 * @param beginDay
	 * @param endDay
	 * @return
	 * @throws DbException
	 */
	public static LinkedHashMap<String, Long> vehicleRouteDurationDimension(MongoClient conn, String db,
			String tableName, String beginDay, String endDay) throws DbException {
		String[] range = new String[] { "0-1", "1-2", "2-3", "3-4", "4-5", "5-6", "6-7", "7-8", "8-15" };
		return vehicleRouteDurationDimension(conn, db, tableName, beginDay, endDay, range, 15);
	}

	/**
	 * 2、行程时长分布 近30天，车辆行程时长分布数据汇总。返回字段包括单个行程时长分布维度(小时)、对应分布维度的单个行程总数
	 * 
	 * @param conn
	 * @param db
	 * @param tableName
	 * @param beginDay
	 * @param endDay
	 * @return
	 * @throws DbException
	 */
	public static LinkedHashMap<String, Long> vehicleRouteDurationDimension(MongoClient conn, String db,
			String tableName, String beginDay, String endDay, String[] range, long endValue) throws DbException {
		LinkedHashMap<String, Long> result = new LinkedHashMap<>();
		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

			List<BasicDBObject> pipeline = new ArrayList<>();

			if (beginDay == null || beginDay.trim().equals("")) {
				beginDay = "20010101";
			}

			if (endDay == null || endDay.trim().equals("")) {
				endDay = "20500101";
			}

			// 添加时间查询条件
			BasicDBObject query = new BasicDBObject();

			query.put("$gte", beginDay);
			query.put("$lte", endDay);

			BasicDBObject match = new BasicDBObject("$match", new BasicDBObject("runDate", query));

			BasicDBObject groupContent = new BasicDBObject("_id", null);

			for (String str : range) {

				String[] values = str.split("-");
				if (values.length == 2) {
					long minVal = Long.parseLong(values[0]) * (60 * 60 * 1000);
					long maxVal = Long.parseLong(values[1]) * (60 * 60 * 1000);

					BasicDBObject cond = new BasicDBObject();

					BasicDBObject[] andExpression = { new BasicDBObject("$gte", Arrays.asList("$durationTime", minVal)),
							new BasicDBObject("$lt", Arrays.asList("$durationTime", maxVal)) };
					BasicDBObject ifExpression = new BasicDBObject();
					ifExpression.put("$and", andExpression);
					cond.put("if", ifExpression);
					cond.put("then", 1);
					cond.put("else", 0);
					groupContent.put(str, new BasicDBObject("$sum", new BasicDBObject("$cond", cond)));
				}

			}
			BasicDBObject cond = new BasicDBObject();
			cond.put("if", new BasicDBObject("$gte", Arrays.asList("$durationTime", endValue * (60 * 60 * 1000))));
			cond.put("then", 1);
			cond.put("else", 0);

			groupContent.put(endValue + "", new BasicDBObject("$sum", new BasicDBObject("$cond", cond)));

			BasicDBObject group = new BasicDBObject("$group", groupContent);
			pipeline.add(match);
			pipeline.add(group);

			logger.info("the aggregate pipeline ：" + pipeline);

			AggregateIterable<Document> iterable = collection.aggregate(pipeline);

			MongoCursor<Document> its = null;

			if (iterable != null) {
				its = iterable.iterator();
				while (its.hasNext()) {
					Document doc = its.next();

					for (String str : range) {

						result.put(str, Long.parseLong(String.valueOf(doc.get(str))));
					}
					result.put(endValue + "", Long.parseLong(String.valueOf(doc.get(endValue + ""))));
				}
			}

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in vehicleRouteDurationDimension", e);

			throw new DbException(5001, "vehicleRouteDurationDimension error", e);
		}
		return result;
	}

	/**
	 * 2、行程时长分布 近30天，车辆行程时长分布数据汇总。返回字段包括单个行程时长分布维度(小时)、对应分布维度的单个行程总数
	 * 
	 * @param conn
	 * @param db
	 * @param tableName
	 * @param beginDay
	 * @param endDay
	 * @return
	 * @throws DbException
	 */
	public static LinkedHashMap<String, Long> vehicleRouteDurationDimension2(MongoClient conn, String db,
			String tableName, String beginDay, String endDay) throws DbException {
		LinkedHashMap<String, Long> result = new LinkedHashMap<>();
		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

			if (beginDay == null || beginDay.trim().equals("")) {
				beginDay = "20010101";
			}

			if (endDay == null || endDay.trim().equals("")) {
				endDay = "20500101";
			}

			// 添加时间查询条件
			BasicDBObject tiemQuery = new BasicDBObject();

			tiemQuery.put("$gte", beginDay);
			tiemQuery.put("$lte", endDay);
			BasicDBObject filter = new BasicDBObject("runDate", tiemQuery);

			FindIterable<Document> findIterable = collection.find(filter);

			String[] range = new String[] { "0-1", "1-2", "2-3", "3-4", "4-5", "5-6", "6-7", "7-8", "8-15", };
			long[] count = new long[range.length + 1];
			long endValue = 15;
			// 数据遍历
			for (Document document : findIterable) {
				int duration = document.getInteger("durationTime");
				double hour = (double) duration / (60 * 60 * 1000);
				if (hour >= endValue) {
					count[range.length] += 1;
				} else {
					for (int i = 0; i < range.length; i++) {
						String[] values = range[i].split("-");
						long minVal = Long.parseLong(values[0]);
						long maxVal = Long.parseLong(values[1]);
						if (hour >= minVal && hour < maxVal) {
							count[i] += 1;
							break;
						}
					}
				}
			}

			// 返回值
			for (int i = 0; i < range.length; i++) {
				result.put(range[i], count[i]);
			}
			result.put(endValue + "", count[range.length]);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in vehicleRouteDurationDimension", e);

			throw new DbException(5001, "vehicleRouteDurationDimension error", e);
		}
		return result;
	}

	/**
	 * 3、行程时段分布 近30天，车辆行程时段分布数据汇总。返回字段包括单个行程时段分布维度(小时)、对应分布维度的单个行程总数。
	 * -----固定读route_period
	 * 
	 * @param conn
	 * @param db
	 *            qdyktest
	 * @param tableName
	 *            route_period
	 * @param beginDay
	 * @param endDay
	 * @return
	 * @throws DbException
	 */

	public static LinkedHashMap<String, Long> vehicleRoutePeriodDimension(MongoClient conn, String db, String tableName,
			String beginDay, String endDay) throws DbException {

		// 凌晨 [0点~5点）、早上[
		// 5点~7点）、早高峰[7点~9点）、上午[9点~12点）、中午[12点~14点）、下午[14点~17点）、晚高峰[17点~19点）、晚上[19点~0点)
		String[] periods = new String[] { "0_5", "5_7", "7_9", "9_12", "12_14", "14_17", "17_19", "19_24" };

		return vehicleRoutePeriodDimension(conn, db, tableName, beginDay, endDay, periods);

	}

	/**
	 * 3、行程时段分布
	 * 近30天，车辆行程时段分布数据汇总。返回字段包括单个行程时段分布维度(小时)、对应分布维度的单个行程总数。-----固定读route-period
	 * 
	 * @param conn
	 * @param db
	 *            qdyktest
	 * @param tableName
	 *            route_period
	 * @param beginDay
	 * @param endDay
	 * @return
	 * @throws DbException
	 */

	public static LinkedHashMap<String, Long> vehicleRoutePeriodDimension(MongoClient conn, String db, String tableName,
			String beginDay, String endDay, String[] periods) throws DbException {
		LinkedHashMap<String, Long> result = new LinkedHashMap<>();
		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

			List<BasicDBObject> pipeline = new ArrayList<>();

			if (beginDay == null || beginDay.trim().equals("")) {
				beginDay = "20010101";
			}

			if (endDay == null || endDay.trim().equals("")) {
				endDay = "20500101";
			}

			// 添加时间查询条件
			BasicDBObject query = new BasicDBObject();

			query.put("$gte", beginDay);
			query.put("$lte", endDay);
			BasicDBObject match = new BasicDBObject("$match", new BasicDBObject("day", query));

			BasicDBObject groupContent = new BasicDBObject("_id", null);

			for (String period : periods) {
				groupContent.append("total_time_" + period, new BasicDBObject("$sum", "$time_" + period));
			}

			BasicDBObject group = new BasicDBObject("$group", groupContent);
			pipeline.add(match);
			pipeline.add(group);

			logger.info(" the aggregate pipeline ：" + pipeline);
			AggregateIterable<Document> iterable = collection.aggregate(pipeline);
			MongoCursor<Document> its = null;

			if (iterable != null) {
				its = iterable.iterator();
				while (its.hasNext()) {
					Document doc = its.next();

					for (String period : periods) {

						result.put(period.replace("_", "-"),
								Long.parseLong(String.valueOf(doc.get("total_time_" + period))));
					}
				}
			}

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in vehicleRoutePeriodDimension", e);

			throw new DbException(5001, "vehicleRoutePeriodDimension error", e);
		}
		return result;
	}

	/**
	 * 求车辆总数 ------------- real_route_info
	 * 
	 * @param conn
	 * @param db
	 * @param tableName
	 * @return
	 * @throws DbException
	 */
	public static int aggregateRouteTotalVehicleCount(MongoClient conn, String db, String tableName)
			throws DbException {
		int result = 0;
		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

			List<BasicDBObject> pipeline = new ArrayList<>();

			// 分组
			BasicDBObject group = new BasicDBObject("$group", new BasicDBObject("_id", "$vehicleId"));

			// 求条数
			BasicDBObject count = new BasicDBObject("$group",
					new BasicDBObject("_id", null).append("count", new BasicDBObject("$sum", 1)));

			pipeline.add(group);
			pipeline.add(count);
			logger.info(" the aggregate pipeline : " + pipeline);
			AggregateIterable<Document> iterable = collection.aggregate(pipeline);
			MongoCursor<Document> its = null;

			if (iterable != null) {
				its = iterable.iterator();
				while (its.hasNext()) {
					Document doc = its.next();
					result = doc.getInteger("count");
				}
			}

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in aggregateRouteTotalVehicleCount", e);

			throw new DbException(5001, "aggregateRouteTotalVehicleCount error", e);
		}
		return result;
	}

	/**
	 * 求总里程（公里）
	 * 
	 * @param conn
	 * @param db
	 * @param tableName
	 * @return
	 * @throws DbException
	 */
	public static double aggregateRouteTotalMileage(MongoClient conn, String db, String tableName) throws DbException {
		double result = 0;
		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

			List<BasicDBObject> pipeline = new ArrayList<>();

			// 求条数
			BasicDBObject group = new BasicDBObject("$group",
					new BasicDBObject("_id", null).append("total_mileage", new BasicDBObject("$sum", "$mileage")));

			pipeline.add(group);

			logger.info(" the aggregate pipeline : " + pipeline);
			AggregateIterable<Document> iterable = collection.aggregate(pipeline);
			MongoCursor<Document> its = null;

			if (iterable != null) {
				its = iterable.iterator();
				while (its.hasNext()) {
					Document doc = its.next();
					result = doc.getDouble("total_mileage");
				}
			}
			
			result=result/1000;    ////////////将米转为千米(公里)

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in aggregateRouteTotalMileage", e);

			throw new DbException(5001, "aggregateRouteTotalMileage error", e);
		}
		return result;
	}
//
//	/**
//	 * 求总时长(小时)
//	 * 
//	 * @param conn
//	 * @param db
//	 * @param tableName
//	 * @return
//	 * @throws DbException
//	 */
//	public static double aggregateRouteTotalDuration(MongoClient conn, String db, String tableName) throws DbException {
//		double result = 0;
//		try {
//			if (conn == null)
//				conn = mongoClient;
//
//			MongoDatabase mongoDatabase = conn.getDatabase(db);
//
//			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);
//
//			List<BasicDBObject> pipeline = new ArrayList<>();
//
//			// 求条数
//			BasicDBObject group = new BasicDBObject("$group", new BasicDBObject("_id", null).append("total_duration",
//					new BasicDBObject("$sum", "$durationTime")));
//
//			pipeline.add(group);
//
//			logger.info(" the aggregate pipeline : " + pipeline);
//			AggregateIterable<Document> iterable = collection.aggregate(pipeline);
//			MongoCursor<Document> its = null;
//
//			if (iterable != null) {
//				its = iterable.iterator();
//				while (its.hasNext()) {
//					Document doc = its.next();
//					result = (double) Long.parseLong(String.valueOf(doc.get("total_duration"))) / (60 * 60 * 1000);
//				}
//			}
//
//		} catch (Exception e) {
//			e.printStackTrace();
//
//			logger.error("in aggregateRouteTotalDuration", e);
//
//			throw new DbException(5001, "aggregateRouteTotalDuration error", e);
//		}
//		return result;
//	}
	
	
	////-------------------
	
	
	public static Long[] getStartAndEndTimeByDay(String yyyymmdd)
	{
		Long [] arr=new Long[2];
		
		Calendar ca=Calendar.getInstance();
		
		ca.set(Integer.parseInt(yyyymmdd.substring(0,4)), Integer.parseInt(yyyymmdd.substring(4,6))-1, Integer.parseInt(yyyymmdd.substring(6,8)), 0, 0, 0);
		ca.set(Calendar.MILLISECOND, 0);
		
		arr[0]=ca.getTimeInMillis();
		
		
		////////-----------
		Calendar ca2=Calendar.getInstance();
		
		ca2.set(Integer.parseInt(yyyymmdd.substring(0,4)), Integer.parseInt(yyyymmdd.substring(4,6))-1, Integer.parseInt(yyyymmdd.substring(6,8)), 23, 59, 59);
		ca2.set(Calendar.MILLISECOND, 999);
		arr[1]=ca2.getTimeInMillis();
		
		
		return arr;
	}
	
	/////////---------------求当天的总时长
	public static double aggregateCurrRouteDuration(MongoClient conn, String db, String tableName, String yyyymmdd) throws DbException {
		double result = 0;
		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

			List<BasicDBObject> pipeline = new ArrayList<>();
			
			
			///////////-----------
			
			Long[] arr_time=getStartAndEndTimeByDay(yyyymmdd);
			
			// 添加时间查询条件
			BasicDBObject query = new BasicDBObject();

			query.put("$gte", arr_time[0]);
			query.put("$lte", arr_time[1]);
			BasicDBObject match = new BasicDBObject("$match", new BasicDBObject("routeStartTime", query));
						
			/////System.out.println("======>match="+match.toJson());

			// 求条数
			BasicDBObject group = new BasicDBObject("$group", new BasicDBObject("_id", null).append("total_duration",
					new BasicDBObject("$sum", "$durationTime")));

			pipeline.add(match);
			pipeline.add(group);
			
			////System.out.println("======>group="+group.toJson());

			System.out.println(" the aggregate pipeline : " + pipeline);
			AggregateIterable<Document> iterable = collection.aggregate(pipeline);
			MongoCursor<Document> its = null;

			if (iterable != null) {
				its = iterable.iterator();
				while (its.hasNext()) {
					Document doc = its.next();
					
					Object obj=doc.get("total_duration");
					
					System.out.println("====>val="+obj);
					
					if(obj instanceof Integer)
						result =(Integer)obj/ (60 * 60 * 1000);
					if(obj instanceof Long)
						result =(Long)obj / (60 * 60 * 1000);
					else if(obj instanceof Double)
						result =  (Double)obj/ (60 * 60 * 1000); 
				}
			}

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in aggregateCurrRouteDuration", e);

			throw new DbException(5001, "aggregateCurrRouteDuration error", e);
		}
		return result;
	}
	
	
	
	///////////////////////////
	
	
	
	
	/**
	 * 求总时长(小时)
	 * 
	 * @param conn
	 * @param db
	 * @param tableName
	 * @return
	 * @throws DbException
	 */
	public static double aggregateRouteTotalDuration(MongoClient conn, String db, String tableName) throws DbException {
		double result = 0;
		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

			List<BasicDBObject> pipeline = new ArrayList<>();

			// 求条数
			BasicDBObject group = new BasicDBObject("$group", new BasicDBObject("_id", null).append("total_duration",
					new BasicDBObject("$sum", "$durationTime")));

			pipeline.add(group);

			logger.info(" the aggregate pipeline : " + pipeline);
			AggregateIterable<Document> iterable = collection.aggregate(pipeline);
			MongoCursor<Document> its = null;

			if (iterable != null) {
				its = iterable.iterator();
				while (its.hasNext()) {
					Document doc = its.next();
					
					Object obj=doc.get("total_duration");
					if(obj instanceof Integer)
						result =(Integer)obj/ (60 * 60 * 1000);
					if(obj instanceof Long)
						result =(Long)obj / (60 * 60 * 1000);
					else if(obj instanceof Double)
						result =  (Double)obj/ (60 * 60 * 1000); 
				}
			}

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in aggregateRouteTotalDuration", e);

			throw new DbException(5001, "aggregateRouteTotalDuration error", e);
		}
		return result;
	}

	/**
	 * 求平均时长(分钟)
	 * 
	 * @param conn
	 * @param db
	 * @param tableName
	 * @return
	 * @throws DbException
	 */
	public static double aggregateRouteAvgDuration(MongoClient conn, String db, String tableName) throws DbException {
		double result = 0;
		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

			List<BasicDBObject> pipeline = new ArrayList<>();

			// 求条数
			BasicDBObject group = new BasicDBObject("$group",
					new BasicDBObject("_id", null).append("avg_duration", new BasicDBObject("$avg", "$durationTime")));

			pipeline.add(group);

			logger.info(" the aggregate pipeline : " + pipeline);
			AggregateIterable<Document> iterable = collection.aggregate(pipeline);
			MongoCursor<Document> its = null;

			if (iterable != null) {
				its = iterable.iterator();
				while (its.hasNext()) {
					Document doc = its.next();
					result = doc.getDouble("avg_duration") / (60 * 1000);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in aggregateVehicleCount", e);

			throw new DbException(5001, "aggregateVehicleCount error", e);
		}
		return result;
	}

	/**
	 * 汇总数据：车辆总数、总时长、总里程、平均时长 ------------- real_route_info
	 * 
	 * @param conn
	 * @param db
	 * @param tableName
	 * @return
	 * @throws DbException
	 */
	public static LinkedHashMap<String, Object> aggregateRouteData(MongoClient conn, String db, String tableName)
			throws DbException {
		LinkedHashMap<String, Object> result = new LinkedHashMap<>();

		// 总车辆数
		int vehicleNum = aggregateRouteTotalVehicleCount(conn, db, tableName);
		result.put("vehicleNum", vehicleNum);

		// 总里程(千米)
		double result1 = aggregateRouteTotalMileage(conn, db, tableName);
		result.put("totalMileage", result1);

		// 总时长（小时）
		double totalDuration = aggregateRouteTotalDuration(conn, db, tableName);
		result.put("totalDuration", totalDuration);
		// 平均时长
		double avgDuration = aggregateRouteAvgDuration(conn, db, tableName);
		result.put("avgDuration", avgDuration);

		return result;

	}

	/**
	 * 视频文件大小汇总求和
	 * 
	 * @param conn
	 * @param db
	 * @param tableName
	 * @param camtype
	 * @return
	 * @throws DbException
	 */
	public static long aggregateVideoFileSize(MongoClient conn, String db, String tableName, String camtype)
			throws DbException {
		long result = 0;
		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

			List<BasicDBObject> pipeline = new ArrayList<>();

			if (StringUtils.isNotBlank(camtype)) {
				// 添加时间查询条件
				BasicDBObject match = new BasicDBObject("$match", new BasicDBObject("camtype", camtype));
				pipeline.add(match);
			}

			// 求和
			BasicDBObject group = new BasicDBObject("$group",
					new BasicDBObject("_id", null).append("total_filesize", new BasicDBObject("$sum", "$fileSize")));

			pipeline.add(group);

			AggregateIterable<Document> iterable = collection.aggregate(pipeline);
			MongoCursor<Document> its = null;

			if (iterable != null) {
				its = iterable.iterator();
				while (its.hasNext()) {
					Document doc = its.next();
					result = Long.parseLong(String.valueOf(doc.get("total_filesize")));
				}
			}

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in aggregateVideoFileSize", e);

			throw new DbException(5001, "aggregateVideoFileSize error", e);
		}
		return result;
	}
	
	
	
	public static long aggregatelidarSize(MongoClient conn, String db, String tableName, Long beginTime, Long endTime)
			throws DbException {
		long result = 0;
		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

			List<BasicDBObject> pipeline = new ArrayList<>();

			
			
			///condition.put( "recordDate", new BasicDBObject("$gte",beginDate).append("$lte", endDate) );
		    // 添加时间查询条件
			BasicDBObject match = new BasicDBObject( "$match", new BasicDBObject("timestamp", new BasicDBObject("$gte",beginTime).append("$lte", endTime)  )  );
			pipeline.add(match);
 

			// 求和
			BasicDBObject group = new BasicDBObject("$group",  new BasicDBObject("_id", null).append("total_filesize", new BasicDBObject("$sum", "$fileSize")));

			pipeline.add(group);
			
			
			logger.info("===========>pipeline="+pipeline.toString());

			AggregateIterable<Document> iterable = collection.aggregate(pipeline);
			MongoCursor<Document> its = null;

			if (iterable != null) {
				its = iterable.iterator();
				while (its.hasNext()) {
					Document doc = its.next();
					
					Object tmp=doc.get("total_filesize");
					
					if(tmp instanceof Integer)
						result=Long.parseLong(""+tmp);
					else if(tmp instanceof Long)
						result=Long.parseLong(""+tmp);
					else if(tmp instanceof Double)
						result=  ((Double)tmp).longValue();
					
				}
			}

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in aggregatelidarSize", e);

			throw new DbException(5001, "aggregatelidarSize error", e);
		}
		return result;
	}
	

	/**
	 * 聚合查询
	 * 
	 * @param conn
	 * @param db
	 * @param tableName
	 * @param params
	 * @return
	 * @throws DbException
	 */
	public static List<Map<String, Object>> aggregateMongo(MongoClient conn, String db, String tableName,
			Map<String, String> params) throws DbException {
		List<Map<String, Object>> list = new ArrayList<>();

		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

			List<BasicDBObject> pipeline = new ArrayList<>();

			logger.info("aggregateMongo==>" + JSON.toJSONString(params));

			String matchJson = params.get("match");
			// 查询条件
			if (StringUtils.isNotBlank(matchJson)) {
				BasicDBObject match = new BasicDBObject("$match", BasicDBObject.parse(matchJson));
				pipeline.add(match);
			}
			String groupJson = params.get("group");
			// 分组条件
			if (StringUtils.isNotBlank(groupJson)) {
				BasicDBObject groupVal = BasicDBObject.parse(groupJson);
				String alias = params.get("alias");
				String funcJson = params.get("func");
				if (StringUtils.isNoneBlank(alias, funcJson)) {
					groupVal.append(alias, BasicDBObject.parse(funcJson));
				}

				BasicDBObject group = new BasicDBObject("$group", groupVal);
				pipeline.add(group);
			}

			String sortJson = params.get("sort");
			// 排序
			if (StringUtils.isNotBlank(sortJson)) {
				BasicDBObject sort = new BasicDBObject("$sort", BasicDBObject.parse(sortJson));
				pipeline.add(sort);
			}

			// 限制返回条数
			String limitVal = params.get("limit");
			if (StringUtils.isNotBlank(limitVal)) {
				BasicDBObject limit = new BasicDBObject("$limit", Long.parseLong(limitVal));
				pipeline.add(limit);
			}

			logger.info(" the aggregate pipeline:" + pipeline);

			AggregateIterable<Document> iterable = collection.aggregate(pipeline);
			MongoCursor<Document> its = null;

			if (iterable != null) {
				its = iterable.iterator();
				while (its.hasNext()) {

					Document doc = its.next();
					Set<String> keys = doc.keySet();

					HashMap<String, Object> map = new HashMap<String, Object>();

					for (String key : keys) {
						// map.put(key, doc.get(key));
						map.put(key, JSONObject.toJSON(doc.get(key)));
					}
					list.add(map);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in aggregateMongo", e);

			throw new DbException(5001, "aggregateMongo error", e);
		}
		return list;

	}

	

	/**
	 * 聚合查询
	 * 
	 * @param conn
	 * @param db
	 * @param tableName
	 * @param params
	 * @return
	 * @throws DbException
	 */
	public static List<Map<String, Object>> aggregateMongo(MongoClient conn, String db, String tableName,
			List<String> jsons) throws DbException {
		List<Map<String, Object>> list = new ArrayList<>();

		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

			List<BasicDBObject> pipeline = new ArrayList<>();

			if(jsons != null && !jsons.isEmpty()) {
				logger.info("");
				for (String json : jsons) {
					pipeline.add(BasicDBObject.parse(json));
				}
			}

			logger.info(" the aggregate pipeline:" + pipeline);

			AggregateIterable<Document> iterable = collection.aggregate(pipeline);
			MongoCursor<Document> its = null;

			if (iterable != null) {
				its = iterable.iterator();
				while (its.hasNext()) {

					Document doc = its.next();
					Set<String> keys = doc.keySet();

					HashMap<String, Object> map = new HashMap<String, Object>();

					for (String key : keys) {
						// map.put(key, doc.get(key));
						map.put(key, JSONObject.toJSON(doc.get(key)));
					}
					list.add(map);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in aggregateMongo", e);

			throw new DbException(5001, "aggregateMongo error", e);
		}
		return list;

	}
	
	/***
	 * 表连接
	 * 
	 * @param conn
	 * @param db
	 * @param tableName
	 * @param params
	 * @return
	 * @throws DbException
	 */
	public static MongoResultVo aggregateJoinMongo(MongoClient conn, String db, String tableName,
			Map<String, String> params,boolean flag) throws DbException {
		List<Map<String, Object>> list = new ArrayList<>();
		MongoResultVo mongoResultVo = new MongoResultVo();
		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

			List<BasicDBObject> pipeline = new ArrayList<>();
			Long total = 0L;

			logger.info("queryJsonMongo==>" + JSON.toJSONString(params));

			// 查询条件
			String matchJson = params.get("match");
			if (StringUtils.isNotBlank(matchJson)) {
				BasicDBObject match = new BasicDBObject("$match", BasicDBObject.parse(matchJson));
				pipeline.add(match);
			} else {
				matchJson = "{}";
			}
			// 求总条数
			if(flag){
				total = collection.count(BasicDBObject.parse(matchJson));
			}
			mongoResultVo.setTotal(total);

			String from = params.get("from");
			String localField = params.get("localField");
			String foreignField = params.get("foreignField");
			String as = params.get("as");

			if (StringUtils.isNoneBlank(from, localField, foreignField, as)) {

				BasicDBObject lookup = new BasicDBObject("$lookup", new BasicDBObject("from", from)
						.append("localField", localField).append("foreignField", foreignField).append("as", as));
				pipeline.add(lookup);
			}

			logger.info("queryJsonMongo==>" + JSON.toJSONString(params));

			// 查询条件
			String innerMatchJson = params.get("innerMatch");
			if (StringUtils.isNotBlank(innerMatchJson)) {
				BasicDBObject innerMatch = new BasicDBObject("$match", BasicDBObject.parse(innerMatchJson));
				pipeline.add(innerMatch);
			}

			String sortJson = params.get("sort");
			// 排序
			if (StringUtils.isNotBlank(sortJson)) {
				BasicDBObject sort = new BasicDBObject("$sort", BasicDBObject.parse(sortJson));
				pipeline.add(sort);
			}

			// 跳过结果的数量
			String skipVal = params.get("skip");
			if (StringUtils.isNotBlank(skipVal)) {
				BasicDBObject skip = new BasicDBObject("$skip", Long.parseLong(skipVal));
				pipeline.add(skip);
			}

			// 限制返回条数
			String limitVal = params.get("limit");
			if (StringUtils.isNotBlank(limitVal)) {
				BasicDBObject limit = new BasicDBObject("$limit", Long.parseLong(limitVal));
				pipeline.add(limit);
			}

			// 限制返回条数
			String unwindJson = params.get("unwind");
			if (StringUtils.isNotBlank(unwindJson)) {
				BasicDBObject unwind = new BasicDBObject("$unwind", "$inventory_docs");
				pipeline.add(unwind);
			}
			logger.info(" the aggregate pipeline:" + pipeline);

			AggregateIterable<Document> iterable = collection.aggregate(pipeline);
			MongoCursor<Document> its = null;

			if (iterable != null) {
				its = iterable.iterator();
				while (its.hasNext()) {
					Document doc = its.next();

					Set<String> keys = doc.keySet();

					HashMap<String, Object> map = new HashMap<String, Object>();

					for (String key : keys) {
						// map.put(key, doc.get(key));
						map.put(key, JSONObject.toJSON(doc.get(key)));
					}

					list.add(map);
				}
				mongoResultVo.setRes(list);
			}

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in aggregateJoinMongo", e);

			throw new DbException(5001, "aggregateJoinMongo error", e);
		}
		return mongoResultVo;
	}

	/**
	 *
	 * @param db
	 *            数据库
	 * @param colName
	 *            要查询的表
	 * @param column
	 *            条件查询的字段
	 * @param sortColumn
	 *            需要根据排序查找最新的字段
	 * @param value
	 * @return 根据确定值进行查找
	 */
	public static Object getLastRecord(String db, String colName, String column, String sortColumn, String value,
			String type) {
		try {
			// 连接到数据库
			MongoDatabase mongoDatabase = mongoClient.getDatabase(db);
			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			BasicDBObject condition = new BasicDBObject();
			condition.put(column, value);
			condition.put("type", type);
			Document beginTime = collection.find(condition).sort(new BasicDBObject(sortColumn, -1)).first();
			if (beginTime.get("beginTime") != null) {
				return beginTime.get("beginTime");
			} else {
				logger.info("Mongodb获取最新数据getLastRecord:null");
				return null;
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error("获取最新数据出错:" + e.getMessage());
		}
		return null;
	}

	/***
	 * 2019-10-17 liaoxb add  根据经纬度查找最近的车道
	 *
	 * colName: map_lane
	 *
	 * uid:    车道id
	 * rc_id:  车道中心线id---真正使用的
	 * direction:   1:单向，   2：双向
	 * min_speed
	 * max_speed
	 * **/
	public static HashMap<String, Object> getLaneInfoByPoint(MongoClient conn, String db, String colName,  double[] points) throws DbException {
		HashMap<String, Object> map = new HashMap<String, Object>();

		try {

			// 连接到数据库
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////

			//// 2.test cycle query
			BasicDBObject condition = new BasicDBObject();

			// condition.put(F_LOCATION, new BasicDBObject("$near",
			// new BasicDBObject("$geometry",
			// new BasicDBObject("type","Point")
			// .append("coordinates", points))));

			double maxDistance=1000;

//			condition.put("geom",
//					new BasicDBObject("$near",
//							new BasicDBObject("$geometry",
//									new BasicDBObject("type", "Point").append("coordinates", points))
//											.append("$maxDistance", maxDistance)
//											)
//
//			);

			condition.put("geom",
					new BasicDBObject("$near",
							new BasicDBObject("$geometry",
									new BasicDBObject("type", "Point").append("coordinates", points))
					)

			);

			//logger.info("condition=" + condition);

			FindIterable<Document> findIterable = collection.find(condition).limit(1);

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				for (String key : keys) {
					map.put(key, d.get(key));
				}

			}

			//logger.info("count=" + count);

		} catch (Exception e) {
			e.printStackTrace();

			//logger.error("in getLaneInfoByPoint", e);
			throw new DbException(8002, "getLaneInfoByPoint error", e);
		} finally {

		}

		return map;
	}

	/**
	 * 保存数据到
	 * @param db
	 * @param colName
	 * @param jsonCondition
	 * @param fields
	 * @param file
	 * @throws IOException 
	 */
	public static long saveData2File(String db, String colName, String jsonCondition,List<String> fields, File file,boolean append) throws IOException {
		MongoDatabase mongoDatabase = mongoClient.getDatabase(db);
		MongoCollection<Document> collection = mongoDatabase.getCollection(colName);

		if (jsonCondition == null || jsonCondition.trim().equals("") || jsonCondition.trim().equals("null"))
			jsonCondition = "{}";
		// 查询条件
		BasicDBObject queryDb = BasicDBObject.parse(jsonCondition);
		logger.info("=====>queryDb=" + queryDb);

		////////////////////////////////////////////
		BasicDBObject fieldDb = new BasicDBObject();
		fieldDb.put("_id", 0);
		if (fields != null && fields.size() > 0) {
			fields.forEach(field->fieldDb.put(field, 1));
		}
		
		
        BufferedWriter writer = new BufferedWriter(new FileWriter(file,append));
    	
		
		FindIterable<Document> findIterable = collection.find(queryDb).projection(fieldDb);
		long count = 0;
		StringBuffer buffer = new StringBuffer(); 
		for (Document d : findIterable) {
			count ++;
			buffer.append(d.toJson());
			buffer.append("\n");
			//1000条数据写一次文件，以防数据量过大时内存溢出
			if(count % 1000 == 0){
				writer.write(buffer.toString());
				writer.flush();
				buffer.delete(0, buffer.length());
			}
		}
		
		logger.info("=====>count=" + count);
		//写入文件
		writer.write(buffer.toString());
		writer.flush();
		//关闭流
		writer.close();
		return count;
	}
	
	public static Object getFieldValue(String colName,String jsonCondition, String field){
		MongoDatabase mongoDatabase = mongoClient.getDatabase(MONGO_DB_NAME);
		MongoCollection<Document> collection = mongoDatabase.getCollection(colName);
		if(StringUtils.isBlank(jsonCondition)){
			jsonCondition = "{}";
		}
		BasicDBObject queryDb = BasicDBObject.parse(jsonCondition);
		BasicDBObject fieldDb = new BasicDBObject();
		fieldDb.put("_id", 0);
		fieldDb.put(field, 1);
		FindIterable<Document> findIterable = collection.find(queryDb).projection(fieldDb).limit(1);
		Document document = findIterable.first();
		if(document != null){
			return document.get(field);
		}
		return null;
	}

	///db.rpt_rsi.update({rsiId:"RSI1001",day:"2020408",hour:"10"}, {$set: {rsuId:"R1000E",rsiId:"RSI1001",day:"2020408",hour:"10",week:"2" }, $inc: {num:1} },  true, false)
	public static String updateOneWithIncByCondition(MongoClient conn, String db, String colName, String whereJson,
			String valJson, boolean isUpsert) throws DbException {
		String result = null;

		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);

			Document document = Document.parse(valJson);

			Document filter = Document.parse(whereJson);

			// 注意update文档里要包含"$set"字段
			Document update = new Document();
			update.append("$set", document);
			
			Document inc = new Document();
			inc.put("num", 1);
			update.append("$inc", inc);
			
			System.out.println("=====>update= " + update.toJson());
			
			UpdateOptions updateOptions = new UpdateOptions();
			updateOptions.upsert(isUpsert);
			

			UpdateResult res = collection.updateOne(filter, update, updateOptions);
			result = String.valueOf(res.getModifiedCount());
			System.out.println("matched count = " + res.getMatchedCount());
			logger.debug("matched count = " + res.getMatchedCount());

		} catch (Exception e) {
			// 
			e.printStackTrace();
			logger.error("in updateOneWithIncByCondition", e);

			throw new DbException(2011, "updateOneWithIncByCondition error", e);
		}

		return result;

	}
	
	public static long updateManyMongo2(MongoClient conn, String db, String colName, String whereJson, String valJson) throws DbException {
		long nums=0;

		try {
			if (conn == null)
				conn = mongoClient;

			MongoDatabase mongoDatabase = conn.getDatabase(db);

			MongoCollection<Document> collection = mongoDatabase.getCollection(colName);

			Document document = Document.parse(valJson);

			////// System.out.println("document"+document);

			///// db.a1.update({_id:{$eq:1003}},{$set:{_id:1003,name:'lxb',age:40}},true,false)

			///// collection.insertOne(document );

			Document filter = Document.parse(whereJson);

			// 注意update文档里要包含"$set"字段
			Document update = new Document();
			update.append("$set", document);

			/////////// 没有不新增，有就update
			UpdateOptions updateOptions = new UpdateOptions();
			updateOptions.upsert(false);

			UpdateResult res = collection.updateMany(filter, update, updateOptions);
			nums=res.getMatchedCount();

			logger.info("matched count = " + nums);

		} catch (Exception e) {
			// 
			e.printStackTrace();
			logger.error("in updateManyMongo2", e);

			throw new DbException(2011, "updateManyMongo2 error", e);
		}

		return nums;

	}
	
//	public static void main(String[] args) throws DbException {
////		String match = "{$match:{\"gpstime\":{\"$gte\":1584436117603,\"$lt\":1584436119603},\"rsuId\":{\"$exists\":true}}}";
////		String project = "{$project:{\"_id\":0,\"rsuId\":1,\"vehicleId\":1}}";
////		String group = "{$group:{\"_id\":{rsuId:\"$rsuId\",vehicleId:\"$vehicleId\"}}}";
////		String limit = "{$group:{\"_id\":\"$rsuId\",vehCount:{$sum:1}}}";
////		List<String> conditions = new ArrayList<String>();
////		conditions.add(match);
////		conditions.add(project);
////		conditions.add(group);
////		conditions.add(limit);
////		List<Map<String, Object>> map = aggregateMongo(null, "svr", "bsm_data_test", conditions);
////		for (Map<String, Object> map2 : map) {
////			System.out.println(map2);
////		}
//		
////		List<double[]> polygons=new ArrayList<double[]>();
////		double[] tmp1=new double[2];
////		tmp1[0]=114.6609220;
////		tmp1[1]=38.2773610;
////		
////		polygons.add(tmp1);
////		
////		double[] tmp2=new double[2];
////		tmp2[0]=114.660338;
////		tmp2[1]= 38.276468;
////		
////		polygons.add(tmp2);
////		
////		
////		double[] tmp3=new double[2];
////		tmp3[0]=114.660131;
////		tmp3[1]= 38.276575;
////		
////		polygons.add(tmp3);
////		
////		
////		double[] tmp4=new double[2];
////		tmp4[0]=114.660696;
////		tmp4[1]=38.277498;
////		
////		polygons.add(tmp4);
////		
////		polygons.add(tmp1);
////		
////		List<Map<String, Object>> ls=queryCarInfoByPolygon4(null, "qdyk", "car_real_info",
////				null, null, null, null, polygons, 0,0);
//		
//		
////		List<DriverTotalVo> ls=aggrateVehicleRouteTotal(null, "qdyk", "real_route_info",
////				"A1911045");
////		System.out.println("ls="+JSON.toJSONString(ls));
//		
//		List<DurationVo> ls2=aggrateVehicleRouteByDay(null, "qdyk", "real_route_info_202010",
//				"S22F000O", "20201001", "20201030");
//		System.out.println("ls2="+JSON.toJSONString(ls2));
//		
//		
//		
//	}
	
	
	
	public static MongoResultVo doQueryLidar(MongoClient conn, String dbName, String tableName, String deviceId, String beginTime, String endTime, String orderJson, int skip, int limit)
					throws DbException {

		MongoResultVo resultVo = new MongoResultVo();

		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		long total = 0L;
		try {

			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase(dbName);
			//// System.out.println("Connect to database successfully");

			MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);
			//// System.out.println("集合选择成功");

			///////////////////////////////////////////////////////////////////
			////// BasicDBObject condition = new BasicDBObject();

			// 查询两个时间范围的，用map包装一下
			///// query3.put( "recordDate", new BasicDBObject("$gte",
			// 20180101000000L).append("$lte", 20500101000000L) );

			//// condition.put( "vin", new BasicDBObject("$eq", vin) );
			// if(vin!=null && !vin.trim().equals("") &&
			// !vin.trim().equals("null"))
			// condition.put( "vin", vin);

			/////////// condition.put( "recordDate", new BasicDBObject("$gte",beginDate).append("$lte", endDate) );
			String jsonCondition = "";

			BasicDBObject cond = new BasicDBObject();

			///////////////////////////////////////////
			if (beginTime == null || beginTime.trim().equals("") || beginTime.trim().equals("null"))
				beginTime = "20190101000000";

			if (endTime == null || endTime.trim().equals("") || endTime.trim().equals("null"))
				endTime = "20500308122100";

			jsonCondition = "{ " + " $or: [ " + " {$and:[{\"beginTime\":{\"$gte\":\"" + beginTime + "\",\"$lte\":\""
					+ endTime + "\"}},{\"endTime\":{\"$gte\":\"" + beginTime + "\", \"$lte\":\"" + endTime + "\"}}]},  "
					+ " {$and:[{\"beginTime\":{\"$gte\":\"" + beginTime + "\",\"$lte\":\"" + endTime
					+ "\"}},{\"endTime\":{\"$gte\":\"" + beginTime + "\", \"$gte\":\"" + endTime + "\"}}]},  "
					+ " {$and:[{\"beginTime\":{\"$lte\":\"" + beginTime + "\",\"$lte\":\"" + endTime
					+ "\"}},{\"endTime\":{\"$gte\":\"" + beginTime + "\", \"$lte\":\"" + endTime + "\"}}]},  "
					+ " {$and:[{\"beginTime\":{\"$lte\":\"" + beginTime + "\",\"$lte\":\"" + endTime
					+ "\"}},{\"endTime\":{\"$gte\":\"" + beginTime + "\", \"$gte\":\"" + endTime + "\"}}]}   " + " ] }";

			cond = BasicDBObject.parse(jsonCondition);
 

			if (deviceId != null && !deviceId.trim().equals("") && !deviceId.trim().equals("null")) {

				cond.append("deviceId", deviceId);
			}
 

			///////////////////////////////////////////
			// if(fileId!=null && !fileId.trim().equals("") &&
			// !fileId.trim().equals("null"))
			// jsonConditionBuff.append("\"fileid\":{$regex:/"+fileId+"/},");
			//
			// if(vid!=null && !vid.trim().equals("") &&
			// !vid.trim().equals("null"))
			// jsonConditionBuff.append("\"vid\":{$regex:/"+vid+"/},");
			//
			// if(camId!=null && !camId.trim().equals("") &&
			// !camId.trim().equals("null"))
			// jsonConditionBuff.append("\"camid\":{$regex:/"+camId+"/},");
			//
			// if(source!=null && !source.trim().equals("") &&
			// !source.trim().equals("null"))
			// jsonConditionBuff.append("\"source\": "+source+",");

			logger.info("====111111111=>jsonCondition=" + jsonCondition);

			//////// 查询条件
			BasicDBObject queryDb = cond;
			//// BasicDBObject queryDb = BasicDBObject.parse(jsonCondition);
			logger.info("------>in doQueryVideo, queryDb=" + queryDb);

			/////////////// liaoxb add
			if (orderJson == null || orderJson.trim().equals("") || orderJson.trim().equals("null")) {
				orderJson = "{\"_id\": 1}";
				//// orderJson="{}";
			}
			logger.info("=====>orderJson=" + orderJson);
			BasicDBObject orderDb = BasicDBObject.parse(orderJson);
			logger.info("=====>orderDb=" + orderDb);

			////////////////////////////////////////////////////////

			total = collection.count(queryDb);
			logger.info("total=" + total);

			FindIterable<Document> findIterable = null;

			if (skip > 0 && limit > 0) {
				findIterable = collection.find(queryDb).sort(orderDb).skip(skip).limit(limit);
			} else if (skip == 0 && limit > 0) {
				findIterable = collection.find(queryDb).sort(orderDb).limit(limit);
			} else {
				findIterable = collection.find(queryDb).sort(orderDb);
			}

			long count = 0;
			for (Document d : findIterable) {

				count++;
				//// System.out.println("=====>d="+d);
				Set<String> keys = d.keySet();

				HashMap<String, Object> map = new HashMap<String, Object>();

				for (String key : keys) {
					map.put(key, d.get(key));
				}

				result.add(map);

			}

			logger.info("count=" + count);
			resultVo.setRes(result);
			resultVo.setTotal(total);

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in doQueryLidar", e);

			throw new DbException(3012, "doQueryLidar error", e);
		} finally {

		}

		return resultVo;

	}
	
	
	public static void queryMongoStats(MongoClient conn) throws DbException {
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();

		try {
			if (conn == null)
				conn = mongoClient;

			// 连接到数据库
			MongoDatabase mongoDatabase = conn.getDatabase("qdyk");
			//// System.out.println("Connect to database successfully");
			Document doc=new Document();
			
			doc=Document.parse("{ \"dbStats\": 1, \"scale\": 1 }");
			
			Document res=mongoDatabase.runCommand(doc);
			
			System.out.println("=====>res="+res.toJson());
			
			
			MongoCollection<Document> collection = mongoDatabase.getCollection("car_data");
			
			ListIndexesIterable<Document> v_indexes=collection.listIndexes();
			
			for(Document index: v_indexes)
			{
				System.out.println("=====>index="+index.toJson());
			}
			
			

		} catch (Exception e) {
			e.printStackTrace();

			logger.error("in queryMongo", e);
 
		} finally {

		}

 
	}
	

	public static void main(String [] args)
	{
		
//		BasicDBObject queryDb = new BasicDBObject();
//		
//		String withExclusiveStartKey="5f8973dc45197b77005ac43f";
//		
//		if(withExclusiveStartKey!=null && !withExclusiveStartKey.trim().equals(""))
//		{
//			ObjectId _id=new ObjectId(withExclusiveStartKey);
//			
//			System.out.println("============>_id="+_id);
//			
//			queryDb.put("_id", (new BasicDBObject()).append("$gte", _id));
//			
//			System.out.println("============>queryDb="+queryDb.toJson());
//		}
//		else
//			System.out.println("aaaaaaaaa="+withExclusiveStartKey);
//		
		
		
		try {
			/////double d=aggregateCurrRouteDuration(null, "qdyk", "real_route_info", "20201104");
			
//			List<Map<String, Object>> ls=queryMongo(null, "qdyk", "real_route_info",
//					"{}", 0, 10);
//			
//			System.out.println("============>ls="+JSON.toJSONString(ls));
			
			
			queryMongoStats(null);
			
			
		} catch (DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
}
