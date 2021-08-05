package com.tusvn.ccinfra.api.data.storage;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import com.tusvn.ccinfra.config.ConfigProxy;

/**
 * @author 作者 wangmh:
 * @version 创建时间：2018年4月10日 上午10:43:52 数据库连接工具类
 */
public class DBUtil {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DBUtil.class);

	/**
	 * 定义一个连接池对象
	 */
//	private static DataSource ds;
//	static {
//		try {
//			Properties props = new Properties();
//			props.load(DBUtil.class.getClassLoader().getResourceAsStream("mysql.properties"));
//			/*
//			 * 得到一个连接池对象
//			 */
//			ds = DruidDataSourceFactory.createDataSource(props);
//		} catch (Exception e) {
//			e.printStackTrace();
//			throw new ExceptionInInitializerError("初始化连接错误，请检查配置文件！");
//		}
//	}
	
	
	/**
	 * 定义一个连接池对象
	 */
	private static DataSource ds;
	static {
		try {
			String namespace = "scene_stat";
			Properties props = new Properties();
			String driverClassName 					= ConfigProxy.getPropertyOrDefault(namespace, "mysql.driverClassName", "com.mysql.jdbc.Driver");
			String url 								= ConfigProxy.getPropertyOrDefault(namespace,  "mysql.url",             "jdbc:mysql://172.17.3.210:3306/wl_base_info?allowMultiQueries=true");
			String username 						= ConfigProxy.getProperty(namespace, "mysql.user" );
			String password 						= ConfigProxy.getProperty(namespace, "mysql.password");
			String initialSize 						= ConfigProxy.getPropertyOrDefault( namespace, "mysql.initialSize", 					"10");
			String maxActive 						= ConfigProxy.getPropertyOrDefault( namespace, "mysql.maxActive",   					"20");
			String maxWait 							= ConfigProxy.getPropertyOrDefault( namespace, "mysql.maxWait",     					"30000");
			String timeBetweenEvictionRunsMillis 	= ConfigProxy.getPropertyOrDefault( namespace, "mysql.timeBetweenEvictionRunsMillis", 	"60000");   ///配置间隔多久才进行一次检测，检测需要关闭的空闲连接，单位是毫秒
			String minEvictableIdleTimeMillis		= ConfigProxy.getPropertyOrDefault( namespace, "mysql.minEvictableIdleTimeMillis",    	"300000");  ///配置一个连接在池中最小生存的时间，单位是毫秒

			props.setProperty("driverClassName", 				driverClassName);
			props.setProperty("url", 							url);
			props.setProperty("username", 						username);
			props.setProperty("password", 						password);
			props.setProperty("initialSize", 					initialSize);
			props.setProperty("maxActive", 						maxActive);
			props.setProperty("maxWait", 						maxWait);
			props.setProperty("timeBetweenEvictionRunsMillis", 	timeBetweenEvictionRunsMillis);
			props.setProperty("minEvictableIdleTimeMillis",    	minEvictableIdleTimeMillis);
			/*
			 * 得到一个连接池对象
			 */
			ds = DruidDataSourceFactory.createDataSource(props);
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error("MYSQL初始化连接错误，请检查配置！");
		}
	}
	

	/**
	 * 从池中获取一个连接
	 * 
	 * @return
	 * @throws SQLException
	 */
	public static Connection getConnection() throws SQLException {
		return ds.getConnection();
	}

	/**
	 * 查询多条记录
	 * @param sql
	 * @return
	 */
	public static List<Map<String,Object>> selectList(String sql) {
		LOGGER.info("Exec select sql:" + sql);
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery(sql);
			List<Map<String,Object>> list = convertList(rs);
			return list;
		} catch (SQLException e) {
			LOGGER.error("查询数据异常:" + e.getMessage());
		}finally {
			closeAll(rs,ps,conn);
		}
		return null;
       
	}
	
	/**
	 * 查询多条记录,此处选择不捕获,抛出异常由外层方法处理
	 * @param sql
	 * @return
	 * @throws Exception 
	 */
	public static List<Map<String,Object>> selectListNoCatch(String sql) throws Exception {
		LOGGER.info("Exec select sql:" + sql);
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery(sql);
			List<Map<String,Object>> list = convertList(rs);
			return list;
		}finally {
			closeAll(rs,ps,conn);
		}
       
	}
	
	/**
	 * 查询一条记录
	 * @param sql
	 * @return
	 */
	public static Map<String,Object> selectOne(String sql) {
		LOGGER.info("Exec select sql:" + sql);
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery(sql);
			Map<String,Object> map = convertMap(rs);
			return map;
		} catch (SQLException e) {
			LOGGER.error("查询数据异常:" + e.getMessage());
		}finally {
			closeAll(rs,ps,conn);
		}
		return null;
       
	}
	/**
	 * 查询一条记录,此处不捕获,抛出异常由外层方法处理
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
	public static Map<String,Object> selectOneNoCatch(String sql) throws Exception {
		LOGGER.info("Exec select sql:" + sql);
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery(sql);
			Map<String,Object> map = convertMap(rs);
			return map;
		}finally {
			closeAll(rs,ps,conn);
		}
	}
	public static Map<String,Object> selectOne(String sql, String schema) {
		LOGGER.info("Exec select sql:" + sql);
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			conn.setSchema(schema);
			
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery(sql);
			Map<String,Object> map = convertMap(rs);
			return map;
		} catch (SQLException e) {
			LOGGER.error("查询数据异常:" + e.getMessage());
		}finally {
			closeAll(rs,ps,conn);
		}
		return null;
       
	}
	
	
	/**
	 * 查询id对应某个字段并且id唯一
	 * @param sql
	 * @return
	 */
	public static String selectColumnById(String sql) {
		
		LOGGER.info("Exec select sql:" + sql);
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery(sql);
			if (rs.next()) {
				return rs.getString(1);
			}
		} catch (SQLException e) {
			LOGGER.error("查询数据异常:" + e.getMessage());
		}finally {
			closeAll(rs,ps,conn);
		}
		return null;
       
	}
	
	/**
	 * 查询id对应某个字段并且id唯一
	 * @param sql
	 * @return
	 * @throws Exception 
	 */
	public static String selectColumnByIdNoCatch(String sql) throws Exception {
		
		LOGGER.info("Exec select sql:" + sql);
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery(sql);
			if (rs.next()) {
				return rs.getString(1);
			}
		}finally {
			closeAll(rs,ps,conn);
		}
		return null;
       
	}
 

	/**
	 * 功能:针对单条记录执行更新操作(新增、修改、删除)
	 * 
	 * @param sql
	 * @return
	 * @throws Exception
	 */
	public static int executeupdate(String sql) throws Exception {
		LOGGER.info("Exec update sql:" + sql);
		Connection conn = null;
		PreparedStatement ps = null;
		int num = -1;
		try {
			conn = getConnection();
			ps = conn.prepareStatement(sql);
			num = ps.executeUpdate();
			return num;
		} catch (SQLException sqle) {
			LOGGER.error("insert/update/delete  data Exception: " + sqle.getMessage());
		} finally {
			closeAll(null, ps, conn);
		}
		return num;
	}
	


	/**
	 * 关闭资源
	 * 
	 * @param rs
	 * @param ps
	 * @param conn
	 */
	public static void closeAll(ResultSet rs, PreparedStatement ps, Connection conn) {
		if (rs != null) {
			try {
				rs.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		if (ps != null) {
			try {
				ps.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		if (conn != null) {
			try {
				conn.close();// 关闭conn
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	/**
	 * ResultSet结果集转List
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	private static List<Map<String,Object>> convertList(ResultSet rs) throws SQLException{
		List<Map<String,Object>> list = new ArrayList<>();
		/*
		 * 获取键名
		 */
		ResultSetMetaData md = rs.getMetaData();
		/*
		 * 获取行的数量
		 */
		int columnCount = md.getColumnCount();
		while (rs.next()) {
			/*
			 * 声明Map
			 */
			Map<String,Object> rowData = new HashMap<>();
			for (int i = 1; i <= columnCount; i++) {
				/*
				 * 获取键名及值
				 */
				rowData.put(md.getColumnLabel(i), rs.getObject(i));
			}
			list.add(rowData);
		}
		return list;
	}
	/**
	 * ResultSet结果集转Map
	 * @param rs
	 * @return
	 * @throws SQLException
	 */
	private static Map<String,Object> convertMap(ResultSet rs) throws SQLException{
		List<Map<String,Object>> resultList = convertList(rs);
		if(resultList != null && resultList.size()>0){
			return resultList.get(0);
		}
		return null;
	}
	
	public static List<Map<String,Object>> selectList(String sql, String schema) {
		LOGGER.info("Exec select sql:" + sql);
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			conn.setSchema(schema);
			
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery(sql);
			List<Map<String,Object>> list = convertList(rs);
			return list;
		} catch (SQLException e) {
			LOGGER.error("查询数据异常:" + e.getMessage());
		}finally {
			closeAll(rs,ps,conn);
		}
		return null;
       
	}
	
	public static int executeupdate(String sql, String schema) throws Exception {
		LOGGER.info("Exec update sql:" + sql);
		Connection conn = null;
		PreparedStatement ps = null;
		int num = -1;
		try {
			conn = getConnection();
			conn.setSchema(schema);
			ps = conn.prepareStatement(sql);
			num = ps.executeUpdate();
			return num;
		} catch (SQLException sqle) {
			LOGGER.error("insert/update/delete  data Exception: " + sqle.getMessage());
		} finally {
			closeAll(null, ps, conn);
		}
		return num;
	}
	
	public static String selectColumnById(String sql, String schema) {
		
		LOGGER.info("Exec select sql:" + sql);
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			conn.setSchema(schema);
			
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery(sql);
			if (rs.next()) {
				return rs.getString(1);
			}
		} catch (SQLException e) {
			LOGGER.error("查询数据异常:" + e.getMessage());
		}finally {
			closeAll(rs,ps,conn);
		}
		return null;
       
	}
	
	public static void main(String[] args) {
//		String appId = "app";
//		String sql = "select client_secret from oauth_client_details where client_id = '"+appId+"' ";
//		
//		String result = selectColumnById(sql);
//		System.out.println("result : " +result);
//		
		
		
		Map<String,String> map = new HashMap<>();
		String carPlate = "苏E-G08P1";
		String sql  = "select vin,car_model_id as carModelId,org_id as orgid from car_info where car_plate = '"+ carPlate  +"'";
		
		Map<String,Object> result = DBUtil.selectOne(sql);
		for (Entry<String, Object> entry : result.entrySet()) {
			map.put(entry.getKey(), entry.getValue().toString());
		}
		System.out.println("map:"+map);
	}
	
}
