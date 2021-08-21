package com.qianfeng;

import com.alibaba.druid.pool.DruidDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;


//druid连接池工具类
public class DruidUtil implements Serializable {
    //获取logger
    private static final Logger logger = LoggerFactory.getLogger(DruidUtil.class);

    //定义DruidDataSource
    public DruidDataSource dataSource;

    //连接相关参数
    private String driver;
    private String url;
    private String user;
    private String pass;

    //连接池初始化参数
    public static final Integer iniSize = 5;
    public static final Integer maxActive = 20;
    public static final Integer minIdle = 5;
    public static final Integer maxWait = 5*10000;
    public static final Integer abandonedTimeOut = 600;


    //获取一个对象，，对象中初始化连接信息参数
    public DruidUtil(String driver, String url, String user, String pass) {
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.pass = pass;
    }

    public DruidUtil(){}

    //获取datasource
    public DruidDataSource getDruidDataSource() {
        //判断datasource是否为空
        if(dataSource == null){
            dataSource = new DruidDataSource();
            //将获取连接资源的参数给他
            dataSource.setDriverClassName(driver);
            dataSource.setUrl(url);
            dataSource.setUsername(user);
            dataSource.setPassword(pass);

            //设置连接池初始化信息
            dataSource.setInitialSize(iniSize);
            dataSource.setMaxActive(maxActive);
            dataSource.setMinIdle(minIdle);
            dataSource.setMaxWait(maxWait);

            //设置是否超时回收
            dataSource.setRemoveAbandoned(true);
            dataSource.setRemoveAbandonedTimeout(abandonedTimeOut); //单位是秒
        }
        //返回
        return dataSource;
    }

    //获取connection
    public Connection getConnection(){
        //定义connection对象
        Connection conn = null;
        try {
            DruidDataSource druidDataSource = getDruidDataSource();
            if(druidDataSource != null){
                conn = druidDataSource.getConnection();  //从源获取连接
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("druiddatasource may by null");
        }
        //返回连接对象
        return conn;
    }

    //一般不关闭
    public void close(){
        if(dataSource != null){
            dataSource.close();
        }
    }

    //执行sql语句
    public Map<String,String> execSQLJson(String sql, String schema, String pk) throws SQLException {
        //获取mysql的三个对象
        Connection conn = getConnection();
        PreparedStatement ppst = null;
        ResultSet rs = null;
        Map<String,String> result = new HashMap<String,String>();
        //判断连接是否为空
        if(null != conn){
            String exeSchema = schema.replaceAll("\\s*", "");
            ppst = conn.prepareStatement(sql);
            rs = ppst.executeQuery();
            Map<String,Object> rowValues = new HashMap<String,Object>();
            while(rs.next()){
                String pkKey = rs.getObject(pk).toString(); //获取主键的值

                String[] fieldNames = exeSchema.split(",");
                //循环schema信息，
                for(String fieldName : fieldNames){
                    Object fValue = rs.getObject(fieldName);
                    rowValues.put(fieldName, fValue);
                }
                //将对象转换成json字符串
                String rowJson = JsonUtil.gObject2Json(rowValues);
                //封装返回结果
                result.put(pkKey, rowJson);
                rowValues.clear();
            }
        }
        return result;
    }



    //测试
    public static void main(String[] args) throws SQLException {
        //PropertyUtil.readProperties("jdbc:")

        Connection conn = new DruidUtil(
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://hadoop01:3306/travel?serverTimezone=UTC&characterEncoding=utf-8",
                "root",
                "root").getConnection();

        System.out.println(conn);


        //测试数据查询
        String sql = "select product_id, product_level, product_type, departure_code, des_city_code, toursim_tickets_type from dim_product1";
        String schema = "product_id, product_level, product_type, departure_code, des_city_code, toursim_tickets_type";
        String pk = "product_id";

        Map<String,String> results2 = new DruidUtil(
                "com.mysql.jdbc.Driver",
                "jdbc:mysql://hadoop01:3306/travel?serverTimezone=UTC&characterEncoding=utf-8&useSSL=false",
                "root",
                "root").execSQLJson(sql, schema, pk);

        for(Map.Entry<String,String> entry : results2.entrySet()){
            String key = entry.getKey();  //主键
            String value = entry.getValue();  //主键对应的郑航值
            Map<String,Object> row = JsonUtil.json2object(value, Map.class);

            //String values = entry.getValue();
            //Row value = JsonUtil.json2object(values, Row.class);
            String productID = row.get("product_id").toString();
            String productLevel = row.get("product_level").toString();
            String productType = row.get("product_type").toString();
            String toursimType = row.get("toursim_tickets_type").toString();
            System.out.println("productID=>" + productID+",key"+key+","+productLevel+","+productType);
        }
    }
}
