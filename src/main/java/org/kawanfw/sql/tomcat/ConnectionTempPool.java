package org.kawanfw.sql.tomcat;

import org.apache.tomcat.jdbc.pool.PoolProperties;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ClassName:org.kawanfw.sql.tomcat.ConnectionTempPool
 * Author:Chen.yiwan
 * Date:2020/6/23
 */
public class ConnectionTempPool {

    private static AtomicInteger tempBorrowCount = new AtomicInteger(0);
    private static PoolProperties poolProperties;
    private static List<Map> tempConnList = new ArrayList<>();
    //监听连接N分钟强行断开
    private static int checkTime=3;

    /**
     * 监听连接 ${checkTime} 分钟强行断开
     */
    static {
        new Thread(() -> {
            while (true) {
                try{
                    System.out.println("=================== check tempBorrowConn start......");
                    long currTime=System.currentTimeMillis();
                    for(int i=0;i<tempConnList.size();i++)
                    {
                        Map tempConnMap=tempConnList.get(i);
                        if(currTime-Long.parseLong(tempConnMap.get("time").toString())>1000*60*checkTime){
                            Connection conn=((Connection)tempConnMap.get("conn"));
                            System.out.println("----------------- kill tempBorrowConn:"+conn.toString());
                            conn.close();
                            tempBorrowCount.decrementAndGet();
                            tempConnList.remove(tempConnMap);
                        }
                    }
                    System.out.println("=================== ConnectionTempPool.tempBorrowCount:" + tempBorrowCount.get());
                    Thread.sleep(1000*10);
                }catch (Exception e)
                {
                    e.printStackTrace();
                }
            }
        }).start();
    }


    /**
     * 获取一个临时数据库连接
     * @param dataSource
     * @return
     */
    public Connection getTempConnection(DataSource dataSource) {
        Connection conn = null;
        try {
            if (tempBorrowCount.get() > 50)
                return null;
            else {
                Class.forName(poolProperties.getDriverClassName());
                conn = DriverManager.getConnection(poolProperties.getUrl(), poolProperties.getUsername(), poolProperties.getPassword());
                Map connMap = new HashMap();
                connMap.put("time", System.currentTimeMillis());
                connMap.put("conn", conn);
                tempConnList.add(connMap);
                tempBorrowCount.incrementAndGet();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 初始化连接池数据库配置信息
     * @param poolProperties
     */
    public void initPoolProperties(PoolProperties poolPropertiesTemp) {
        if (null == poolProperties)
            poolProperties = poolPropertiesTemp;
    }

    /**
     * 单例生成ConnectionTempPool
     * @return ConnectionTempPool
     */
    public static ConnectionTempPool getInstance(){
        return ReferenceClass.instance;
    }

    /**
     * 静态内部类
     */
    private static class ReferenceClass{
        private static ConnectionTempPool instance=new ConnectionTempPool();
    }
}
