package org.kawanfw.sql.tomcat;

import org.apache.tomcat.jdbc.pool.DataSource;


/**
 * ClassName:org.kawanfw.sql.tomcat.PoolMonitor
 * Author:Chen.yiwan
 * Date:2020/6/21
 */
public class PoolMonitor implements Runnable {
    private DataSource tomcatDataSource;
    private long interval;

    public PoolMonitor(DataSource dataSource,long interval)
    {
        this.interval=interval;
        this.tomcatDataSource=dataSource;
    }

    @Override
    public void run() {
        while (true)
        {
            /***
             * 打印连接池信息
             */
            System.out.println("--------------------------- org.apache.tomcat.jdbc.pool Info ---------------------------");
            System.out.println("名称:"+tomcatDataSource.getPoolName());
            System.out.println("初始化连接数:"+tomcatDataSource.getInitialSize());
            System.out.println("最大空闲连接数:"+tomcatDataSource.getMaxIdle());
            System.out.println("最小空闲连接数:"+tomcatDataSource.getMinIdle());
            System.out.println("最大活跃连接数:"+tomcatDataSource.getMaxActive());
            System.out.println("正在使用的连接数:"+tomcatDataSource.getNumActive());
            System.out.println("从池中获取的连接次数:"+tomcatDataSource.getBorrowedCount());


            try{
                Thread.sleep(interval);
            }catch (Exception e)
            {
                e.printStackTrace();
            }
        }
    }



}
