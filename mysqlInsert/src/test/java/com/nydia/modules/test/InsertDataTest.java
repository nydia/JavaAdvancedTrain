package com.nydia.modules.test;


import com.alibaba.druid.pool.DruidDataSource;
import com.nydia.modules.entity.master.User;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class InsertDataTest {

    @Autowired
    private com.nydia.modules.service.master.IUserService masterUserService;

    ThreadLocal<Long> runTime = new ThreadLocal<>();

    @Test
    public void insertData() {
        //insertByJdbcInStatement();
        insertByJdbcInStatementV2();
        //insertByJdbcInPreparedStatement();
    }

    //1. Spring框架+Druid+单线程
    public void insertDataByDruidInSingleThread() {
        runTime.set(new Date().getTime());
        for (int i = 0; i < 1000000; i++) {
            com.nydia.modules.entity.master.User userInsert = new com.nydia.modules.entity.master.User();
            userInsert.setUserName("王五");
            userInsert.setNickName("小五-" + i);
            userInsert.setPassword("123456");
            userInsert.setIdCard("34112");
            masterUserService.insertUser(userInsert);
        }
        System.out.println("执行时间1000000次插入的时间：" + (new Date().getTime() - runTime.get()) + "（毫秒）");
    }

    //2. Spring框架+Druid+100个线程
    public void insertDataByDruidIn100Thread() {
        runTime.set(new Date().getTime());
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        List<Future<Object>> futures = new ArrayList<>();
        for (int i = 0; i < 1000000; i++) {
            Future<Object> future = executorService.submit(new Callable<Object>() {
                public Object call(){
                    User user = User.builder().userName("王五").nickName("小五-").password("123456").idCard("2344556666").build();
                    masterUserService.insertUser(user);
                    return null;
                }
            });
            futures.add(future);
        }
        for (Future<Object> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        executorService.shutdown();
        System.out.println("执行时间1000000次插入的时间：" + (new Date().getTime() - runTime.get()) + "（毫秒）");
    }

    //3. Jdbc+Statement + (url带rewriteBatchedStatements=true)
    @SuppressWarnings("Duplicates")
    public void insertByJdbcInStatement(){
        final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        String DB_URL = "jdbc:mysql://localhost:3316/db?rewriteBatchedStatements=true&serverTimezone=Asia/Shanghai";
        //String DB_URL = "jdbc:mysql://localhost:3316/db?serverTimezone=Asia/Shanghai";
        final String USER = "root";
        final String PASS = "";
        Connection conn = null;
        Statement stmt = null;
        try{
            // 注册 JDBC 驱动
            Class.forName(JDBC_DRIVER);
            // 打开链接
            System.out.println("连接数据库...");
            //conn = DriverManager.getConnection(DB_URL,USER,PASS);
            conn = getDataSource().getConnection();//使用连接池
            conn.setAutoCommit(false);
            // 执行
            System.out.println(" 实例化Statement对象...");
            stmt = conn.createStatement();
            long startTime = new Date().getTime();
            for(int i = 1; i <= 1000000; i ++){
                String sql = "insert into `geek_user` ( `user_name`, `password`, `nick_name`, `id_card`) values('王五','123456','小五','2344556666')";
                stmt.addBatch(sql);
                if(i % 100000 == 0){
                    stmt.executeBatch();
                    conn.commit();//执行完后，手动提交事务
                    stmt.clearBatch();
                    System.out.println("执行到" + i);
                }
            }
            long endTime = new Date().getTime();
            System.out.println("执行时间：" + (endTime - startTime) + "(毫秒)");
            // 完成后关闭
            stmt.close();
            conn.setAutoCommit(true);
            conn.close();
        }catch(SQLException se){
            // 处理 JDBC 错误
            se.printStackTrace();
            try {
                // 若出现异常，对数据库中所有已完成的操作全部撤销，则回滚到事务开始状态
                if(!conn.isClosed()){
                    conn.rollback();
                    System.out.println("插入失败，回滚！");
                }
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }catch(Exception e){
            // 处理 Class.forName 错误
            e.printStackTrace();
        }finally{
            // 关闭资源
            try{
                if(stmt!=null) stmt.close();
                conn.setAutoCommit(true);
            }catch(SQLException se2){
            }
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        System.out.println("Goodbye!");
    }

    //3.1. Jdbc+Statement + (url带rewriteBatchedStatements=true) + Druid(10) + 10线程
    @SuppressWarnings("Duplicates")
    public void insertByJdbcInStatementV2(){
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        List<Future<Object>> futureList = new ArrayList<>();
        long startTime = new Date().getTime();
        for(int i = 1; i<= 10; i ++){
            Future<Object> future = executorService.submit(new Callable<Object>() {
                public Object call(){
                    Connection conn = null;
                    Statement stmt = null;
                    try{
                        conn = getDataSource().getConnection();//使用连接池
                        conn.setAutoCommit(false);
                        System.out.println("实例化Statement对象...");
                        stmt = conn.createStatement();
                        for(int i = 1; i <= 100000; i ++){
                            String sql = "insert into `geek_user` ( `user_name`, `password`, `nick_name`, `id_card`) values('王五','123456','小五','2344556666')";
                            stmt.addBatch(sql);
                            if(i % 10000 == 0){
                                stmt.executeBatch();
                                conn.commit();//执行完后，手动提交事务
                                stmt.clearBatch();
                            }
                        }
                        stmt.close();// 完成后关闭
                        conn.setAutoCommit(true);
                        conn.close();// 完成后关闭
                    }catch(SQLException se){
                        se.printStackTrace();// 处理 JDBC 错误
                    }catch(Exception e){
                        e.printStackTrace();// 处理 Class.forName 错误
                    }finally{
                        try{
                            if(stmt!=null) stmt.close();// 关闭资源
                            conn.setAutoCommit(true);
                            if(conn!=null) conn.close();// 关闭资源
                        }catch(SQLException se2){
                        }
                    }
                    System.out.println("Goodbye!");
                    return null;

                }
            });
            futureList.add(future);
        }
        for(Future<Object> f: futureList){
            try {
                f.get();
            }catch (InterruptedException e){
            }catch (ExecutionException e){}
        }
        long endTime = new Date().getTime();
        System.out.println("执行时间：" + (endTime - startTime) + "(毫秒)");
        executorService.shutdown();
    }

    //4. Jdbc+PreparedStatement(url + rewriteBatchedStatements=true)
    @SuppressWarnings("Duplicates")
    public void insertByJdbcInPreparedStatement(){
        final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        String DB_URL = "jdbc:mysql://localhost:3316/db?rewriteBatchedStatements=true&&serverTimezone=Asia/Shanghai";
        final String USER = "root";
        final String PASS = "";
        Connection conn = null;
        PreparedStatement pstm = null;
        try{
            Class.forName(JDBC_DRIVER);// 注册 JDBC 驱动
            System.out.println("连接数据库...");
            conn = DriverManager.getConnection(DB_URL,USER,PASS);
            pstm = conn.prepareStatement("insert into `geek_user` ( `user_name`, `password`, `nick_name`, `id_card`) values('王五','123456','小五','2344556666')");
            conn.setAutoCommit(false);//把Auto commit设置为false,不让它自动提交
            long startTime = new Date().getTime();
            for(int i = 1; i <= 1000000; i ++){
                // 将一组参数添加到此 PreparedStatement 对象的批处理命令中。
                pstm.addBatch();
                if(i % 100000 == 0){
                    pstm.executeBatch();
                    System.out.println("执行到" + i);
                }
            }
            conn.commit();//执行完后，手动提交事务
            long endTime = new Date().getTime();
            System.out.println("执行时间：" + (endTime - startTime) + "(毫秒)");
            conn.setAutoCommit(true);//再把自动提交打开，避免影响其他需要自动提交的操作
            conn.close();
        }catch(SQLException se){
            // 处理 JDBC 错误
            se.printStackTrace();
            try {
                if(!conn.isClosed()){
                    conn.rollback();
                    System.out.println("插入失败，回滚！");
                }
            } catch (SQLException e1) {
                e1.printStackTrace();
            }
        }catch(Exception e){
            // 处理 Class.forName 错误
            e.printStackTrace();
        }finally{
            // 关闭资源
            try{
                if(pstm!=null) pstm.close();
                conn.setAutoCommit(true);
            }catch(SQLException se2){
            }
            try{
                if(conn!=null) conn.close();
            }catch(SQLException se){
                se.printStackTrace();
            }
        }
        System.out.println("Goodbye!");
    }

    // ========== 连接池 ==============
    public DataSource getDataSource(){
        DruidDataSource dataSource = new DruidDataSource();
        dataSource.setUrl("jdbc:mysql://localhost:3316/db?rewriteBatchedStatements=true&&serverTimezone=Asia/Shanghai");
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUsername("root");
        dataSource.setPassword("");
        dataSource.setInitialSize(10);
        dataSource.setMinIdle(5);
        dataSource.setMaxWait(60000);
        dataSource.setMaxActive(10);
        return dataSource;
    }


}
