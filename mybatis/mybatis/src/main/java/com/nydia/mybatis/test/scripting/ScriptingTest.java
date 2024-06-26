package com.nydia.mybatis.test.scripting;

import com.nydia.mybatis.entity.User;
import com.nydia.mybatis.mapper.UserMapper;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

/**
 * @Auther: nydia_lhq@hotmail.com
 * @Date: 2023/8/20 23:33
 * @Description: Scripting Test
 */
public class ScriptingTest {

    public SqlSessionFactory getSqlSessionFactory() throws IOException {
        //注意此处路径不要写错
        String resource = "config/mybatis-config.xml";
        InputStream inputStream = Resources.getResourceAsStream(resource);
        return new SqlSessionFactoryBuilder().build(inputStream);
    }

    @Test
    public void update() throws IOException {
        //1、获取SqlSessionFactory实例
        SqlSessionFactory sqlSessionFactory = getSqlSessionFactory();
        //2、打开一个会话
        SqlSession openSession = sqlSessionFactory.openSession(ExecutorType.SIMPLE);
        try {
            // 3、获取接口的实现类对象，会为接口自动的创建一个代理对象，代理对象去执行增删改查方法
            UserMapper mapper = openSession.getMapper(UserMapper.class);
            int result = mapper.updateForScripting(User.builder()
                    .id(318).username("name-123")
                    .password("abc123456")
                    .build());
            openSession.commit();
            System.out.println(result);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //4、使用完毕后关闭会话
            openSession.close();
        }
    }

}
