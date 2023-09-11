package com.nydia.mybatis.mapper;

import com.nydia.mybatis.entity.User;
import com.nydia.mybatis.entity.UserAlias;
import org.apache.ibatis.annotations.Flush;
import org.apache.ibatis.annotations.Lang;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.cursor.Cursor;
import org.apache.ibatis.executor.BatchResult;
import org.apache.ibatis.scripting.xmltags.XMLLanguageDriver;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
@Mapper
public interface UserMapper {

    int insert(User user);

    int insert2(User user);

    int insert3(User user);

    int update(User user);

    User selectById(Integer id);

    List<User> selectByName(String username);

    Map selectByIdForMap(Integer id);

    User lasyLoadQuery(Integer id);

    User selectByIdForResultSet(Integer id);

    List<User> selectByNameForResultSet(String username);

    UserAlias selectByIdForAlias(Integer id);

    Cursor<User> selectByNameForCursor(String username);

    List<User> selectByNameForMap(String username);

    User selectByIdForAutoMapping(Integer id);

    @Flush
    List<BatchResult> flush();

   // @Lang(XMLLanguageDriver.class)
    @Select("<script>select id,username,password from tbl_user where id = #{id}</script>")
    User selectByIdForLang(Integer id);

}
