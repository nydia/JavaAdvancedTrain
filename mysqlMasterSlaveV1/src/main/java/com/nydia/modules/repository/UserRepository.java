package com.nydia.modules.repository;

import com.nydia.modules.entity.User;
import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface UserRepository extends CrudRepository<User, Long> {

    List<User> selectUser(User user);

}
