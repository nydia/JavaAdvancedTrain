package com.nydia.ruleengine.groovy.groovy_shell;

import org.springframework.stereotype.Service;

@Service
@GroovyFunction
public class TestService {
    public String testQuery(long id) {
        return "Test query success, id is " + id;
    }
}