package com.nydia.ruleengine.groovy.groovy_shell;

import groovy.lang.Binding;
import groovy.lang.GroovyShell;
import groovy.lang.Script;
import org.codehaus.groovy.control.CompilerConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Hashtable;
import java.util.Map;


@RestController
@RequestMapping("/groovy/single/script")
public class SingleScriptController {

    private static final Object lock = new Object();
    private static final GroovyShell groovyShell;
    private static Hashtable<String, Script> scriptCache = new Hashtable<>();

    @Autowired
    private Binding groovyBinding; // 默认绑定已有方法的实例

    static {
        CompilerConfiguration cfg = new CompilerConfiguration();
        groovyShell = new GroovyShell(cfg);
    }

    /**
     * 在客户端本地只实例化单例RuleExecutor
     * 然后多个线程同时操作scriptCache，需要保证线程安全。
     *
     * @param expression
     * @return
     */
    private Script getScriptFromCache(String expression) {
        if (scriptCache.containsKey(expression)) {
            return scriptCache.get(expression);
        }
        synchronized (lock) {
            if (scriptCache.containsKey(expression)) {
                return scriptCache.get(expression);
            }
            Script script = groovyShell.parse(expression);
            scriptCache.put(expression, script);
            return script;
        }
    }

    public Object ruleParse(String expression) {
        Script script = getScriptFromCache(expression);
        script.setBinding(groovyBinding);
        return script.run();
    }

    public Object ruleParse(String expression, Map<String, Object> paramMap) {
        Binding binding = groovyBinding;
        paramMap.forEach((key,value)->binding.setProperty(key,value));
        Script script = getScriptFromCache(expression);
        script.setBinding(binding);
        return script.run();
    }

    @RequestMapping(value = "/execute", method = RequestMethod.POST)
    public @ResponseBody
    Object ruleExecutor(@RequestBody ScriptRequest request) {
        if (request.getParamMap() == null) {
            return ruleParse(request.getExpression());
        } else {
            return ruleParse(request.getExpression(), request.getParamMap());
        }
    }
}  
