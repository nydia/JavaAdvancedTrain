package net.beeapm.agent.plugin.jdbc.handler;

import net.beeapm.agent.common.BeeUtils;
import net.beeapm.agent.common.SamplingUtil;
import net.beeapm.agent.log.ILog;
import net.beeapm.agent.log.LogFactory;
import net.beeapm.agent.model.Span;
import net.beeapm.agent.plugin.handler.AbstractHandler;
import net.beeapm.agent.plugin.jdbc.JdbcConfig;
import net.beeapm.agent.plugin.jdbc.common.JdbcContext;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.LinkedHashMap;

public class PreparedStatementParamHandler extends AbstractHandler {
    //private static final ILog log = LogFactory.getLog(ConnectionHandler.class.getSimpleName());


    @Override
    public Span before(String className, String methodName, Object[] allArguments,Object[] extVal) {
        if(!JdbcConfig.me().isEnable() || SamplingUtil.NO()){
            return null;
        }
        Span span = JdbcContext.getJdbcSpan();
        // 参数处理
        if(span != null && allArguments.length > 1 && JdbcConfig.me().isEnableParam()) {
            LinkedHashMap<String,Object> params = (LinkedHashMap<String,Object>)span.getTag("params");
            if(params == null){
                params = new LinkedHashMap<String,Object>();
                span.addTag("params",params);
            }
            String indexKey = allArguments[0] + "";
            if(!params.containsKey(indexKey)){
                Object val = allArguments[1];
                if(isExcludeType(val)){
                    params.put(indexKey,"{!}");
                }else{
                    params.put(indexKey,val);
                }
            }
        }
        return span;
    }

    @Override
    public Object after(String className, String methodName, Object[] allArguments, Object result, Throwable t,Object[] extVal) {
        return result;
    }

    public boolean isExcludeType(Object val){
        if(BeeUtils.isPrimitive(val.getClass())
                || val instanceof java.sql.Date
                || val instanceof Date
                || val instanceof Time
                || val instanceof Timestamp){
            return false;
        }
        return true;
    }

}
