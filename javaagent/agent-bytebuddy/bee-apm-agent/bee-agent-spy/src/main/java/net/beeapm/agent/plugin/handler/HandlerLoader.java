package net.beeapm.agent.plugin.handler;

import net.beeapm.agent.common.AgentClassLoader;
import net.beeapm.agent.log.LogUtil;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 插件handler加载
 *
 * @author agent
 * @date 2022/7/31
 */
public class HandlerLoader {
    private static ConcurrentHashMap<String, IHandler> handlerMap = new ConcurrentHashMap<String, IHandler>();
    private static ReentrantLock INSTANCE_LOAD_LOCK = new ReentrantLock();
    private static final EmptyHandler EMPTY_HANDLER = new EmptyHandler();
    private static String rootPath;

    public static void init(String path) {
        rootPath = path;
    }


    public static IHandler load(String className) {
        try {
            ClassLoader targetClassLoader = Thread.currentThread().getContextClassLoader();
            String instanceKey = className + "_OF_" + targetClassLoader.getClass().getName() + "@" + Integer.toHexString(targetClassLoader.hashCode());
            IHandler inst = handlerMap.get(instanceKey);
            if (inst == null) {
                INSTANCE_LOAD_LOCK.lock();
                ClassLoader classLoader = new AgentClassLoader(targetClassLoader, rootPath, new String[]{"plugins"});
                try {
                    inst = (IHandler) Class.forName(className, true, classLoader).newInstance();
                } finally {
                    INSTANCE_LOAD_LOCK.unlock();
                }
                if (inst != null) {
                    handlerMap.put(instanceKey, inst);
                }
            }
            return inst;
        } catch (IllegalAccessException e) {
            LogUtil.log("plugin handler load error-IllegalAccessException", e);
        } catch (InstantiationException e) {
            LogUtil.log("plugin handler load error-InstantiationException", e);
        } catch (ClassNotFoundException e) {
            LogUtil.log("plugin handler load error-ClassNotFoundException", e);
        } catch (Throwable t) {
            LogUtil.log("plugin handler load error-Throwable", t);
        }
        return EMPTY_HANDLER;
    }
}
