package io.kimmking.javacourse.mq.activemq;

import org.apache.activemq.broker.BrokerService;

public class ActiveMQServer {

    public static void main(String[] args) throws Exception{
        // 尝试用java代码启动一个ActiveMQ broker server
        // 然后用前面的测试demo代码，连接这个嵌入式的server

        BrokerService brokerService = new BrokerService();
        brokerService.addConnector("tcp://localhost:61616");
        brokerService.setDataDirectory("C:\\temp");
        brokerService.setUseShutdownHook(true);
        brokerService.start();

        //Thread.sleep(10000000);

        // 关闭
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    brokerService.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                System.exit(0);
            }
        });
    }


}
