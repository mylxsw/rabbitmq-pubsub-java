package demo;

import com.rabbitmq.client.ConnectionFactory;
import cc.aicode.rabbitmq.Publisher;
import cc.aicode.rabbitmq.Subscriber;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Main {
    private static String EXCHANGE_NAME = "master";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("127.0.0.1");
        factory.setPort(5672);
        factory.setUsername("guest");
        factory.setPassword("guest");
        factory.setVirtualHost("/");

        // 发布消息，每次模拟发布5个消息到Exchange
        new Thread(() -> {
            try {
                Publisher publisher = new Publisher(factory.newConnection(), Main.EXCHANGE_NAME);
                for (int i = 0; i < 1; i++) {
                    try {
                        publisher.publish("{\"id\":121, \"name\":\"guanyiyao\"}", "user.create");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

//                for (int i = 0; i < 5; i++) {
//                    try {
//                        publisher.publish("{\"id\":121}", "user.delete");
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }).start();

        Thread.sleep(3000);

        // 订阅消息
        new Subscriber(factory.newConnection(), Main.EXCHANGE_NAME).init("user_monitor", "user.*").subscribe((message, routingKey) -> {
            // TODO 业务逻辑
            System.out.printf("    <%s> message consumed: %s\n", routingKey, message);
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            throw new Exception("出错了");
        });
    }
}
