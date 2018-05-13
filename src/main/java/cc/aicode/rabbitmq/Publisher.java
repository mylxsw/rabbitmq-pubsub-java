package cc.aicode.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Publisher extends PubSub {

    /**
     * 构造方法
     *
     * @param connection   RabbitMQ连接
     * @param exchangeName Exchange Name
     */
    public Publisher(Connection connection, String exchangeName) {
        super(connection, exchangeName);
    }

    public void publish(String message, String routingKey) throws IOException, TimeoutException {

        try (Channel channel = getConnection().createChannel()) {
            channel.exchangeDeclare(exchangeName(), "topic", true);
            channel.basicPublish(exchangeName(), routingKey, MessageProperties.PERSISTENT_BASIC, message.getBytes());

            log("message published.");
        }
    }

}
