package cc.aicode.rabbitmq;

import com.rabbitmq.client.Connection;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

abstract class PubSub {

    private String exchangeName;
    private Connection connection;

    /**
     * 构造方法
     *
     * @param connection   RabbitMQ连接
     * @param exchangeName Exchange Name
     */
    public PubSub(Connection connection, String exchangeName) {
        this.exchangeName = exchangeName;
        this.connection = connection;
    }

    /**
     * 创建RabbitMQ连接
     *
     * @return RabbitMQ连接
     */
    Connection getConnection() {
        return connection;
    }

    /**
     * Exchange name
     *
     * @return exchange name
     */
    String exchangeName() {
        return exchangeName;
    }

    /**
     * Record log
     *
     * @param message log message
     */
    void log(String message) {
        System.out.printf("[%s] [%s] %s\n", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME), getClass().getSimpleName(), message);
    }
}
