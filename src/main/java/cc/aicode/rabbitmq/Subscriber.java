package cc.aicode.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Subscriber extends PubSub {

    /**
     * 队列名称
     */
    private String queueName;

    /**
     * RabbitMQ Channel
     */
    private Channel channel;

    /**
     * 构造方法
     *
     * @param connection   RabbitMQ连接
     * @param exchangeName Exchange Name
     */
    public Subscriber(Connection connection, String exchangeName) {
        super(connection, exchangeName);
    }

    /**
     * 消息处理函数签名
     */
    public interface Handler {
        /**
         * 消息处理函数
         *
         * @param message    订阅到的消息内容
         * @param routingKey 路由KEY
         */
        void handle(String message, String routingKey);
    }

    /**
     * 消息订阅初始化
     *
     * @param queueName  订阅的队列名称
     * @param routingKey 订阅的路由规则
     * @throws IOException
     */
    public Subscriber init(String queueName, String routingKey) throws IOException {
        this.queueName = queueName;

        // 打开RabbitMQ连接，创建channel
        Connection connection = getConnection();
        channel = connection.createChannel();

        // 声明Exchange：主体，失败，重试
        channel.exchangeDeclare(exchangeName(), "topic", true);
        channel.exchangeDeclare(retryExchangeName(), "topic", true);
        channel.exchangeDeclare(failedExchangeName(), "topic", true);

        // 声明监听队列
        channel.queueDeclare(queueName, true, false, false, null);
        channel.queueDeclare(queueName + "@failed", true, false, false, null);

        Map<String, Object> arguments = new HashMap<String, Object>();
        arguments.put("x-dead-letter-exchange", exchangeName());
        arguments.put("x-message-ttl", 30 * 1000);
        channel.queueDeclare(queueName + "@retry", true, false, false, arguments);

        // 绑定监听队列到Exchange
        channel.queueBind(queueName, exchangeName(), routingKey);
        channel.queueBind(queueName + "@failed", failedExchangeName(), routingKey);
        channel.queueBind(queueName + "@retry", retryExchangeName(), routingKey);

        return this;
    }

    /**
     * 执行订阅处理
     *
     * @param handler 消息处理函数
     * @throws IOException
     */
    public void subscribe(Handler handler) throws IOException {
        log("Waiting for messages. To exit press CTRL+C");

        // 消息消费处理
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                                       AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    String message = new String(body, "UTF-8");
                    log("Received '" + message + "'");

                    // 消息处理函数
                    handler.handle(message, envelope.getRoutingKey());

                } catch (Exception e) {
                    long retryCount = getRetryCount(properties);
                    if (retryCount > 3) {
                        // 重试次数大于3次，则自动加入到失败队列
                        log("failed. send message to failed exchange");
                        channel.basicPublish(failedExchangeName(), envelope.getRoutingKey(), MessageProperties.PERSISTENT_BASIC, body);
                    } else {
                        // 重试次数小于3，则加入到重试队列，30s后再重试
                        log("exception. send message to retry exchange");
                        channel.basicPublish(retryExchangeName(), envelope.getRoutingKey(), properties, body);
                    }
                }

                // 注意，由于使用了basicConsume的autoAck特性，因此这里就不需要手动执行
                // channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };
        // 执行消息消费处理
        channel.basicConsume(queueName, true, consumer);
    }

    /**
     * 获取重试Exchange名称
     *
     * @return Exchange name
     */
    protected String retryExchangeName() {
        return exchangeName() + "@retry";
    }

    /**
     * 获取失败消息处理Exchange名称
     *
     * @return Exchange name
     */
    protected String failedExchangeName() {
        return exchangeName() + "@failed";
    }

    /**
     * 获取消息重试次数
     *
     * @param properties AMQP消息属性
     * @return 消息重试次数
     */
    protected Long getRetryCount(AMQP.BasicProperties properties) {
        Long retryCount = 0L;
        try {
            Map<String, Object> headers = properties.getHeaders();
            if (headers != null) {
                if (headers.containsKey("x-death")) {
                    List<Map<String, Object>> deaths = (List<Map<String, Object>>) headers.get("x-death");
                    if (deaths.size() > 0) {
                        Map<String, Object> death = deaths.get(0);
                        retryCount = (Long) death.get("count");
                    }
                }
            }
        } catch (Exception e) {
        }

        return retryCount;
    }
}