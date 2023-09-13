package rmq.client.receiver;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class RMQConsumer {

    private ConnectionFactory connectionFactory;
    private Connection connection;
    private String host;
    private Channel channel;
    private String queueName;
    private IRMQMessageReader irmqMessageReader;

    public RMQConsumer(String host, String queueName, IRMQMessageReader irmqMessageReader){
        this.host = host;
        this.queueName = queueName;
        this.irmqMessageReader = irmqMessageReader;
    }

    private void createChannel() throws IOException, TimeoutException {
        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost(host);
        connection = connectionFactory.newConnection();
        channel = connection.createChannel();
        channel.queueDeclare(queueName, true, false, false, null);
        System.out.println("Waiting for messages");
    }

    public void startListening() throws IOException, TimeoutException {
        createChannel();

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            irmqMessageReader.readMessage(message);
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
    }

    public void closeConnection() throws IOException, TimeoutException {
        if(channel.isOpen())
            channel.close();
        if(connection.isOpen())
            connection.close();
    }

}
