package rmq.client.sender;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

public class RMQSender {

    public void sendMessage(String host, String name, String message) throws IOException, TimeoutException {
        ConnectionFactory connectionFactory;
        Connection connection = null;
        Channel channel = null;
        try{
            connectionFactory = new ConnectionFactory();
            connectionFactory.setHost(host);
            connection = connectionFactory.newConnection();
            channel = connection.createChannel();
            channel.queueDeclare(name, true, false, false, null);

            channel.basicPublish("", name, null, message.getBytes(StandardCharsets.UTF_8));
        }catch (Exception e){
            System.out.println("Exception : " + e.getMessage());
        }finally {
            if(channel.isOpen())
                channel.close();
            if(connection.isOpen())
                connection.close();
        }
    }

}
