package br.com.darp.rabbitmq;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.MessageProperties;


import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class Send {
    private final static String QUEUE_NAME = "logs";
    private final static String QUEUE_ERROR = "logs-error";
    private final static String EXCHANGE_NAME = "logs-exchange";

    public static void main(String[] argv) throws Exception{

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setUsername("user");
        connectionFactory.setPassword("pass");

        Map<String, Object> configs = new HashMap<>();
        configs.put("x-dead-letter-exchange","error-dlx");
        configs.put("x-dead-letter-routing-key","erro");

        boolean durable = true;

        try(Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel()){
            channel.exchangeDeclare(EXCHANGE_NAME,"direct",true,false,null);
            channel.exchangeDeclare("error-dlx","direct",true,false,null);
            channel.queueDeclare(QUEUE_ERROR,durable,false,false,null);
            channel.queueDeclare(QUEUE_NAME, durable, false, false,configs);

            String mensagem = "....";

            channel.basicPublish(EXCHANGE_NAME,"build", MessageProperties.PERSISTENT_TEXT_PLAIN,
                    mensagem.getBytes(StandardCharsets.UTF_8));
            channel.basicPublish(EXCHANGE_NAME,"build", MessageProperties.PERSISTENT_TEXT_PLAIN,
                    mensagem.getBytes(StandardCharsets.UTF_8));
            channel.basicPublish(EXCHANGE_NAME,"build", MessageProperties.PERSISTENT_TEXT_PLAIN,
                    mensagem.getBytes(StandardCharsets.UTF_8));

            System.out.println("[x] Enviando 3X '" + mensagem + "'");
        }
    }
}
