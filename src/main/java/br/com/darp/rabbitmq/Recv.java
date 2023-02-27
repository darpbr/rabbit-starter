package br.com.darp.rabbitmq;
import br.com.darp.exceptions.DomainException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import io.quarkus.runtime.Startup;

import javax.enterprise.context.ApplicationScoped;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
@Startup
@ApplicationScoped
public class Recv {
    private final static String QUEUE_NAME = "logs";
    private final static String QUEUE_ERROR = "logs-error";
    private final static String EXCHANGE_NAME = "logs-exchange";

    public Recv() throws Exception{

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        factory.setUsername("user");
        factory.setPassword("pass");
        factory.setAutomaticRecoveryEnabled(true);
        factory.setHandshakeTimeout(6000);
        factory.setConnectionTimeout(6000);
        factory.setRequestedChannelMax(5);

        Map<String, Object> configs = new HashMap<>();
        configs.put("x-dead-letter-exchange","error-dlx");
        configs.put("x-dead-letter-routing-key","erro");

        boolean durable = true;
        int prefetch = 5;
        boolean autoAck = false;

        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME,"direct",true,false,null);
        channel.exchangeDeclare("error-dlx","direct",true,false,null);
        channel.basicQos(prefetch);
        channel.queueDeclare(QUEUE_NAME,durable,false,false,configs);
        channel.queueDeclare(QUEUE_ERROR,durable,false,false,null);
        channel.queueBind(QUEUE_NAME,EXCHANGE_NAME,"build");
        channel.queueBind(QUEUE_ERROR,"error-dlx","erro");

        System.out.println(" [*] Esperando mensagens. Para sair, encerre a aplicação.");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String mensagem = new String(delivery.getBody(), StandardCharsets.UTF_8);
            System.out.println(" [X] Recebida mensagem '" + mensagem + "'");
            try{
                doWork(mensagem);
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (DomainException dExc){
                System.out.println("Error: " + dExc.getMessage());
                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                channel.basicReject(delivery.getEnvelope().getDeliveryTag(), false);
            }
                finally{
                System.out.println("[X] Done");
            }
        };
        channel.basicConsume(QUEUE_NAME,autoAck,deliverCallback,consumerTag -> { });
    }

    private static void doWork(String task) throws InterruptedException {
        int count = 0;
        for (char ch: task.toCharArray() ){
            if(count >= 4){
                throw new DomainException("Tempo limite esgotado!");
            }
            if(ch == '.')
            {
                System.out.println(count + " Espere 10s por favor.");
                Thread.sleep(10000);
            }
            count++;
        }
    }
}
