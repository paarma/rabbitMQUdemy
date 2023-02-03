package org.rabbitmq.publissuscribe.fanoutExchange;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeoutException;

/**
 * Para este ejecicio cada aplicación (consumidor) recibirá notificaciones en vivo de todos
 * los eventos que ocurran. Por ello cada consumidor necesitará su propia cola.
 *
 * Recordar que rabbitMQ elimina el mensaje de la cola una vez que es consumido. Si se tienen varios
 * consumidores, los mensajes serán distribuidos entre ellos. Entonces para garantizar que TODOS los mensajes
 * sean recibidos por TODOS los consumidores se debe asegurar que cada consumidor consuma exclusivamente de
 * una cola (una cola para cada consumidor)
 */
public class ConsumidorEventos {

    //Nombre del exchange
    public static final String EVENTOS = "eventos";

    public static void main(String[] args) throws IOException, TimeoutException {
        //Abrir conexion
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();

        //Establecer canal
        Channel channel = connection.createChannel();

        /**
         * Declarar exchange "eventos"
         * exchangeDeclare(): Es idenpotente es decir que se creará la priera vez que se llame,
         * las otros invocaciones a el no lo crearán nuevamente dado a que ya se creó la primera vez.
         */
        channel.exchangeDeclare(EVENTOS, BuiltinExchangeType.FANOUT);

        /**
         * Creamos una cola que asociamos al exchange "eventos".
         *
         * queueDeclare(). Creará una cola de nombre aleatoreo generada por el broker exclusiva
         *  para garantizar que solo puede haber un consumidor conectado a ella.
         *
         *  Esta cola es autodestruible para que sea eliminada por el broker una vez se desconecte el consumidor.
         *  No durable para que la declaración no persista en caso de reiniciar el broker.
         */

        //Obtenermos el nombre de la cola.
        String queueName = channel.queueDeclare().getQueue();

        //Asociamos la cola al exchange.
        channel.queueBind(queueName, EVENTOS, "");

        //Crear subscripcion a una cola asociada al exchange "eventos"
        channel.basicConsume(queueName,
                true,
                (consumerTag, message) -> {
                    String messageBody = new String(message.getBody(), Charset.defaultCharset());

                    System.out.println("Mensaje recibido: "+messageBody);
                },
                consumerTag -> {
                    System.out.println("Consumidor "+consumerTag+" cancelado");
                });
    }
}
