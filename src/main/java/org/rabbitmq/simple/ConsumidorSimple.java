package org.rabbitmq.simple;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.concurrent.TimeoutException;

/**
 * Al ejecutar el consumidor. No usamos el try-with-resources para cerrar las conexiónes y canal ya que
 * el consumidor debe estar escuchando constantemente las colas para procesar los mensajes que lleguen.
 *
 * Por ello al ejecutar esta clase se mantendrá en ejeción y solo terminará hasta que se cierre la conexión
 * y el canal
 */
public class ConsumidorSimple {
    public static void main(String[] args) throws IOException, TimeoutException {

        //Abrir conexion AMQ
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();

        //Establecer canal
        Channel channel = connection.createChannel();

        /**
         * Crear o declarar la cola.
         * Se vinculará automaticamente al exchange sin nombre mediante un binding-key
         * igual al nombre de la cola.
         *
         * queueDeclare() Es idenpotente es decir que se creará la priera vez que se llame,
         * las otros invocaciones a ella no la crearán nuevamente dado a que ya se creó la primera vez.
         */
        String queueName = "primera-cola";
        channel.queueDeclare(queueName, false, false, false, null);

        /**
         * Crear la subscripción a la cola.
         *
         * @params:
         * queue: Nombre de la cola.
         *
         * autoAck: boolean que indica si se debe enviar una confirmación de mensaje recibido a rabbitMQ.
         *      true: El cliente enviará la confirmación de recibo al momento de recibir el mensaje.
         *      Rabbit lo marcará como recibido y lo eliminará de la cola.
         *
         *      false: Se debe enviar la confirmación de recibido de manera explícita (manual). En caso de no
         *      enviarlo manualmente entonces rabbit reenviará el mensje a otro consumidor suscrito a la cola.
         *
         * deliverCallback: Interfaz funcional que invoca al método donde se procesará el mensaje.
         *      En este caso se pasa como argumento una lambda con "consumerTag" que es el identificador
         *      del consumidor y el mensaje.
         *
         * cancelCallback: Interfaz funcional que tiene un método el cual es llamado si el consumidor es
         *      cancelado de manera implícita. Ejemplo si la cola a la que está inscrito es eliminada.
         *      En este caso se pasa como argumento una lambda con el "consumerTag" (identificador del consumidor)
         */
        channel.basicConsume(queueName,
                true,
                (consumerTag, message) -> {
                    String messageBody = new String(message.getBody(), Charset.defaultCharset());

                    System.out.println("Mensaje: "+messageBody);
                    System.out.println("Exchange: "+message.getEnvelope().getExchange());
                    System.out.println("Routing key: "+message.getEnvelope().getRoutingKey());
                    System.out.println("Delivery tag (identificador del mensaje): "+message.getEnvelope().getDeliveryTag());
                },
                consumerTag -> {
                    System.out.println("Consumidor "+consumerTag+" cancelado");
                });

    }
}
