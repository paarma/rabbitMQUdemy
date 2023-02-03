package org.rabbitmq.simple;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ProductorSimple {
    public static void main(String[] args) throws IOException, TimeoutException {
        String message = "Hola mundo";

        /**
         * Abrir conexión AMQ.
         * ConnectionFactory tiene los metodos set para setear el host, password, usuario, etc.
         * de rabbitMQ. En este caso no los asingamos y por ello utilizará los valores por defecto.
         *
         * Establecer un canal.
         * Channel aporta los métodos necesarios para declarar exchanges, colas, bindings,
         * enviar y consumir mensajes.
         *
         * Se usa try-with-resources ya que el canal y la conexión se deben cerrar
         * luego de enviar el mensaje.
         */
        ConnectionFactory connectionFactory = new ConnectionFactory();

        try(Connection connection = connectionFactory.newConnection();
            Channel channel = connection.createChannel()){

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
         * Enviar mensaje.
         *
         * @params:
         * exchange: En este caso al no especificar un nombre de exchange
         *      se enviaría al exchange por defecto.
         *
         * routing-key: En este caso se envía el nombre de la cola.
         *
         * props: En este caso se envia null.
         *
         * message.getBytes(): Cuerpo del mensaje en formato de array de bytes.
         */
        channel.basicPublish("", queueName, null, message.getBytes());

        }
    }
}