package org.rabbitmq.publissuscribe.fanoutExchange;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * La funcion de este productor es enviar continuamente eventos (mensajes) a un
 * fanout-exchange.
 *
 * Con ello se notificarán todas las aplicaciones suscritas al servicio.
 */
public class ProductorEventos {

    //Nombre del exchange
    public static final String EVENTOS = "eventos";

    public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {

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
            Channel channel = connection.createChannel()) {

            /**
             * Crear fanout-exchange llamado "eventos"
             *
             * @params:
             *
             * exchange: Es el nombre del exchange en este caso "eventos",
             * BuiltinExchangeType.FANOUT = tipo del exchange.
             *
             * exchangeDeclare(): Es idenpotente es decir que se creará la priera vez que se llame,
             * las otros invocaciones a el no lo crearán nuevamente dado a que ya se creó la primera vez.
             */
            channel.exchangeDeclare(EVENTOS, BuiltinExchangeType.FANOUT);

            /**
             * Enviar mensajes al fanout-exchange llamado "eventos"
             * En este caso se enviará un mensaje cada segundo.
             *
             * exchange: Es el nombre del exchange en este caso "eventos".
             * routinKey: Es vacío ya que el fanout-exchange NO tiene en cuenta ese parámetro.
             * props: null.
             *
             */
            int count = 1;
            while (true) {
                String message = "Evento "+count;
                System.out.println("Produciendo mensaje: "+message);

                channel.basicPublish(EVENTOS, "", null, message.getBytes());

                Thread.sleep(1000);
                count++;
            }
        }

    }
}
