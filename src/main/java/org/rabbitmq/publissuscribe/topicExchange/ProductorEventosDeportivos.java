package org.rabbitmq.publissuscribe.topicExchange;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Pulicará eventos (mensajes) los cuales estarán clasificados por
 * país, deporte, tipoEvento (envivo o noticia)
 */
public class ProductorEventosDeportivos {

    public static final String EXCHANGE = "eventos-deportivos";

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
             * Crear topic-exchange llamado "eventos-deportivos"
             *
             * @params:
             *
             * exchange: Es el nombre del exchange en este caso "eventos-deportivos",
             * BuiltinExchangeType.TOPIC = tipo del exchange.
             *
             * exchangeDeclare(): Es idenpotente es decir que se creará la priera vez que se llame,
             * las otros invocaciones a el no lo crearán nuevamente dado a que ya se creó la primera vez.
             */
            channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.TOPIC);

            //Clasificaciones de los mensajes:
            //Pais
            List<String> countries = Arrays.asList("Inglaterra", "USA", "Rusia");
            //Deporte
            List<String> sports = Arrays.asList("futbol", "tenis", "voleibol");
            //tipo evento
            List<String> eventTypes = Arrays.asList("envivo", "noticia");

            /**
             * Enviar mensajes al topic-exchange llamado "eventos-deportivos"
             * En este caso se enviará un mensaje cada segundo.
             *
             * exchange: Es el nombre del exchange en este caso "eventos-deportivos".
             * routingKey: Utilizado para que el exchenge sepa a que colas enviar el mensaje.
             * props: null.
             *
             */
            int count = 1;
            while (true) {

                //Se ordenan las listas de manera aleatorea para obtener siempre valores diferentes
                ordenarListasAleatoreamente(countries, sports, eventTypes);

                //Obtenemos los items de las listas
                String country = countries.get(0);
                String sport = sports.get(0);
                String eventType = eventTypes.get(0);

                //RoutingKey. El cual estará formado por el pais.deporte.tipoEvento
                String routingKey = country + "." + sport + "." + eventType;

                String message = "Evento "+count;
                System.out.println("Produciendo mensaje: ("
                        +country+", "+sport+", "+eventType+") "+message);

                channel.basicPublish(EXCHANGE, routingKey, null, message.getBytes());

                Thread.sleep(1000);
                count++;
            }
        }
    }

    //Se ordenan las listas de manera aleatorea
    private static void ordenarListasAleatoreamente(List<String> countries,
                                                    List<String> sports,
                                                    List<String> eventTypes){
        Collections.shuffle(countries);
        Collections.shuffle(sports);
        Collections.shuffle(eventTypes);
    }
}
