package org.rabbitmq.publissuscribe.topicExchange;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class ConsumidorEventosDeportivos {

    public static final String EXCHANGE = "eventos-deportivos";

    public static void main(String[] args) throws IOException, TimeoutException {
        //Abrir conexion
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();

        //Establecer canal
        Channel channel = connection.createChannel();

        /**
         * Declarar exchange "eventos-deportivos"
         * exchangeDeclare(): Es idenpotente es decir que se creará la priera vez que se llame,
         * las otros invocaciones a el no lo crearán nuevamente dado a que ya se creó la primera vez.
         */
        channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.TOPIC);

        /**
         * Creamos una cola que asociamos al exchange "eventos-deportivos".
         *
         * queueDeclare(). Creará una cola de nombre aleatoreo generada por el broker exclusiva
         *  para garantizar que solo puede haber un consumidor conectado a ella.
         *
         *  Esta cola es autodestruible para que sea eliminada por el broker una vez se desconecte el consumidor.
         *  No durable para que la declaración no persista en caso de reiniciar el broker.
         */

        //Obtenemos el nombre de la cola.
        String queueName = channel.queueDeclare().getQueue();

        //Patron del RoutingKey. pais.deporte.tipoEvento
        /**
         * Caracteres especiales reconocidos por el topic-exchange
         * "*" identifica una palabra.
         * "#" identifica múltiples palabras delimitadas por puntos.
         *
         * Ejemplo:
         * Para recibir todos los eventos del deporte "fultbol" independiente del país y tipo de evento:
         * El consumidor deberá suscribirse a una cola que esté asociada al exchange mediante un
         * routingKey que esté formado por la siguiente expresión regular:
         *      "*.futbol.*"
         *
         * Ejemplo recibir todos los eventos del país "Inglaterra" independiente del deporte o tipoEvento:
         *      "Inglaterra.#" o también podría ser "Inglaterra.*.*"
         *
         *  Ejemplo recibir todos los eventos:
         *      "#"
         */

        /**
         * Para este ejemplo, la variable routingKey la ingresaremos por teclado:
         *
         * Ejecutamos 2 consumidores
         * Uno para recibir todos los eventos de futbol ingresando el routing-key "*.futbol.*"
         * Otro consumidor para recibir todos los eventos en vivo ingresando el routing-key  "#.envivo"
         */


        System.out.println("Ingrese routing-key: ");
        Scanner scanner = new Scanner(System.in);
        String routingKey = scanner.nextLine();

        //Asociamos la cola al exchange.
        channel.queueBind(queueName, EXCHANGE, routingKey);

        //Crear subscripcion a una cola asociada al exchange "eventos-deportivos"
        channel.basicConsume(queueName,
                true,
                (consumerTag, message) -> {
                    String messageBody = new String(message.getBody(), Charset.defaultCharset());

                    System.out.println("Mensaje recibido: "+messageBody);
                    System.out.println("RoutingKey: "+message.getEnvelope().getRoutingKey());
                },
                consumerTag -> {
                    System.out.println("Consumidor "+consumerTag+" cancelado");
                });
    }
}
