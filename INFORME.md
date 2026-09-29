# Informe

## Coordinación de las instancias de Sum

Las distintas instancias de Sum consumen los mensajes enviados por el Gateway desde una única working queue compartida. De esta manera, Rabbitmq distribuye los mensajes entre las réplicas, los datos de un mismo cliente pueden ser procesados por distintas instancias de Sum por esta competencia.

Los mensajes recibidos pueden ser de tipo DATA o EOF. Los mensajes DATA contienen los registros enviados por el cliente y son acumulados localmente por cada instancia de Sum, agrupando por clientID y fruta.

Además de la working queue, las instancias de Sum se encuentran organizadas en un ring de control.

El mensaje EOF indica que el Gateway terminó de enviar los datos de un cliente. Sin embargo, recibir este mensaje no implica que todas las instancias de Sum hayan terminado de procesar los mensajes DATA, ya que algunos pueden encontrarse todavía en la cola o siendo procesados por otras réplicas.
Para resolver esta situación, el Gateway incluye en el EOF la cantidad total de mensajes DATA enviados para ese cliente. A partir del clientID se determina de forma determinística una instancia líder de Sum. Si el EOF es recibido por otra réplica, se propaga un mensaje BARRIER_INIT hasta llegar al líder.

El líder inicia entonces una ronda de mensajes COUNT entre las instancias de Sum. Cada réplica agrega al mensaje la cantidad de mensajes DATA que procesó para ese cliente. Cuando el COUNT vuelve al líder, éste compara la cantidad observada con la cantidad total informada por el Gateway.

Si todavía faltan mensajes por procesar, se inicia una nueva ronda luego de una espera. Cuando ambas cantidades coinciden, se considera finalizada esta etapa y comienza la propagación del mensaje FINISH.

Durante FINISH, cada instancia de Sum prepara sus resultados parciales y los envía a las instancias de Aggregation correspondientes. De esta forma, la finalización de un cliente no depende de qué réplica haya recibido el EOF, y se garantiza que todos los datos hayan sido procesados antes de avanzar a la siguiente etapa.
## Coordinación entre Sum y Aggregation

Una vez finalizada la coordinación entre las instancias de Sum, cada una de ellas debe enviar sus resultados parciales hacia las instancias de Aggregation.
Para distribuir la información se utiliza un criterio determinístico de particionado basado en la combinación entre el clientID y la fruta. De esta forma, todos los aportes correspondientes a una misma fruta de un mismo cliente son enviados siempre a la misma instancia de Aggregation.
Este particionado permite evitar el broadcast completo de datos. En lugar de enviar todos los resultados de cada Sum a todos los Aggregators, cada registro agregado se dirige únicamente al Aggregator responsable de ese shard.

Cada instancia de Sum envía una contribución PARTIAL a cada Aggregator una vez finalizada la etapa de recolección de información. Los Aggregators pueden comenzar a procesar estas contribuciones a medida que llegan, pero para determinar que su partición está completa necesitan saber que recibieron la contribución de todas las instancias de Sum. Por este motivo, cuando un Sum no posee registros para un determinado shard envía igualmente un PARTIAL vacío. De esta forma, la recepción de un PARTIAL por cada Sum funciona también como mecanismo de finalización de la partición.

Cada Aggregator mantiene estado separado por cliente y espera recibir un PARTIAL de cada instancia de Sum. A medida que llegan los mensajes, consolida los registros correspondientes a una misma fruta. Una vez recibidas todas las contribuciones, considera completa su partición para ese cliente y puede avanzar al cálculo de su top parcial.

Además, las colas utilizadas entre Sum y Aggregation son estables, por lo que un Sum puede publicar un PARTIAL aunque el Aggregator todavía no haya comenzado a consumir. El mensaje permanece en RabbitMQ hasta que la instancia correspondiente esté disponible.

## Coordinación entre Aggregation y Join

## Escalabilidad

### Múltiples clientes
### Grandes volúmenes de datos
### Cantidad de controles

## Finalización de los controles