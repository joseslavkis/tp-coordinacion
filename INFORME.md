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

Una vez que una instancia de Aggregation recibió un PARTIAL de cada instancia de Sum para un cliente, considera completa su partición. En ese momento consolida los registros recibidos y calcula un top parcial de tamaño máximo TOP_SIZE.

Cada Aggregator envía entonces un mensaje TOP_PARTIAL hacia una cola compartida consumida por Join. El mensaje incluye el clientID, el identificador de la instancia de Aggregation que produjo el resultado y los elementos pertenecientes a su top parcial.
El envío del top parcial es suficiente para calcular posteriormente el top global. Como los datos fueron particionados previamente de forma determinística entre los Aggregators, cada fruta de un cliente pertenece a un único shard. Además, cualquier elemento que pueda formar parte del top global debe encontrarse necesariamente dentro del top de tamaño TOP_SIZE de su propia partición. Por este motivo no es necesario que los Aggregators envíen todos los elementos consolidados al Join.

Join mantiene estado independiente para cada cliente. A medida que recibe mensajes TOP_PARTIAL, acumula los candidatos y registra qué instancias de Aggregation ya enviaron su contribución. El aggregationID permite además detectar mensajes duplicados y evitar procesar dos veces la misma contribución.

Cuando Join recibió un TOP_PARTIAL de todas las instancias de Aggregation, considera finalizada la etapa para ese cliente. En ese momento ordena el conjunto de candidatos utilizando la función de comparación provista por FruitItem, selecciona los primeros TOP_SIZE elementos y genera el resultado final.

Finalmente, Join envía un mensaje RESULT hacia la cola de salida consumida por el Gateway, que es responsable de devolver el resultado al cliente correspondiente.
Incluso un Aggregator que no posea elementos para su partición envía un TOP_PARTIAL vacío. De esta forma, la recepción de un mensaje proveniente de cada instancia de Aggregation funciona también como mecanismo de finalización, sin necesidad de introducir un mensaje EOF adicional entre Aggregation y Join.

## Escalabilidad

### Múltiples clientes

El sistema permite procesar múltiples clientes de manera concurrente manteniendo separado el estado correspondiente a cada uno de ellos. Para esto, todos los mensajes internos incluyen un clientID, que identifica de forma unívoca el flujo al que pertenecen.

Las instancias de Sum mantienen de forma independiente los datos acumulados, la cantidad de mensajes procesados y el estado de coordinación de cada cliente. De la misma forma, las instancias de Aggregation y Join mantienen su estado separado por clientID.

Esto permite que mensajes pertenecientes a distintos clientes se encuentren intercalados en las mismas colas sin que sus resultados se mezclen. Además, cada cliente realiza de manera independiente su propia coordinación de finalización, desde la detección del fin de los mensajes DATA hasta la generación del resultado final.

### Grandes volúmenes de datos

La working queue compartida entre el Gateway y las instancias de Sum permite distribuir los mensajes DATA entre todas las réplicas disponibles. De esta forma, los datos enviados por un mismo cliente no deben ser procesados completamente por una única instancia de Sum, sino que pueden repartirse entre varias.

Cada Sum realiza una agregación local de los registros que procesa. Por lo tanto, la siguiente etapa no recibe nuevamente todos los mensajes DATA originales, sino únicamente los resultados parciales ya consolidados.

Una vez terminada la recolección, estos resultados se particionan entre las distintas instancias de Aggregation. Cada combinación de cliente y fruta es enviada a un único Aggregator, evitando realizar broadcast del volumen completo de datos.

Finalmente, cada Aggregator envía a Join solamente su top parcial de tamaño máximo TOP_SIZE. De esta forma, la cantidad de elementos que Join debe procesar depende principalmente de la cantidad de Aggregators y del tamaño del top, y no directamente de la cantidad total de registros enviados inicialmente por los clientes.

### Cantidad de controles

La cantidad de instancias de Sum y Aggregation puede modificarse sin cambiar la lógica general del sistema.

Al aumentar la cantidad de instancias de Sum, la working queue dispone de más consumidores que pueden repartirse los mensajes DATA. La coordinación necesaria para determinar el final de la ingesta se realiza mediante el ring de control: los mensajes COUNT y FINISH recorren las instancias de Sum, por lo que el costo de esta coordinación depende de la cantidad de réplicas y no de la cantidad total de mensajes DATA procesados.

Al aumentar la cantidad de Aggregators, aumenta también la cantidad de particiones en las que se distribuyen los resultados de Sum. Cada registro consolidado es asignado a exactamente una de estas instancias mediante el criterio de particionado determinístico, por lo que agregar Aggregators no implica replicar el conjunto completo de datos.

Cada Sum envía una contribución PARTIAL a cada Aggregator para permitir detectar la finalización de cada partición. Algunas de estas contribuciones pueden ser vacías, pero los registros reales permanecen particionados y no son enviados a todos los Aggregators.

Por último, Join recibe como máximo un top parcial por cada instancia de Aggregation. Por lo tanto, al aumentar la cantidad de Aggregators también aumenta la cantidad de candidatos que Join debe combinar, pero cada uno de ellos envía únicamente hasta TOP_SIZE elementos en lugar de toda su partición.

## Finalización de los controles

Las instancias de Sum, Aggregation y Join manejan la señal SIGTERM para permitir una finalización ordenada de los controles.

Al recibir esta señal, cada componente inicia su procedimiento de Shutdown, evitando interrumpir de forma abrupta operaciones que ya se encuentran en curso. En particular, se detiene el consumo de nuevos mensajes y se permite que los callbacks que ya estaban siendo ejecutados finalicen antes de cerrar los recursos de salida utilizados para publicar mensajes.

En Aggregation y Join, primero se cierra el consumer de entrada y, una vez finalizado cualquier procesamiento activo, se cierran las conexiones utilizadas para publicar los resultados hacia la siguiente etapa.

En Sum el procedimiento requiere además detener los mecanismos asociados a la coordinación distribuida. Durante el shutdown se impide iniciar nuevos reintentos de COUNT, se cancelan los timers pendientes, se detienen los consumers de datos y de control y se espera la finalización de los reintentos que ya se encontraban activos. Sólo después se cierran los recursos de salida utilizados para enviar mensajes PARTIAL y mensajes del ring de control.

De esta forma, los controles pueden finalizar ordenadamente ante una señal SIGTERM, evitando cerrar recursos que todavía puedan estar siendo utilizados por operaciones en curso.