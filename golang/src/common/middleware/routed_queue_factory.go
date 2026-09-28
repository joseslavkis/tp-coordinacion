package middleware

import amqp "github.com/rabbitmq/amqp091-go"

func CreateRoutedQueueMiddleware(exchangeName string, routingKeys []string, connectionSettings ConnSettings) (RoutedMiddleware, error) {
	middleware, err := newExchangeMiddleware(exchangeName, routingKeys, connectionSettings)
	if err != nil {
		return nil, err
	}
	exchange := middleware.(*ExchangeMiddleware)
	for _, routingKey := range routingKeys {
		if _, err := exchange.publisher.QueueDeclare(routingKey, true, false, false, false, amqp.Table(nil)); err != nil {
			return nil, closeFailedRoutedExchange(exchange)
		}
		if err := exchange.publisher.QueueBind(routingKey, routingKey, exchangeName, false, nil); err != nil {
			return nil, closeFailedRoutedExchange(exchange)
		}
	}
	return exchange, nil
}

func closeFailedRoutedExchange(exchange *ExchangeMiddleware) error {
	disconnected := exchange.connection.IsClosed()
	_ = exchange.Close()
	if disconnected {
		return ErrMessageMiddlewareDisconnected
	}
	return ErrMessageMiddlewareMessage
}
