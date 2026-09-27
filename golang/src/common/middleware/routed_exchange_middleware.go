package middleware

import amqp "github.com/rabbitmq/amqp091-go"

type RoutedMiddleware interface {
	Middleware
	SendTo(routingKey string, message Message) error
}

func (exchange *ExchangeMiddleware) SendTo(routingKey string, message Message) error {
	return exchange.sendTo(routingKey, message, exchange.publisher.Publish)
}

func (exchange *ExchangeMiddleware) sendTo(routingKey string, message Message, publish func(string, string, bool, bool, amqp.Publishing) error) error {
	exchange.lifecycleMutex.Lock()
	defer exchange.lifecycleMutex.Unlock()

	if exchange.closed || exchange.connection.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}

	configured := false
	for _, key := range exchange.routingKeys {
		if key == routingKey {
			configured = true
			break
		}
	}
	if !configured {
		return ErrMessageMiddlewareMessage
	}

	if err := publish(exchange.exchangeName, routingKey, false, false, amqp.Publishing{Body: []byte(message.Body)}); err != nil {
		if exchange.connection.IsClosed() {
			return ErrMessageMiddlewareDisconnected
		}
		return ErrMessageMiddlewareMessage
	}

	return nil
}
