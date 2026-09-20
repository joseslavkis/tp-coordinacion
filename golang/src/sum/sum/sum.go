package sum

import (
	"fmt"
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware
	fruitItemMap   map[string]map[string]fruititem.FruitItem
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:     inputQueue,
		outputExchange: outputExchange,
		fruitItemMap:   map[string]map[string]fruititem.FruitItem{},
	}, nil
}

func (sum *Sum) Run() {
	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	envelope, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}

	switch envelope.Type {
	case inner.MessageTypeData:
		sum.handleDataMessage(envelope.ClientID, envelope.Records)
	case inner.MessageTypeEOF:
		if err := sum.handleEndOfRecordMessage(envelope.ClientID); err != nil {
			slog.Error("While handling end of record message", "err", err)
			nack()
			return
		}
	default:
		slog.Error("Unexpected message type", "type", envelope.Type)
		nack()
		return
	}
	ack()
}

func (sum *Sum) handleEndOfRecordMessage(clientID string) error {
	slog.Info("Received End Of Records message", "client_id", clientID)
	clientRecords := sum.fruitItemMap[clientID]
	if len(clientRecords) > 0 {
		fruitRecords := make([]fruititem.FruitItem, 0, len(clientRecords))
		for _, fruitRecord := range clientRecords {
			fruitRecords = append(fruitRecords, fruitRecord)
		}
		message, err := inner.SerializeMessage(inner.MessageTypeData, clientID, fruitRecords)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		if err := sum.outputExchange.Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	message, err := inner.SerializeMessage(inner.MessageTypeEOF, clientID, []fruititem.FruitItem{})
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	delete(sum.fruitItemMap, clientID)
	return nil
}

func (sum *Sum) handleDataMessage(clientID string, fruitRecords []fruititem.FruitItem) {
	clientRecords, ok := sum.fruitItemMap[clientID]
	if !ok {
		clientRecords = map[string]fruititem.FruitItem{}
		sum.fruitItemMap[clientID] = clientRecords
	}
	for _, fruitRecord := range fruitRecords {
		current, ok := clientRecords[fruitRecord.Fruit]
		if ok {
			clientRecords[fruitRecord.Fruit] = current.Sum(fruitRecord)
		} else {
			clientRecords[fruitRecord.Fruit] = fruitRecord
		}
	}
}
