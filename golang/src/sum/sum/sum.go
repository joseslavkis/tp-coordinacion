package sum

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"

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
	mu              sync.Mutex
	inputQueue      middleware.Middleware
	outputExchange  middleware.Middleware
	controlInput    middleware.Middleware
	controlOutput   middleware.Middleware
	id              int
	sumAmount       int
	fruitItemMap    map[string]map[string]fruititem.FruitItem
	processedCount  map[string]uint64
	dataPublished   map[string]bool
	barriers        map[string]*clientBarrierState
	countRounds     map[tokenKey]*countRoundProgress
	finishRounds    map[tokenKey]*finishRoundProgress
	outboundActions map[outboundKey]*outboundAction
	retryDelay      retryDelayFunc
	retryScheduler  retryScheduler
}

func NewSum(config SumConfig) (*Sum, error) {
	if config.SumAmount <= 0 {
		return nil, errors.New("sum amount must be greater than zero")
	}
	if config.Id < 0 || config.Id >= config.SumAmount {
		return nil, fmt.Errorf("sum id %d is outside [0, %d)", config.Id, config.SumAmount)
	}
	if config.SumPrefix == "" {
		return nil, errors.New("sum prefix is required")
	}

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

	controlInput, err := middleware.CreateQueueMiddleware(controlQueueName(config.SumPrefix, config.Id), connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}
	controlOutput, err := middleware.CreateQueueMiddleware(controlQueueName(config.SumPrefix, successorID(config.Id, config.SumAmount)), connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		controlInput.Close()
		return nil, err
	}

	sum := &Sum{
		inputQueue:     inputQueue,
		outputExchange: outputExchange,
		controlInput:   controlInput,
		controlOutput:  controlOutput,
		id:             config.Id,
		sumAmount:      config.SumAmount,
		fruitItemMap:   map[string]map[string]fruititem.FruitItem{},
		processedCount: map[string]uint64{},
		dataPublished:  map[string]bool{},
	}
	sum.initializeControlState()
	return sum, nil
}

func (sum *Sum) Run() {
	if sum.controlInput != nil {
		go func() {
			if err := sum.controlInput.StartConsuming(func(msg middleware.Message, ack, nack func()) {
				sum.handleControlMessage(msg, ack, nack)
			}); err != nil {
				slog.Error("While consuming Sum control queue", "err", err)
			}
		}()
	}
	if err := sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming Sum input queue", "err", err)
	}
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
		if sum.sumAmount == 0 || sum.controlOutput == nil {
			err = sum.handleLegacyEndOfRecordMessage(envelope.ClientID)
		} else {
			err = sum.handleEndOfRecordMessage(envelope.ClientID, envelope.TotalMessages)
		}
		if err != nil {
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

func (sum *Sum) handleEndOfRecordMessage(clientID string, totalMessages uint64) error {
	sum.mu.Lock()
	sum.initializeControlStateLocked()
	if existing, ok := sum.barriers[clientID]; ok {
		if existing.expected != totalMessages {
			sum.mu.Unlock()
			return fmt.Errorf("client %q EOF total changed from %d to %d", clientID, existing.expected, totalMessages)
		}
		sum.mu.Unlock()
		return nil
	}
	sum.barriers[clientID] = &clientBarrierState{expected: totalMessages, leaderID: sum.id}
	sum.mu.Unlock()

	return sum.startCountRound(clientID)
}

func (sum *Sum) handleLegacyEndOfRecordMessage(clientID string) error {
	slog.Info("Received End Of Records message", "client_id", clientID)

	sum.mu.Lock()
	clientRecords := sum.fruitItemMap[clientID]
	dataPublished := sum.dataPublished[clientID]
	fruitRecords := copyFruitRecords(clientRecords)
	sum.mu.Unlock()

	if len(fruitRecords) > 0 && !dataPublished {
		message, err := inner.SerializeDataMessage(clientID, fruitRecords)
		if err != nil {
			return err
		}
		if err := sum.outputExchange.Send(*message); err != nil {
			return err
		}
		sum.mu.Lock()
		if sum.dataPublished == nil {
			sum.dataPublished = map[string]bool{}
		}
		sum.dataPublished[clientID] = true
		sum.mu.Unlock()
	}

	message, err := inner.SerializeEOFMessage(clientID, 0)
	if err != nil {
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		return err
	}
	sum.mu.Lock()
	delete(sum.fruitItemMap, clientID)
	delete(sum.processedCount, clientID)
	delete(sum.dataPublished, clientID)
	sum.mu.Unlock()
	return nil
}

func (sum *Sum) handleDataMessage(clientID string, fruitRecords []fruititem.FruitItem) {
	sum.mu.Lock()
	defer sum.mu.Unlock()
	if sum.fruitItemMap == nil {
		sum.fruitItemMap = map[string]map[string]fruititem.FruitItem{}
	}
	if sum.processedCount == nil {
		sum.processedCount = map[string]uint64{}
	}
	clientRecords, ok := sum.fruitItemMap[clientID]
	if !ok {
		clientRecords = map[string]fruititem.FruitItem{}
		sum.fruitItemMap[clientID] = clientRecords
	}
	for _, fruitRecord := range fruitRecords {
		if current, ok := clientRecords[fruitRecord.Fruit]; ok {
			clientRecords[fruitRecord.Fruit] = current.Sum(fruitRecord)
		} else {
			clientRecords[fruitRecord.Fruit] = fruitRecord
		}
	}
	sum.processedCount[clientID]++
}

func copyFruitRecords(records map[string]fruititem.FruitItem) []fruititem.FruitItem {
	result := make([]fruititem.FruitItem, 0, len(records))
	for _, record := range records {
		result = append(result, record)
	}
	return result
}
