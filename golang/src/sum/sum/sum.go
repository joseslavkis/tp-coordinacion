package sum

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

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
	mu                  sync.Mutex
	lifecycleMu         sync.Mutex
	shutdownDone        chan struct{}
	shutdownErr         error
	running             bool
	retryStopping       bool
	activeRetries       sync.WaitGroup
	inputQueue          middleware.Middleware
	outputExchange      middleware.RoutedMiddleware
	controlInput        middleware.Middleware
	controlOutput       middleware.Middleware
	id                  int
	sumAmount           int
	aggregationAmount   int
	aggregationPrefix   string
	fruitItemMap        map[string]map[string]fruititem.FruitItem
	processedCount      map[string]uint64
	barriers            map[string]*clientBarrierState
	initForwards        map[initForwardKey]*initForwardProgress
	countRounds         map[tokenKey]*countRoundProgress
	finishRounds        map[tokenKey]*finishRoundProgress
	completedClients    map[string]time.Time
	completedSinceSweep int
	countRetryDelay     countRetryDelayFunc
	countRetryTimer     countRetryScheduler
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
	if config.AggregationAmount <= 0 {
		return nil, errors.New("aggregation amount must be greater than zero")
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
	outputExchange, err := middleware.CreateRoutedQueueMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
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

	return &Sum{
		inputQueue:        inputQueue,
		outputExchange:    outputExchange,
		controlInput:      controlInput,
		controlOutput:     controlOutput,
		id:                config.Id,
		sumAmount:         config.SumAmount,
		aggregationAmount: config.AggregationAmount,
		aggregationPrefix: config.AggregationPrefix,
		fruitItemMap:      map[string]map[string]fruititem.FruitItem{},
		processedCount:    map[string]uint64{},
		barriers:          map[string]*clientBarrierState{},
		initForwards:      map[initForwardKey]*initForwardProgress{},
		countRounds:       map[tokenKey]*countRoundProgress{},
		finishRounds:      map[tokenKey]*finishRoundProgress{},
		completedClients:  map[string]time.Time{},
		countRetryDelay:   defaultCountRetryDelay,
		countRetryTimer:   defaultCountRetryScheduler,
	}, nil
}

func (sum *Sum) Run() error {
	sum.lifecycleMu.Lock()
	if sum.shutdownDone != nil || sum.running {
		sum.lifecycleMu.Unlock()
		return sum.Shutdown()
	}
	sum.running = true
	sum.lifecycleMu.Unlock()
	var controlDone chan error
	if sum.controlInput != nil {
		controlDone = make(chan error, 1)
		go func() {
			controlDone <- sum.controlInput.StartConsuming(func(msg middleware.Message, ack, nack func()) {
				sum.handleControlMessage(msg, ack, nack)
			})
		}()
	}
	inputDone := make(chan error, 1)
	go func() {
		inputDone <- sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
			sum.handleMessage(msg, ack, nack)
		})
	}()
	var inputErr, controlErr error
	var controlFirst bool
	if controlDone != nil {
		select {
		case inputErr = <-inputDone:
		case controlErr = <-controlDone:
			controlFirst = true
		}
	} else {
		inputErr = <-inputDone
	}
	sum.lifecycleMu.Lock()
	stopping := sum.shutdownDone != nil
	sum.lifecycleMu.Unlock()
	closeDone := make(chan error, 1)
	go func() { closeDone <- sum.Shutdown() }()
	if controlFirst {
		inputErr = <-inputDone
	} else if controlDone != nil {
		controlErr = <-controlDone
	}
	closeErr := <-closeDone
	if inputErr != nil && (!stopping || !errors.Is(inputErr, middleware.ErrMessageMiddlewareDisconnected)) {
		slog.Error("While consuming Sum input queue", "err", inputErr)
	}
	if controlErr != nil && (!stopping || !errors.Is(controlErr, middleware.ErrMessageMiddlewareDisconnected)) {
		slog.Error("While consuming Sum control queue", "err", controlErr)
	}
	if inputErr != nil && (!stopping || !errors.Is(inputErr, middleware.ErrMessageMiddlewareDisconnected)) {
		return inputErr
	}
	if controlErr != nil && (!stopping || !errors.Is(controlErr, middleware.ErrMessageMiddlewareDisconnected)) {
		return controlErr
	}
	return closeErr
}

func (sum *Sum) Shutdown() error {
	sum.lifecycleMu.Lock()
	if done := sum.shutdownDone; done != nil {
		sum.lifecycleMu.Unlock()
		<-done
		return sum.shutdownErr
	}
	sum.shutdownDone = make(chan struct{})
	done := sum.shutdownDone
	sum.mu.Lock()
	sum.retryStopping = true
	var cancels []countRetryCancel
	for _, barrier := range sum.barriers {
		barrier.timerGeneration++
		barrier.timerPending = false
		if barrier.timerCancel != nil {
			cancels = append(cancels, barrier.timerCancel)
			barrier.timerCancel = nil
		}
	}
	sum.mu.Unlock()
	sum.lifecycleMu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	var firstErr error
	if err := sum.inputQueue.Close(); err != nil {
		firstErr = err
	}
	if sum.controlInput != nil {
		if err := sum.controlInput.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	sum.activeRetries.Wait()
	if err := sum.outputExchange.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := sum.controlOutput.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	sum.lifecycleMu.Lock()
	sum.shutdownErr = firstErr
	close(done)
	sum.lifecycleMu.Unlock()
	return firstErr
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	envelope, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("Discarding malformed Sum working-queue message", "err", err)
		ack()
		return
	}

	switch envelope.Type {
	case inner.MessageTypeData:
		sum.handleDataMessage(envelope.ClientID, envelope.Records)
	case inner.MessageTypeEOF:
		err = sum.handleEndOfRecordMessage(envelope.ClientID, envelope.TotalMessages)
	default:
		slog.Error("Discarding unexpected Sum working-queue message", "type", envelope.Type)
		ack()
		return
	}
	if err != nil {
		slog.Error("While forwarding Sum working-queue completion message", "type", envelope.Type, "client_id", envelope.ClientID, "err", err)
		nack()
		return
	}
	ack()
}

func (sum *Sum) handleEndOfRecordMessage(clientID string, totalMessages uint64) error {
	sum.mu.Lock()
	if sum.hasFinishedClientLocked(clientID) {
		sum.mu.Unlock()
		slog.Error("Discarding late EOF for completed client", "client_id", clientID)
		return nil
	}
	sum.mu.Unlock()
	leaderID := leaderForClient(clientID, sum.sumAmount)
	if leaderID == sum.id {
		return sum.initializeBarrier(clientID, totalMessages)
	}
	return sum.forwardBarrierInit(clientID, leaderID, totalMessages, 1)
}

func (sum *Sum) handleDataMessage(clientID string, fruitRecords []fruititem.FruitItem) {
	sum.mu.Lock()
	defer sum.mu.Unlock()
	if sum.hasFinishedClientLocked(clientID) {
		slog.Error("Discarding late DATA for completed client", "client_id", clientID)
		return
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
