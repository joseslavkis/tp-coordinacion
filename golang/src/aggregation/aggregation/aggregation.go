package aggregation

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

const (
	completedClientTTL           = 5 * time.Minute
	completedClientSweepInterval = 100
)

type aggregationState struct {
	records          map[string]fruititem.FruitItem
	receivedPartials map[int]struct{}
	publishing       bool
}

type poisonError struct {
	err error
}

func (err *poisonError) Error() string {
	return err.err.Error()
}

func (err *poisonError) Unwrap() error {
	return err.err
}

type Aggregation struct {
	mu                  sync.Mutex
	outputQueue         middleware.Middleware
	inputExchange       middleware.Middleware
	topSize             int
	sumAmount           int
	states              map[string]*aggregationState
	completedClients    map[string]time.Time
	completedSinceSweep int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	if config.SumAmount <= 0 {
		return nil, errors.New("sum amount must be greater than zero")
	}
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:      outputQueue,
		inputExchange:    inputExchange,
		topSize:          config.TopSize,
		sumAmount:        config.SumAmount,
		states:           map[string]*aggregationState{},
		completedClients: map[string]time.Time{},
	}, nil
}

func (aggregation *Aggregation) Run() {
	if err := aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming Aggregation completion exchange", "err", err)
	}
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	envelope, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("Discarding malformed completion message", "err", err)
		ack()
		return
	}

	switch envelope.Type {
	case inner.MessageTypePartial:
		err = aggregation.handlePartialMessage(envelope)
	default:
		slog.Error("Discarding unexpected Aggregation message", "type", envelope.Type)
		ack()
		return
	}
	if err != nil {
		var poison *poisonError
		if errors.As(err, &poison) {
			slog.Error("Discarding invalid completion message", "type", envelope.Type, "client_id", envelope.ClientID, "sum_id", envelope.SumID, "err", err)
			ack()
			return
		}
		slog.Error("While publishing RESULT", "client_id", envelope.ClientID, "err", err)
		nack()
		return
	}
	ack()
}

func (aggregation *Aggregation) handlePartialMessage(envelope inner.Envelope) error {
	if err := aggregation.validateSumID(envelope.SumID); err != nil {
		return err
	}
	clientID := envelope.ClientID
	aggregation.mu.Lock()
	state, completed, err := aggregation.stateForClientLocked(clientID)
	if err != nil || completed {
		aggregation.mu.Unlock()
		return err
	}
	if _, duplicate := state.receivedPartials[envelope.SumID]; !duplicate {
		for _, record := range envelope.Records {
			if current, ok := state.records[record.Fruit]; ok {
				state.records[record.Fruit] = current.Sum(record)
			} else {
				state.records[record.Fruit] = record
			}
		}
		state.receivedPartials[envelope.SumID] = struct{}{}
	}
	records, shouldPublish := aggregation.prepareResultLocked(state)
	aggregation.mu.Unlock()
	if shouldPublish {
		return aggregation.publishResult(clientID, state, records)
	}
	return nil
}

func (aggregation *Aggregation) validateSumID(sumID int) error {
	if aggregation.sumAmount <= 0 {
		return &poisonError{err: errors.New("aggregation sum amount must be greater than zero")}
	}
	if sumID < 0 || sumID >= aggregation.sumAmount {
		return &poisonError{err: fmt.Errorf("sum id %d is outside [0, %d)", sumID, aggregation.sumAmount)}
	}
	return nil
}

func (aggregation *Aggregation) stateForClientLocked(clientID string) (*aggregationState, bool, error) {
	if completedAt, completed := aggregation.completedClients[clientID]; completed {
		if time.Since(completedAt) < completedClientTTL {
			return nil, true, nil
		}
		delete(aggregation.completedClients, clientID)
	}
	state := aggregation.states[clientID]
	if state == nil {
		state = &aggregationState{
			records:          map[string]fruititem.FruitItem{},
			receivedPartials: map[int]struct{}{},
		}
		aggregation.states[clientID] = state
	}
	return state, false, nil
}

func (aggregation *Aggregation) prepareResultLocked(state *aggregationState) ([]fruititem.FruitItem, bool) {
	if state.publishing || len(state.receivedPartials) != aggregation.sumAmount {
		return nil, false
	}
	state.publishing = true
	return buildFruitTop(state.records, aggregation.topSize), true
}

func (aggregation *Aggregation) publishResult(clientID string, state *aggregationState, records []fruititem.FruitItem) error {
	message, err := inner.SerializeResultMessage(clientID, records)
	if err != nil {
		aggregation.mu.Lock()
		if aggregation.states[clientID] == state {
			state.publishing = false
		}
		aggregation.mu.Unlock()
		slog.Error("Discarding invalid RESULT state", "client_id", clientID, "err", err)
		return nil
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		aggregation.mu.Lock()
		if aggregation.states[clientID] == state {
			state.publishing = false
		}
		aggregation.mu.Unlock()
		return err
	}

	aggregation.mu.Lock()
	if aggregation.states[clientID] == state {
		aggregation.recordCompletedClientLocked(clientID)
		delete(aggregation.states, clientID)
	}
	aggregation.mu.Unlock()
	return nil
}

func (aggregation *Aggregation) recordCompletedClientLocked(clientID string) {
	aggregation.completedClients[clientID] = time.Now()
	aggregation.completedSinceSweep++
	if aggregation.completedSinceSweep >= completedClientSweepInterval {
		aggregation.sweepCompletedClientsLocked(time.Now())
	}
}

func (aggregation *Aggregation) sweepCompletedClientsLocked(now time.Time) {
	for clientID, completedAt := range aggregation.completedClients {
		if now.Sub(completedAt) >= completedClientTTL {
			delete(aggregation.completedClients, clientID)
		}
	}
	aggregation.completedSinceSweep = 0
}

func buildFruitTop(records map[string]fruititem.FruitItem, topSize int) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(records))
	for _, item := range records {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
