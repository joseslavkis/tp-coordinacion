package aggregation

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"

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

type aggregationKey struct {
	clientID string
	round    uint64
}

type aggregationRound struct {
	records          map[string]fruititem.FruitItem
	receivedPartials map[int]struct{}
	done             map[int]struct{}
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
	mu            sync.Mutex
	outputQueue   middleware.Middleware
	inputExchange middleware.Middleware
	topSize       int
	sumAmount     int
	rounds        map[aggregationKey]*aggregationRound
	roundByClient map[string]uint64
	completed     map[aggregationKey]struct{}
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
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		topSize:       config.TopSize,
		sumAmount:     config.SumAmount,
		rounds:        map[aggregationKey]*aggregationRound{},
		roundByClient: map[string]uint64{},
		completed:     map[aggregationKey]struct{}{},
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
	case inner.MessageTypeSumDone:
		err = aggregation.handleSumDoneMessage(envelope)
	default:
		slog.Error("Discarding unexpected Aggregation message", "type", envelope.Type)
		ack()
		return
	}
	if err != nil {
		var poison *poisonError
		if errors.As(err, &poison) {
			slog.Error("Discarding invalid completion message", "type", envelope.Type, "client_id", envelope.ClientID, "round", envelope.Round, "sum_id", envelope.SumID, "err", err)
			ack()
			return
		}
		slog.Error("While publishing RESULT", "client_id", envelope.ClientID, "round", envelope.Round, "err", err)
		nack()
		return
	}
	ack()
}

func (aggregation *Aggregation) handlePartialMessage(envelope inner.Envelope) error {
	if err := aggregation.validateSumID(envelope.SumID); err != nil {
		return err
	}
	key := aggregationKey{clientID: envelope.ClientID, round: envelope.Round}
	aggregation.mu.Lock()
	round, completed, err := aggregation.roundForEnvelopeLocked(key)
	if err != nil || completed {
		aggregation.mu.Unlock()
		return err
	}
	if _, duplicate := round.receivedPartials[envelope.SumID]; !duplicate {
		for _, record := range envelope.Records {
			if current, ok := round.records[record.Fruit]; ok {
				round.records[record.Fruit] = current.Sum(record)
			} else {
				round.records[record.Fruit] = record
			}
		}
		round.receivedPartials[envelope.SumID] = struct{}{}
	}
	records, shouldPublish := aggregation.prepareResultLocked(round)
	aggregation.mu.Unlock()
	if shouldPublish {
		return aggregation.publishResult(key, round, records)
	}
	return nil
}

func (aggregation *Aggregation) handleSumDoneMessage(envelope inner.Envelope) error {
	if err := aggregation.validateSumID(envelope.SumID); err != nil {
		return err
	}
	key := aggregationKey{clientID: envelope.ClientID, round: envelope.Round}
	aggregation.mu.Lock()
	round, completed, err := aggregation.roundForEnvelopeLocked(key)
	if err != nil || completed {
		aggregation.mu.Unlock()
		return err
	}
	round.done[envelope.SumID] = struct{}{}
	records, shouldPublish := aggregation.prepareResultLocked(round)
	aggregation.mu.Unlock()
	if shouldPublish {
		return aggregation.publishResult(key, round, records)
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

func (aggregation *Aggregation) roundForEnvelopeLocked(key aggregationKey) (*aggregationRound, bool, error) {
	if _, ok := aggregation.completed[key]; ok {
		return nil, true, nil
	}
	if activeRound, ok := aggregation.roundByClient[key.clientID]; ok && activeRound != key.round {
		return nil, false, &poisonError{err: fmt.Errorf("client %q active round is %d, got %d", key.clientID, activeRound, key.round)}
	}
	round := aggregation.rounds[key]
	if round == nil {
		round = &aggregationRound{
			records:          map[string]fruititem.FruitItem{},
			receivedPartials: map[int]struct{}{},
			done:             map[int]struct{}{},
		}
		aggregation.rounds[key] = round
		aggregation.roundByClient[key.clientID] = key.round
	}
	return round, false, nil
}

func (aggregation *Aggregation) prepareResultLocked(round *aggregationRound) ([]fruititem.FruitItem, bool) {
	if round.publishing || len(round.receivedPartials) != aggregation.sumAmount || len(round.done) != aggregation.sumAmount {
		return nil, false
	}
	round.publishing = true
	return buildFruitTop(round.records, aggregation.topSize), true
}

func (aggregation *Aggregation) publishResult(key aggregationKey, round *aggregationRound, records []fruititem.FruitItem) error {
	message, err := inner.SerializeResultMessage(key.clientID, records)
	if err != nil {
		aggregation.mu.Lock()
		if aggregation.rounds[key] == round {
			round.publishing = false
		}
		aggregation.mu.Unlock()
		slog.Error("Discarding invalid RESULT state", "client_id", key.clientID, "round", key.round, "err", err)
		return nil
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		aggregation.mu.Lock()
		if aggregation.rounds[key] == round {
			round.publishing = false
		}
		aggregation.mu.Unlock()
		return err
	}

	aggregation.mu.Lock()
	if aggregation.rounds[key] == round {
		aggregation.completed[key] = struct{}{}
		delete(aggregation.rounds, key)
		delete(aggregation.roundByClient, key.clientID)
	}
	aggregation.mu.Unlock()
	return nil
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
