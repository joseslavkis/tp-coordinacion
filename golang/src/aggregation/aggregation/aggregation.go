package aggregation

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
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

type aggregationKey struct {
	clientID string
	round    uint64
}

type aggregationRound struct {
	records    map[string]fruititem.FruitItem
	partials   map[int][]fruititem.FruitItem
	done       map[int]struct{}
	publishing bool
}

const (
	resultRetryBaseDelay = 25 * time.Millisecond
	resultRetryMaxDelay  = time.Second
)

type resultRetryDelayFunc func(attempt uint) time.Duration
type resultRetryCancel func()
type resultRetryScheduler func(delay time.Duration, callback func()) resultRetryCancel

type resultRetryAction struct {
	message    middleware.Message
	round      *aggregationRound
	attempt    uint
	generation uint64
	sending    bool
	completed  bool
	cancel     resultRetryCancel
}

type Aggregation struct {
	mu            sync.Mutex
	outputQueue   middleware.Middleware
	inputExchange middleware.Middleware
	fruitItemMap  map[string]map[string]fruititem.FruitItem
	topSize       int
	sumAmount     int
	rounds        map[aggregationKey]*aggregationRound
	roundByClient map[string]uint64
	completed     map[aggregationKey]struct{}
	resultRetries map[aggregationKey]*resultRetryAction
	retryDelay    resultRetryDelayFunc
	retrySchedule resultRetryScheduler
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

	aggregation := &Aggregation{
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		fruitItemMap:  map[string]map[string]fruititem.FruitItem{},
		topSize:       config.TopSize,
		sumAmount:     config.SumAmount,
	}
	aggregation.initializeStage3State()
	return aggregation, nil
}

func (aggregation *Aggregation) Run() {
	if err := aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	}); err != nil {
		slog.Error("While consuming Aggregation input exchange", "err", err)
	}
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	envelope, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		nack()
		return
	}

	stage3Message := false
	switch envelope.Type {
	case inner.MessageTypeData:
		aggregation.handleDataMessage(envelope.ClientID, envelope.Records)
	case inner.MessageTypeEOF:
		err = aggregation.handleEndOfRecordsMessage(envelope.ClientID)
	case inner.MessageTypePartial:
		stage3Message = true
		err = aggregation.handlePartialMessage(envelope)
	case inner.MessageTypeSumDone:
		stage3Message = true
		err = aggregation.handleSumDoneMessage(envelope)
	default:
		err = fmt.Errorf("unexpected inner message type %q", envelope.Type)
	}
	if err != nil {
		if stage3Message {
			slog.Error("Discarding permanently invalid Stage 3 aggregation message", "type", envelope.Type, "client_id", envelope.ClientID, "round", envelope.Round, "sum_id", envelope.SumID, "err", err)
			ack()
			return
		}
		slog.Error("While handling aggregation message", "type", envelope.Type, "err", err)
		nack()
		return
	}
	ack()
}

func (aggregation *Aggregation) initializeStage3State() {
	aggregation.mu.Lock()
	defer aggregation.mu.Unlock()
	aggregation.initializeStage3StateLocked()
}

func (aggregation *Aggregation) initializeStage3StateLocked() {
	if aggregation.fruitItemMap == nil {
		aggregation.fruitItemMap = map[string]map[string]fruititem.FruitItem{}
	}
	if aggregation.rounds == nil {
		aggregation.rounds = map[aggregationKey]*aggregationRound{}
	}
	if aggregation.roundByClient == nil {
		aggregation.roundByClient = map[string]uint64{}
	}
	if aggregation.completed == nil {
		aggregation.completed = map[aggregationKey]struct{}{}
	}
	if aggregation.resultRetries == nil {
		aggregation.resultRetries = map[aggregationKey]*resultRetryAction{}
	}
	if aggregation.retryDelay == nil {
		aggregation.retryDelay = defaultResultRetryDelay
	}
	if aggregation.retrySchedule == nil {
		aggregation.retrySchedule = defaultResultRetryScheduler
	}
}

func (aggregation *Aggregation) handlePartialMessage(envelope inner.Envelope) error {
	if err := aggregation.validateSumID(envelope.SumID); err != nil {
		return err
	}
	key := aggregationKey{clientID: envelope.ClientID, round: envelope.Round}
	aggregation.mu.Lock()
	aggregation.initializeStage3StateLocked()
	round, completed, err := aggregation.roundForEnvelopeLocked(key)
	if err != nil || completed {
		aggregation.mu.Unlock()
		return err
	}
	if previous, duplicate := round.partials[envelope.SumID]; duplicate {
		if !sameRecords(previous, envelope.Records) {
			aggregation.mu.Unlock()
			return fmt.Errorf("sum %d sent a divergent duplicate PARTIAL", envelope.SumID)
		}
	} else {
		for _, record := range envelope.Records {
			if current, ok := round.records[record.Fruit]; ok {
				round.records[record.Fruit] = current.Sum(record)
			} else {
				round.records[record.Fruit] = record
			}
		}
		round.partials[envelope.SumID] = append([]fruititem.FruitItem(nil), envelope.Records...)
	}
	records, shouldPublish := aggregation.prepareResultLocked(round)
	aggregation.mu.Unlock()
	if shouldPublish {
		aggregation.startResultPublish(key, round, records)
	}
	return nil
}

func (aggregation *Aggregation) handleSumDoneMessage(envelope inner.Envelope) error {
	if err := aggregation.validateSumID(envelope.SumID); err != nil {
		return err
	}
	key := aggregationKey{clientID: envelope.ClientID, round: envelope.Round}
	aggregation.mu.Lock()
	aggregation.initializeStage3StateLocked()
	round, completed, err := aggregation.roundForEnvelopeLocked(key)
	if err != nil || completed {
		aggregation.mu.Unlock()
		return err
	}
	round.done[envelope.SumID] = struct{}{}
	records, shouldPublish := aggregation.prepareResultLocked(round)
	aggregation.mu.Unlock()
	if shouldPublish {
		aggregation.startResultPublish(key, round, records)
	}
	return nil
}

func (aggregation *Aggregation) validateSumID(sumID int) error {
	if aggregation.sumAmount <= 0 {
		return errors.New("aggregation sum amount must be greater than zero for Stage 3 messages")
	}
	if sumID < 0 || sumID >= aggregation.sumAmount {
		return fmt.Errorf("sum id %d is outside [0, %d)", sumID, aggregation.sumAmount)
	}
	return nil
}

func (aggregation *Aggregation) roundForEnvelopeLocked(key aggregationKey) (*aggregationRound, bool, error) {
	if _, ok := aggregation.completed[key]; ok {
		return nil, true, nil
	}
	if activeRound, ok := aggregation.roundByClient[key.clientID]; ok && activeRound != key.round {
		return nil, false, fmt.Errorf("client %q active round is %d, got %d", key.clientID, activeRound, key.round)
	}
	round := aggregation.rounds[key]
	if round == nil {
		round = &aggregationRound{
			records:  map[string]fruititem.FruitItem{},
			partials: map[int][]fruititem.FruitItem{},
			done:     map[int]struct{}{},
		}
		aggregation.rounds[key] = round
		aggregation.roundByClient[key.clientID] = key.round
	}
	return round, false, nil
}

func (aggregation *Aggregation) prepareResultLocked(round *aggregationRound) ([]fruititem.FruitItem, bool) {
	if round.publishing || len(round.partials) != aggregation.sumAmount || len(round.done) != aggregation.sumAmount {
		return nil, false
	}
	round.publishing = true
	return buildFruitTop(round.records, aggregation.topSize), true
}

func (aggregation *Aggregation) startResultPublish(key aggregationKey, round *aggregationRound, records []fruititem.FruitItem) {
	message, err := inner.SerializeResultMessage(key.clientID, records)
	if err != nil {
		aggregation.mu.Lock()
		if aggregation.rounds[key] == round {
			round.publishing = false
		}
		aggregation.mu.Unlock()
		slog.Error("While serializing RESULT", "client_id", key.clientID, "round", key.round, "err", err)
		return
	}

	aggregation.mu.Lock()
	if aggregation.rounds[key] != round {
		aggregation.mu.Unlock()
		return
	}
	if _, exists := aggregation.resultRetries[key]; exists {
		aggregation.mu.Unlock()
		return
	}
	action := &resultRetryAction{message: *message, round: round}
	aggregation.resultRetries[key] = action
	aggregation.mu.Unlock()
	aggregation.attemptResultPublish(key, action)
}

func (aggregation *Aggregation) attemptResultPublish(key aggregationKey, action *resultRetryAction) {
	aggregation.mu.Lock()
	if aggregation.resultRetries[key] != action || action.sending || action.completed {
		aggregation.mu.Unlock()
		return
	}
	action.sending = true
	aggregation.mu.Unlock()

	err := aggregation.outputQueue.Send(action.message)

	aggregation.mu.Lock()
	if aggregation.resultRetries[key] != action || action.completed {
		aggregation.mu.Unlock()
		return
	}
	action.sending = false
	if err == nil {
		action.completed = true
		cancel := action.cancel
		action.cancel = nil
		aggregation.completed[key] = struct{}{}
		delete(aggregation.rounds, key)
		delete(aggregation.roundByClient, key.clientID)
		delete(aggregation.resultRetries, key)
		aggregation.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		return
	}

	attempt := action.attempt
	action.attempt++
	action.generation++
	generation := action.generation
	delay := aggregation.retryDelay(attempt)
	scheduler := aggregation.retrySchedule
	aggregation.mu.Unlock()

	slog.Warn("Scheduling RESULT retry", "client_id", key.clientID, "round", key.round, "attempt", attempt+1, "delay", delay, "err", err)
	cancel := scheduler(delay, func() {
		aggregation.retryResultPublish(key, action, generation)
	})
	aggregation.mu.Lock()
	if aggregation.resultRetries[key] == action && !action.completed && action.generation == generation {
		action.cancel = cancel
		aggregation.mu.Unlock()
		return
	}
	aggregation.mu.Unlock()
	cancel()
}

func (aggregation *Aggregation) retryResultPublish(key aggregationKey, action *resultRetryAction, generation uint64) {
	aggregation.mu.Lock()
	if aggregation.resultRetries[key] != action || action.completed || action.generation != generation {
		aggregation.mu.Unlock()
		return
	}
	action.cancel = nil
	aggregation.mu.Unlock()
	aggregation.attemptResultPublish(key, action)
}

func resultJitteredBackoff(attempt uint, sample uint64) time.Duration {
	bound := resultRetryBaseDelay
	for step := uint(0); step < attempt && bound < resultRetryMaxDelay; step++ {
		if bound > resultRetryMaxDelay/2 {
			bound = resultRetryMaxDelay
			break
		}
		bound *= 2
	}
	if bound > resultRetryMaxDelay {
		bound = resultRetryMaxDelay
	}
	floor := bound / 2
	span := uint64(bound-floor) + 1
	return floor + time.Duration(sample%span)
}

func defaultResultRetryDelay(attempt uint) time.Duration {
	return resultJitteredBackoff(attempt, rand.Uint64())
}

func defaultResultRetryScheduler(delay time.Duration, callback func()) resultRetryCancel {
	timer := time.AfterFunc(delay, callback)
	return func() { timer.Stop() }
}

func sameRecords(left, right []fruititem.FruitItem) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(clientID string) error {
	slog.Info("Received End Of Records message", "client_id", clientID)
	aggregation.mu.Lock()
	records := buildFruitTop(aggregation.fruitItemMap[clientID], aggregation.topSize)
	aggregation.mu.Unlock()

	message, err := inner.SerializeResultMessage(clientID, records)
	if err != nil {
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		return err
	}
	aggregation.mu.Lock()
	delete(aggregation.fruitItemMap, clientID)
	aggregation.mu.Unlock()
	return nil
}

func (aggregation *Aggregation) handleDataMessage(clientID string, fruitRecords []fruititem.FruitItem) {
	aggregation.mu.Lock()
	defer aggregation.mu.Unlock()
	aggregation.initializeStage3StateLocked()
	clientRecords, ok := aggregation.fruitItemMap[clientID]
	if !ok {
		clientRecords = map[string]fruititem.FruitItem{}
		aggregation.fruitItemMap[clientID] = clientRecords
	}
	for _, fruitRecord := range fruitRecords {
		if current, ok := clientRecords[fruitRecord.Fruit]; ok {
			clientRecords[fruitRecord.Fruit] = current.Sum(fruitRecord)
		} else {
			clientRecords[fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(clientID string) []fruititem.FruitItem {
	aggregation.mu.Lock()
	defer aggregation.mu.Unlock()
	return buildFruitTop(aggregation.fruitItemMap[clientID], aggregation.topSize)
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
