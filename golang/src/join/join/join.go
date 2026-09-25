package join

import (
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	mu                  sync.Mutex
	inputQueue          middleware.Middleware
	outputQueue         middleware.Middleware
	aggregationAmount   int
	topSize             int
	states              map[string]*joinState
	completedClients    map[string]time.Time
	completedSinceSweep int
}

const (
	completedClientTTL           = 5 * time.Minute
	completedClientSweepInterval = 100
)

type joinState struct {
	candidates           []fruititem.FruitItem
	receivedAggregations map[int]struct{}
	publishing           bool
}

func NewJoin(config JoinConfig) (*Join, error) {
	if config.AggregationAmount <= 0 {
		return nil, errors.New("aggregation amount must be greater than zero")
	}
	if config.TopSize <= 0 {
		return nil, errors.New("top size must be greater than zero")
	}
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue: inputQueue, outputQueue: outputQueue,
		aggregationAmount: config.AggregationAmount, topSize: config.TopSize,
		states: map[string]*joinState{}, completedClients: map[string]time.Time{},
	}, nil
}

func (join *Join) Run() {
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	envelope, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("Discarding malformed Join message", "err", err)
		ack()
		return
	}
	if envelope.Type != inner.MessageTypeTopPartial || envelope.AggregationID < 0 || envelope.AggregationID >= join.aggregationAmount {
		slog.Error("Discarding unexpected Join message", "type", envelope.Type, "aggregation_id", envelope.AggregationID)
		ack()
		return
	}

	join.mu.Lock()
	if completedAt, completed := join.completedClients[envelope.ClientID]; completed {
		if time.Since(completedAt) < completedClientTTL {
			join.mu.Unlock()
			ack()
			return
		}
		delete(join.completedClients, envelope.ClientID)
	}
	state := join.states[envelope.ClientID]
	if state == nil {
		state = &joinState{receivedAggregations: map[int]struct{}{}}
		join.states[envelope.ClientID] = state
	}
	if _, duplicate := state.receivedAggregations[envelope.AggregationID]; !duplicate {
		state.candidates = append(state.candidates, envelope.Records...)
		state.receivedAggregations[envelope.AggregationID] = struct{}{}
	}
	if state.publishing || len(state.receivedAggregations) != join.aggregationAmount {
		join.mu.Unlock()
		ack()
		return
	}
	state.publishing = true
	candidates := append([]fruititem.FruitItem(nil), state.candidates...)
	join.mu.Unlock()

	sort.SliceStable(candidates, func(i, j int) bool {
		return candidates[j].Less(candidates[i])
	})
	candidates = candidates[:min(join.topSize, len(candidates))]
	message, err := inner.SerializeResultMessage(envelope.ClientID, candidates)
	if err == nil {
		err = join.outputQueue.Send(*message)
	}
	if err != nil {
		join.mu.Lock()
		if join.states[envelope.ClientID] == state {
			state.publishing = false
		}
		join.mu.Unlock()
		slog.Error("While publishing RESULT", "client_id", envelope.ClientID, "err", err)
		nack()
		return
	}
	join.mu.Lock()
	if join.states[envelope.ClientID] == state {
		join.recordCompletedClientLocked(envelope.ClientID)
		delete(join.states, envelope.ClientID)
	}
	join.mu.Unlock()
	ack()
}

func (join *Join) recordCompletedClientLocked(clientID string) {
	join.completedClients[clientID] = time.Now()
	join.completedSinceSweep++
	if join.completedSinceSweep >= completedClientSweepInterval {
		join.sweepCompletedClientsLocked(time.Now())
	}
}

func (join *Join) sweepCompletedClientsLocked(now time.Time) {
	for clientID, completedAt := range join.completedClients {
		if now.Sub(completedAt) >= completedClientTTL {
			delete(join.completedClients, clientID)
		}
	}
	join.completedSinceSweep = 0
}
