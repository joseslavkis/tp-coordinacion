package sum

import (
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const (
	controlRetryBaseDelay = 25 * time.Millisecond
	controlRetryMaxDelay  = time.Second
)

type retryDelayFunc func(attempt uint) time.Duration
type cancelRetry func()
type retryScheduler func(delay time.Duration, callback func()) cancelRetry

type tokenKey struct {
	clientID string
	leaderID int
	round    uint64
}

type outboundKey struct {
	tokenKey
	nodeID int
	phase  string
}

type clientBarrierState struct {
	expected          uint64
	leaderID          int
	round             uint64
	lastObserved      uint64
	incompleteAttempt uint
	timerGeneration   uint64
	timerPending      bool
	timerCancel       cancelRetry
	barrierPassed     bool
	finishStarted     bool
	finishReturned    bool
}

type countRoundProgress struct {
	expected        uint64
	incomingCount   uint64
	incomingVisited uint64
	outboundCount   uint64
	outboundVisited uint64
	forwarded       bool
	returned        bool
}

type finishRoundProgress struct {
	records          []fruititem.FruitItem
	outboundVisited  uint64
	partialPublished bool
	donePublished    bool
	forwarded        bool
}

type outboundAction struct {
	target     middleware.Middleware
	message    middleware.Message
	onSuccess  func()
	attempt    uint
	generation uint64
	sending    bool
	completed  bool
	cancel     cancelRetry
}

func controlQueueName(prefix string, id int) string {
	return fmt.Sprintf("%s_control_%d", prefix, id)
}

func successorID(id, sumAmount int) int {
	return (id + 1) % sumAmount
}

func jitteredBackoff(attempt uint, sample uint64) time.Duration {
	bound := controlRetryBaseDelay
	for step := uint(0); step < attempt && bound < controlRetryMaxDelay; step++ {
		if bound > controlRetryMaxDelay/2 {
			bound = controlRetryMaxDelay
			break
		}
		bound *= 2
	}
	if bound > controlRetryMaxDelay {
		bound = controlRetryMaxDelay
	}
	floor := bound / 2
	span := uint64(bound-floor) + 1
	return floor + time.Duration(sample%span)
}

func defaultRetryDelay(attempt uint) time.Duration {
	return jitteredBackoff(attempt, rand.Uint64())
}

func defaultRetryScheduler(delay time.Duration, callback func()) cancelRetry {
	timer := time.AfterFunc(delay, callback)
	return func() { timer.Stop() }
}

func (sum *Sum) initializeControlState() {
	sum.mu.Lock()
	defer sum.mu.Unlock()
	sum.initializeControlStateLocked()
}

func (sum *Sum) initializeControlStateLocked() {
	if sum.fruitItemMap == nil {
		sum.fruitItemMap = map[string]map[string]fruititem.FruitItem{}
	}
	if sum.processedCount == nil {
		sum.processedCount = map[string]uint64{}
	}
	if sum.dataPublished == nil {
		sum.dataPublished = map[string]bool{}
	}
	if sum.barriers == nil {
		sum.barriers = map[string]*clientBarrierState{}
	}
	if sum.countRounds == nil {
		sum.countRounds = map[tokenKey]*countRoundProgress{}
	}
	if sum.finishRounds == nil {
		sum.finishRounds = map[tokenKey]*finishRoundProgress{}
	}
	if sum.outboundActions == nil {
		sum.outboundActions = map[outboundKey]*outboundAction{}
	}
	if sum.retryDelay == nil {
		sum.retryDelay = defaultRetryDelay
	}
	if sum.retryScheduler == nil {
		sum.retryScheduler = defaultRetryScheduler
	}
}

func (sum *Sum) startCountRound(clientID string) error {
	sum.mu.Lock()
	sum.initializeControlStateLocked()
	barrier, ok := sum.barriers[clientID]
	if !ok || barrier.barrierPassed || barrier.timerPending {
		sum.mu.Unlock()
		return nil
	}
	barrier.round++
	key := tokenKey{clientID: clientID, leaderID: sum.id, round: barrier.round}
	count := sum.processedCount[clientID]
	progress := &countRoundProgress{
		expected:        barrier.expected,
		outboundCount:   count,
		outboundVisited: 1,
	}
	sum.countRounds[key] = progress
	sum.mu.Unlock()

	message, err := inner.SerializeCountMessage(clientID, sum.id, key.round, barrier.expected, count, 1)
	if err != nil {
		return err
	}
	actionKey := outboundKey{tokenKey: key, nodeID: sum.id, phase: "count"}
	sum.dispatchOutbound(actionKey, sum.controlOutput, *message, func() {
		sum.mu.Lock()
		if current := sum.countRounds[key]; current == progress {
			current.forwarded = true
		}
		sum.mu.Unlock()
	})
	return nil
}

func (sum *Sum) handleControlMessage(msg middleware.Message, ack func(), _ func()) {
	envelope, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("Discarding malformed Sum control message", "err", err)
		ack()
		return
	}
	if envelope.Type != inner.MessageTypeCount && envelope.Type != inner.MessageTypeFinish {
		slog.Error("Discarding unexpected Sum control message", "type", envelope.Type)
		ack()
		return
	}
	if envelope.LeaderID < 0 || envelope.LeaderID >= sum.sumAmount || envelope.Visited > uint64(sum.sumAmount) {
		slog.Error("Discarding invalid Sum control metadata", "leader_id", envelope.LeaderID, "visited", envelope.Visited, "sum_amount", sum.sumAmount)
		ack()
		return
	}

	switch envelope.Type {
	case inner.MessageTypeCount:
		sum.handleCountToken(envelope)
	case inner.MessageTypeFinish:
		sum.handleFinishToken(envelope)
	}
	ack()
}

func (sum *Sum) handleCountToken(envelope inner.Envelope) {
	if envelope.Count > envelope.TotalMessages {
		slog.Error("COUNT exceeded expected messages", "client_id", envelope.ClientID, "round", envelope.Round, "count", envelope.Count, "expected", envelope.TotalMessages)
		return
	}
	key := tokenKey{clientID: envelope.ClientID, leaderID: envelope.LeaderID, round: envelope.Round}
	if envelope.LeaderID == sum.id {
		sum.handleReturnedCount(key, envelope)
		return
	}
	if envelope.Visited >= uint64(sum.sumAmount) {
		slog.Error("Discarding COUNT with exhausted traversal", "client_id", envelope.ClientID, "round", envelope.Round, "visited", envelope.Visited)
		return
	}

	sum.mu.Lock()
	sum.initializeControlStateLocked()
	if existing, ok := sum.countRounds[key]; ok {
		if existing.expected != envelope.TotalMessages || existing.incomingCount != envelope.Count || existing.incomingVisited != envelope.Visited {
			slog.Error("Discarding conflicting duplicate COUNT", "client_id", envelope.ClientID, "round", envelope.Round)
		}
		sum.mu.Unlock()
		return
	}
	localCount := sum.processedCount[envelope.ClientID]
	if localCount > math.MaxUint64-envelope.Count {
		sum.mu.Unlock()
		slog.Error("COUNT overflow", "client_id", envelope.ClientID, "round", envelope.Round)
		return
	}
	outboundCount := envelope.Count + localCount
	outboundVisited := envelope.Visited + 1
	progress := &countRoundProgress{
		expected:        envelope.TotalMessages,
		incomingCount:   envelope.Count,
		incomingVisited: envelope.Visited,
		outboundCount:   outboundCount,
		outboundVisited: outboundVisited,
	}
	sum.countRounds[key] = progress
	sum.mu.Unlock()

	if outboundCount > envelope.TotalMessages {
		slog.Error("COUNT exceeded expected messages after contribution", "client_id", envelope.ClientID, "round", envelope.Round, "count", outboundCount, "expected", envelope.TotalMessages)
		return
	}
	message, err := inner.SerializeCountMessage(envelope.ClientID, envelope.LeaderID, envelope.Round, envelope.TotalMessages, outboundCount, outboundVisited)
	if err != nil {
		slog.Error("While serializing COUNT", "err", err)
		return
	}
	actionKey := outboundKey{tokenKey: key, nodeID: sum.id, phase: "count"}
	sum.dispatchOutbound(actionKey, sum.controlOutput, *message, func() {
		sum.mu.Lock()
		if current := sum.countRounds[key]; current == progress {
			current.forwarded = true
		}
		sum.mu.Unlock()
	})
}

func (sum *Sum) handleReturnedCount(key tokenKey, envelope inner.Envelope) {
	if envelope.Visited != uint64(sum.sumAmount) {
		slog.Error("Discarding COUNT returned before full traversal", "client_id", envelope.ClientID, "round", envelope.Round, "visited", envelope.Visited)
		return
	}

	sum.mu.Lock()
	sum.initializeControlStateLocked()
	barrier, ok := sum.barriers[envelope.ClientID]
	progress := sum.countRounds[key]
	if !ok || barrier.leaderID != sum.id || barrier.expected != envelope.TotalMessages || barrier.round != envelope.Round || progress == nil {
		sum.mu.Unlock()
		slog.Error("Discarding COUNT for unknown leader round", "client_id", envelope.ClientID, "round", envelope.Round)
		return
	}
	if progress.returned {
		sum.mu.Unlock()
		return
	}
	progress.returned = true
	if envelope.Count == barrier.expected {
		barrier.barrierPassed = true
		barrier.timerGeneration++
		cancel := barrier.timerCancel
		barrier.timerCancel = nil
		barrier.timerPending = false
		sum.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		sum.onCountBarrierPassed(key)
		return
	}
	if envelope.Count > barrier.expected {
		sum.mu.Unlock()
		slog.Error("COUNT exceeded expected messages at leader", "client_id", envelope.ClientID, "round", envelope.Round, "count", envelope.Count, "expected", barrier.expected)
		return
	}
	if envelope.Count > barrier.lastObserved {
		barrier.lastObserved = envelope.Count
		barrier.incompleteAttempt = 0
	} else {
		barrier.incompleteAttempt++
	}
	attempt := barrier.incompleteAttempt
	barrier.timerGeneration++
	generation := barrier.timerGeneration
	barrier.timerPending = true
	delay := sum.retryDelay(attempt)
	scheduler := sum.retryScheduler
	sum.mu.Unlock()

	cancel := scheduler(delay, func() {
		sum.retryCountRound(envelope.ClientID, envelope.Round, generation)
	})
	sum.mu.Lock()
	barrier = sum.barriers[envelope.ClientID]
	if barrier != nil && barrier.round == envelope.Round && barrier.timerPending && barrier.timerGeneration == generation {
		barrier.timerCancel = cancel
		sum.mu.Unlock()
		return
	}
	sum.mu.Unlock()
	cancel()
}

func (sum *Sum) retryCountRound(clientID string, previousRound, generation uint64) {
	sum.mu.Lock()
	barrier := sum.barriers[clientID]
	if barrier == nil || barrier.barrierPassed || barrier.round != previousRound || !barrier.timerPending || barrier.timerGeneration != generation {
		sum.mu.Unlock()
		return
	}
	barrier.timerPending = false
	barrier.timerCancel = nil
	sum.mu.Unlock()
	if err := sum.startCountRound(clientID); err != nil {
		slog.Error("While starting COUNT retry round", "client_id", clientID, "err", err)
	}
}

func (sum *Sum) dispatchOutbound(key outboundKey, target middleware.Middleware, message middleware.Message, onSuccess func()) {
	sum.mu.Lock()
	sum.initializeControlStateLocked()
	if _, exists := sum.outboundActions[key]; exists {
		sum.mu.Unlock()
		return
	}
	action := &outboundAction{target: target, message: message, onSuccess: onSuccess}
	sum.outboundActions[key] = action
	sum.mu.Unlock()
	sum.attemptOutbound(key, action)
}

func (sum *Sum) attemptOutbound(key outboundKey, action *outboundAction) {
	sum.mu.Lock()
	current := sum.outboundActions[key]
	if current != action || action.sending || action.completed {
		sum.mu.Unlock()
		return
	}
	action.sending = true
	sum.mu.Unlock()

	err := action.target.Send(action.message)

	sum.mu.Lock()
	current = sum.outboundActions[key]
	if current != action || action.completed {
		sum.mu.Unlock()
		return
	}
	action.sending = false
	if err == nil {
		action.completed = true
		cancel := action.cancel
		action.cancel = nil
		sum.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		action.onSuccess()
		sum.mu.Lock()
		if sum.outboundActions[key] == action {
			delete(sum.outboundActions, key)
		}
		sum.mu.Unlock()
		return
	}

	attempt := action.attempt
	action.attempt++
	action.generation++
	generation := action.generation
	delay := sum.retryDelay(attempt)
	scheduler := sum.retryScheduler
	sum.mu.Unlock()

	slog.Warn("Scheduling outbound retry", "client_id", key.clientID, "round", key.round, "phase", key.phase, "attempt", attempt+1, "delay", delay, "err", err)
	cancel := scheduler(delay, func() {
		sum.retryOutbound(key, action, generation)
	})
	sum.mu.Lock()
	if sum.outboundActions[key] == action && !action.completed && action.generation == generation {
		action.cancel = cancel
		sum.mu.Unlock()
		return
	}
	sum.mu.Unlock()
	cancel()
}

func (sum *Sum) retryOutbound(key outboundKey, action *outboundAction, generation uint64) {
	sum.mu.Lock()
	if sum.outboundActions[key] != action || action.completed || action.generation != generation {
		sum.mu.Unlock()
		return
	}
	action.cancel = nil
	sum.mu.Unlock()
	sum.attemptOutbound(key, action)
}
