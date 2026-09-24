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
	countRetryBaseDelay          = 25 * time.Millisecond
	countRetryMaxDelay           = time.Second
	completedClientTTL           = 5 * time.Minute
	completedClientSweepInterval = 100
)

type countRetryDelayFunc func(attempt uint) time.Duration
type countRetryCancel func()
type countRetryScheduler func(delay time.Duration, callback func()) countRetryCancel

type tokenKey struct {
	clientID string
	leaderID int
	round    uint64
}

type clientBarrierState struct {
	expected          uint64
	leaderID          int
	round             uint64
	lastObserved      uint64
	incompleteAttempt uint
	timerGeneration   uint64
	timerPending      bool
	timerCancel       countRetryCancel
	barrierPassed     bool
	finishStarted     bool
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
	forwarded        bool
}

func controlQueueName(prefix string, id int) string {
	return fmt.Sprintf("%s_control_%d", prefix, id)
}

func successorID(id, sumAmount int) int {
	return (id + 1) % sumAmount
}

func jitteredBackoff(attempt uint, sample uint64) time.Duration {
	bound := countRetryBaseDelay
	for step := uint(0); step < attempt && bound < countRetryMaxDelay; step++ {
		if bound > countRetryMaxDelay/2 {
			bound = countRetryMaxDelay
			break
		}
		bound *= 2
	}
	if bound > countRetryMaxDelay {
		bound = countRetryMaxDelay
	}
	floor := bound / 2
	span := uint64(bound-floor) + 1
	return floor + time.Duration(sample%span)
}

func defaultCountRetryDelay(attempt uint) time.Duration {
	return jitteredBackoff(attempt, rand.Uint64())
}

func defaultCountRetryScheduler(delay time.Duration, callback func()) countRetryCancel {
	timer := time.AfterFunc(delay, callback)
	return func() { timer.Stop() }
}

func (sum *Sum) startCountRound(clientID string) error {
	sum.mu.Lock()
	barrier := sum.barriers[clientID]
	if barrier == nil || barrier.barrierPassed || barrier.timerPending {
		sum.mu.Unlock()
		return nil
	}

	if barrier.round > 0 {
		key := tokenKey{clientID: clientID, leaderID: sum.id, round: barrier.round}
		progress := sum.countRounds[key]
		if progress != nil && !progress.returned {
			if progress.forwarded {
				sum.mu.Unlock()
				return nil
			}
			sum.mu.Unlock()
			return sum.forwardCount(key, progress)
		}
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

	return sum.forwardCount(key, progress)
}

func (sum *Sum) forwardCount(key tokenKey, progress *countRoundProgress) error {
	message, err := inner.SerializeCountMessage(key.clientID, key.leaderID, key.round, progress.expected, progress.outboundCount, progress.outboundVisited)
	if err != nil {
		slog.Error("Discarding invalid COUNT state", "client_id", key.clientID, "round", key.round, "err", err)
		return nil
	}
	if err := sum.controlOutput.Send(*message); err != nil {
		return err
	}
	sum.mu.Lock()
	if current := sum.countRounds[key]; current == progress {
		current.forwarded = true
	}
	sum.mu.Unlock()
	return nil
}

func (sum *Sum) handleControlMessage(msg middleware.Message, ack func(), nack func()) {
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
	if sum.sumAmount <= 0 || envelope.LeaderID < 0 || envelope.LeaderID >= sum.sumAmount || envelope.Visited == 0 || envelope.Visited > uint64(sum.sumAmount) {
		slog.Error("Discarding invalid Sum control metadata", "client_id", envelope.ClientID, "leader_id", envelope.LeaderID, "visited", envelope.Visited, "sum_amount", sum.sumAmount)
		ack()
		return
	}

	switch envelope.Type {
	case inner.MessageTypeCount:
		err = sum.handleCountToken(envelope)
	case inner.MessageTypeFinish:
		err = sum.handleFinishToken(envelope)
	}
	if err != nil {
		slog.Error("While forwarding Sum control message", "type", envelope.Type, "client_id", envelope.ClientID, "round", envelope.Round, "err", err)
		nack()
		return
	}
	ack()
}

func (sum *Sum) handleCountToken(envelope inner.Envelope) error {
	sum.mu.Lock()
	if sum.hasFinishedClientLocked(envelope.ClientID) {
		sum.mu.Unlock()
		slog.Error("Discarding late COUNT for completed client", "client_id", envelope.ClientID, "round", envelope.Round)
		return nil
	}
	sum.mu.Unlock()
	if envelope.Count > envelope.TotalMessages {
		slog.Error("Discarding COUNT that exceeds expected messages", "client_id", envelope.ClientID, "round", envelope.Round, "count", envelope.Count, "expected", envelope.TotalMessages)
		return nil
	}
	key := tokenKey{clientID: envelope.ClientID, leaderID: envelope.LeaderID, round: envelope.Round}
	if envelope.LeaderID == sum.id {
		return sum.handleReturnedCount(key, envelope)
	}
	if envelope.Visited >= uint64(sum.sumAmount) {
		slog.Error("Discarding COUNT with exhausted traversal", "client_id", envelope.ClientID, "round", envelope.Round, "visited", envelope.Visited)
		return nil
	}

	sum.mu.Lock()
	progress, exists := sum.countRounds[key]
	if exists {
		if progress.expected != envelope.TotalMessages || progress.incomingCount != envelope.Count || progress.incomingVisited != envelope.Visited {
			sum.mu.Unlock()
			slog.Error("Discarding conflicting duplicate COUNT", "client_id", envelope.ClientID, "round", envelope.Round)
			return nil
		}
		if progress.forwarded {
			sum.mu.Unlock()
			return nil
		}
		sum.mu.Unlock()
		return sum.forwardCount(key, progress)
	}

	localCount := sum.processedCount[envelope.ClientID]
	if localCount > math.MaxUint64-envelope.Count {
		sum.mu.Unlock()
		slog.Error("Discarding COUNT because the accumulated value overflows", "client_id", envelope.ClientID, "round", envelope.Round)
		return nil
	}
	outboundCount := envelope.Count + localCount
	if outboundCount > envelope.TotalMessages {
		sum.mu.Unlock()
		slog.Error("Discarding COUNT that exceeds expected messages after local contribution", "client_id", envelope.ClientID, "round", envelope.Round, "count", outboundCount, "expected", envelope.TotalMessages)
		return nil
	}
	outboundVisited := envelope.Visited + 1
	progress = &countRoundProgress{
		expected:        envelope.TotalMessages,
		incomingCount:   envelope.Count,
		incomingVisited: envelope.Visited,
		outboundCount:   outboundCount,
		outboundVisited: outboundVisited,
	}
	sum.countRounds[key] = progress
	sum.mu.Unlock()

	return sum.forwardCount(key, progress)
}

func (sum *Sum) handleReturnedCount(key tokenKey, envelope inner.Envelope) error {
	if envelope.Visited != uint64(sum.sumAmount) {
		slog.Error("Discarding COUNT returned before full traversal", "client_id", envelope.ClientID, "round", envelope.Round, "visited", envelope.Visited)
		return nil
	}

	sum.mu.Lock()
	barrier := sum.barriers[envelope.ClientID]
	progress := sum.countRounds[key]
	if barrier == nil || barrier.leaderID != sum.id || barrier.expected != envelope.TotalMessages || barrier.round != envelope.Round || progress == nil {
		sum.mu.Unlock()
		slog.Error("Discarding COUNT for unknown leader round", "client_id", envelope.ClientID, "round", envelope.Round)
		return nil
	}
	if progress.returned {
		if barrier.barrierPassed && envelope.Count == barrier.expected {
			sum.mu.Unlock()
			return sum.onCountBarrierPassed(key)
		}
		sum.mu.Unlock()
		return nil
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
		return sum.onCountBarrierPassed(key)
	}
	if envelope.Count > barrier.expected {
		sum.mu.Unlock()
		slog.Error("Discarding COUNT that exceeds expected messages at leader", "client_id", envelope.ClientID, "round", envelope.Round, "count", envelope.Count, "expected", barrier.expected)
		return nil
	}
	if envelope.Count > barrier.lastObserved {
		barrier.lastObserved = envelope.Count
		barrier.incompleteAttempt = 0
	} else {
		barrier.incompleteAttempt++
	}
	attempt := barrier.incompleteAttempt
	sum.mu.Unlock()
	sum.scheduleCountRetry(envelope.ClientID, key.round, attempt)
	return nil
}

func (sum *Sum) scheduleCountRetry(clientID string, previousRound uint64, attempt uint) {
	sum.mu.Lock()
	barrier := sum.barriers[clientID]
	if barrier == nil || barrier.barrierPassed || barrier.round != previousRound || barrier.timerPending {
		sum.mu.Unlock()
		return
	}
	barrier.timerGeneration++
	generation := barrier.timerGeneration
	barrier.timerPending = true
	delay := sum.countRetryDelay(attempt)
	scheduler := sum.countRetryTimer
	expectedBarrier := barrier
	sum.mu.Unlock()

	cancel := scheduler(delay, func() {
		sum.retryCountRound(clientID, previousRound, generation, expectedBarrier)
	})
	sum.mu.Lock()
	barrier = sum.barriers[clientID]
	if barrier == expectedBarrier && barrier.round == previousRound && barrier.timerPending && barrier.timerGeneration == generation {
		barrier.timerCancel = cancel
		sum.mu.Unlock()
		return
	}
	sum.mu.Unlock()
	cancel()
}

func (sum *Sum) retryCountRound(clientID string, previousRound, generation uint64, expectedBarrier *clientBarrierState) {
	sum.mu.Lock()
	barrier := sum.barriers[clientID]
	if barrier != expectedBarrier || barrier.barrierPassed || barrier.round != previousRound || !barrier.timerPending || barrier.timerGeneration != generation {
		sum.mu.Unlock()
		return
	}
	barrier.timerPending = false
	barrier.timerCancel = nil
	sum.mu.Unlock()

	if err := sum.startCountRound(clientID); err != nil {
		slog.Warn("While sending COUNT retry", "client_id", clientID, "err", err)
		sum.mu.Lock()
		barrier = sum.barriers[clientID]
		if barrier == nil || barrier.barrierPassed {
			sum.mu.Unlock()
			return
		}
		currentRound := barrier.round
		attempt := barrier.incompleteAttempt + 1
		sum.mu.Unlock()
		sum.scheduleCountRetry(clientID, currentRound, attempt)
	}
}

func (sum *Sum) removeClientStateLocked(clientID string) countRetryCancel {
	var cancel countRetryCancel
	if barrier := sum.barriers[clientID]; barrier != nil {
		barrier.timerGeneration++
		cancel = barrier.timerCancel
	}
	delete(sum.fruitItemMap, clientID)
	delete(sum.processedCount, clientID)
	delete(sum.barriers, clientID)
	for key := range sum.countRounds {
		if key.clientID == clientID {
			delete(sum.countRounds, key)
		}
	}
	for key := range sum.finishRounds {
		if key.clientID == clientID {
			delete(sum.finishRounds, key)
		}
	}
	sum.recordCompletedClientLocked(clientID)
	return cancel
}

func (sum *Sum) recordCompletedClientLocked(clientID string) {
	sum.completedClients[clientID] = time.Now()
	sum.completedSinceSweep++
	if sum.completedSinceSweep >= completedClientSweepInterval {
		sum.sweepCompletedClientsLocked(time.Now())
	}
}

func (sum *Sum) sweepCompletedClientsLocked(now time.Time) {
	for clientID, completedAt := range sum.completedClients {
		if now.Sub(completedAt) >= completedClientTTL {
			delete(sum.completedClients, clientID)
		}
	}
	sum.completedSinceSweep = 0
}

func (sum *Sum) hasFinishedClientLocked(clientID string) bool {
	completedAt, completed := sum.completedClients[clientID]
	if !completed {
		return false
	}
	if time.Since(completedAt) >= completedClientTTL {
		delete(sum.completedClients, clientID)
		return false
	}
	return true
}
