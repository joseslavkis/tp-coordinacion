package sum

import (
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
)

func (sum *Sum) onCountBarrierPassed(key tokenKey) {
	sum.mu.Lock()
	sum.initializeControlStateLocked()
	barrier := sum.barriers[key.clientID]
	if barrier == nil || !barrier.barrierPassed || barrier.round != key.round || barrier.finishStarted {
		sum.mu.Unlock()
		return
	}
	barrier.finishStarted = true
	progress := &finishRoundProgress{
		records:         copyFruitRecords(sum.fruitItemMap[key.clientID]),
		outboundVisited: 1,
	}
	sum.finishRounds[key] = progress
	sum.mu.Unlock()
	sum.publishPartial(key, progress)
}

func (sum *Sum) handleFinishToken(envelope inner.Envelope) {
	key := tokenKey{clientID: envelope.ClientID, leaderID: envelope.LeaderID, round: envelope.Round}
	if envelope.LeaderID == sum.id {
		if envelope.Visited != uint64(sum.sumAmount) {
			slog.Error("Discarding FINISH returned before full traversal", "client_id", envelope.ClientID, "round", envelope.Round, "visited", envelope.Visited)
			return
		}
		sum.mu.Lock()
		barrier := sum.barriers[envelope.ClientID]
		if barrier == nil || !barrier.finishStarted || barrier.round != envelope.Round {
			sum.mu.Unlock()
			slog.Error("Discarding FINISH for unknown leader round", "client_id", envelope.ClientID, "round", envelope.Round)
			return
		}
		barrier.finishReturned = true
		sum.mu.Unlock()
		return
	}
	if envelope.Visited >= uint64(sum.sumAmount) {
		slog.Error("Discarding FINISH with exhausted traversal", "client_id", envelope.ClientID, "round", envelope.Round, "visited", envelope.Visited)
		return
	}

	sum.mu.Lock()
	sum.initializeControlStateLocked()
	if existing := sum.finishRounds[key]; existing != nil {
		if existing.outboundVisited != envelope.Visited+1 {
			slog.Error("Discarding conflicting duplicate FINISH", "client_id", envelope.ClientID, "round", envelope.Round)
		}
		sum.mu.Unlock()
		return
	}
	progress := &finishRoundProgress{
		records:         copyFruitRecords(sum.fruitItemMap[envelope.ClientID]),
		outboundVisited: envelope.Visited + 1,
	}
	sum.finishRounds[key] = progress
	sum.mu.Unlock()
	sum.publishPartial(key, progress)
}

func (sum *Sum) publishPartial(key tokenKey, progress *finishRoundProgress) {
	message, err := inner.SerializePartialMessage(key.clientID, sum.id, key.round, progress.records)
	if err != nil {
		slog.Error("While serializing PARTIAL", "client_id", key.clientID, "round", key.round, "err", err)
		return
	}
	actionKey := outboundKey{tokenKey: key, nodeID: sum.id, phase: "partial"}
	sum.dispatchOutbound(actionKey, sum.outputExchange, *message, func() {
		sum.mu.Lock()
		if sum.finishRounds[key] != progress || progress.partialPublished {
			sum.mu.Unlock()
			return
		}
		progress.partialPublished = true
		sum.mu.Unlock()
		sum.publishSumDone(key, progress)
	})
}

func (sum *Sum) publishSumDone(key tokenKey, progress *finishRoundProgress) {
	message, err := inner.SerializeSumDoneMessage(key.clientID, sum.id, key.round)
	if err != nil {
		slog.Error("While serializing SUM_DONE", "client_id", key.clientID, "round", key.round, "err", err)
		return
	}
	actionKey := outboundKey{tokenKey: key, nodeID: sum.id, phase: "sum_done"}
	sum.dispatchOutbound(actionKey, sum.outputExchange, *message, func() {
		sum.mu.Lock()
		if sum.finishRounds[key] != progress || progress.donePublished {
			sum.mu.Unlock()
			return
		}
		progress.donePublished = true
		delete(sum.fruitItemMap, key.clientID)
		delete(sum.processedCount, key.clientID)
		delete(sum.dataPublished, key.clientID)
		sum.mu.Unlock()
		sum.forwardFinish(key, progress)
	})
}

func (sum *Sum) forwardFinish(key tokenKey, progress *finishRoundProgress) {
	message, err := inner.SerializeFinishMessage(key.clientID, key.leaderID, key.round, progress.outboundVisited)
	if err != nil {
		slog.Error("While serializing FINISH", "client_id", key.clientID, "round", key.round, "err", err)
		return
	}
	actionKey := outboundKey{tokenKey: key, nodeID: sum.id, phase: "finish"}
	sum.dispatchOutbound(actionKey, sum.controlOutput, *message, func() {
		sum.mu.Lock()
		if sum.finishRounds[key] == progress {
			progress.forwarded = true
		}
		sum.mu.Unlock()
	})
}
