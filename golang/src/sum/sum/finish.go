package sum

import (
	"log/slog"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
)

func (sum *Sum) onCountBarrierPassed(key tokenKey) error {
	sum.mu.Lock()
	barrier := sum.barriers[key.clientID]
	if barrier == nil || !barrier.barrierPassed || barrier.round != key.round {
		sum.mu.Unlock()
		return nil
	}
	progress := sum.finishRounds[key]
	if !barrier.finishStarted {
		barrier.finishStarted = true
		progress = &finishRoundProgress{
			records:         copyFruitRecords(sum.fruitItemMap[key.clientID]),
			outboundVisited: 1,
		}
		sum.finishRounds[key] = progress
	}
	sum.mu.Unlock()
	return sum.completeFinish(key, progress)
}

func (sum *Sum) handleFinishToken(envelope inner.Envelope) error {
	key := tokenKey{clientID: envelope.ClientID, leaderID: envelope.LeaderID, round: envelope.Round}

	sum.mu.Lock()
	if _, completed := sum.completedClients[envelope.ClientID]; completed {
		sum.mu.Unlock()
		return nil
	}
	sum.mu.Unlock()

	if envelope.LeaderID == sum.id {
		if envelope.Visited != uint64(sum.sumAmount) {
			slog.Error("Discarding FINISH returned before full traversal", "client_id", envelope.ClientID, "round", envelope.Round, "visited", envelope.Visited)
			return nil
		}
		sum.mu.Lock()
		barrier := sum.barriers[envelope.ClientID]
		if barrier == nil || !barrier.finishStarted || barrier.round != envelope.Round {
			sum.mu.Unlock()
			slog.Error("Discarding FINISH for unknown leader round", "client_id", envelope.ClientID, "round", envelope.Round)
			return nil
		}
		cancel := sum.removeClientStateLocked(envelope.ClientID)
		sum.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		return nil
	}
	if envelope.Visited >= uint64(sum.sumAmount) {
		slog.Error("Discarding FINISH with exhausted traversal", "client_id", envelope.ClientID, "round", envelope.Round, "visited", envelope.Visited)
		return nil
	}

	sum.mu.Lock()
	progress := sum.finishRounds[key]
	if progress != nil {
		if progress.outboundVisited != envelope.Visited+1 {
			sum.mu.Unlock()
			slog.Error("Discarding conflicting duplicate FINISH", "client_id", envelope.ClientID, "round", envelope.Round)
			return nil
		}
	} else {
		progress = &finishRoundProgress{
			records:         copyFruitRecords(sum.fruitItemMap[envelope.ClientID]),
			outboundVisited: envelope.Visited + 1,
		}
		sum.finishRounds[key] = progress
	}
	sum.mu.Unlock()
	return sum.completeFinish(key, progress)
}

func (sum *Sum) completeFinish(key tokenKey, progress *finishRoundProgress) error {
	if err := sum.publishPartial(key, progress); err != nil {
		return err
	}
	if err := sum.publishSumDone(key, progress); err != nil {
		return err
	}
	return sum.forwardFinish(key, progress)
}

func (sum *Sum) publishPartial(key tokenKey, progress *finishRoundProgress) error {
	sum.mu.Lock()
	current := sum.finishRounds[key]
	if current != progress {
		sum.mu.Unlock()
		return nil
	}
	if progress.partialPublished {
		sum.mu.Unlock()
		return nil
	}
	records := append([]fruititem.FruitItem(nil), progress.records...)
	sum.mu.Unlock()

	message, err := inner.SerializePartialMessage(key.clientID, sum.id, key.round, records)
	if err != nil {
		slog.Error("Discarding invalid PARTIAL state", "client_id", key.clientID, "round", key.round, "err", err)
		return nil
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		return err
	}

	sum.mu.Lock()
	if current := sum.finishRounds[key]; current == progress {
		progress.partialPublished = true
		progress.records = nil
	}
	sum.mu.Unlock()
	return nil
}

func (sum *Sum) publishSumDone(key tokenKey, progress *finishRoundProgress) error {
	sum.mu.Lock()
	current := sum.finishRounds[key]
	if current != progress {
		sum.mu.Unlock()
		return nil
	}
	if progress.donePublished {
		sum.mu.Unlock()
		return nil
	}
	sum.mu.Unlock()

	message, err := inner.SerializeSumDoneMessage(key.clientID, sum.id, key.round)
	if err != nil {
		slog.Error("Discarding invalid SUM_DONE state", "client_id", key.clientID, "round", key.round, "err", err)
		return nil
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		return err
	}

	sum.mu.Lock()
	if current := sum.finishRounds[key]; current == progress {
		progress.donePublished = true
		delete(sum.fruitItemMap, key.clientID)
		delete(sum.processedCount, key.clientID)
	}
	sum.mu.Unlock()
	return nil
}

func (sum *Sum) forwardFinish(key tokenKey, progress *finishRoundProgress) error {
	sum.mu.Lock()
	current := sum.finishRounds[key]
	if current != progress || progress.forwarded {
		sum.mu.Unlock()
		return nil
	}
	visited := progress.outboundVisited
	sum.mu.Unlock()

	message, err := inner.SerializeFinishMessage(key.clientID, key.leaderID, key.round, visited)
	if err != nil {
		slog.Error("Discarding invalid FINISH state", "client_id", key.clientID, "round", key.round, "err", err)
		return nil
	}
	if err := sum.controlOutput.Send(*message); err != nil {
		return err
	}

	var cancel countRetryCancel
	sum.mu.Lock()
	if current := sum.finishRounds[key]; current == progress {
		progress.forwarded = true
		if key.leaderID != sum.id {
			cancel = sum.removeClientStateLocked(key.clientID)
		}
	}
	sum.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return nil
}
