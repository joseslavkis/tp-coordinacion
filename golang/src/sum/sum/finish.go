package sum

import (
	"fmt"
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
		progress = sum.newFinishProgressLocked(key.clientID, 1)
		sum.finishRounds[key] = progress
	}
	sum.mu.Unlock()
	return sum.completeFinish(key, progress)
}

func (sum *Sum) handleFinishToken(envelope inner.Envelope) error {
	key := tokenKey{clientID: envelope.ClientID, leaderID: envelope.LeaderID, round: envelope.Round}

	sum.mu.Lock()
	if sum.hasFinishedClientLocked(envelope.ClientID) {
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
		progress = sum.newFinishProgressLocked(envelope.ClientID, envelope.Visited+1)
		sum.finishRounds[key] = progress
	}
	sum.mu.Unlock()
	return sum.completeFinish(key, progress)
}

func (sum *Sum) newFinishProgressLocked(clientID string, visited uint64) *finishRoundProgress {
	progress := &finishRoundProgress{
		partials:        make([]partialProgress, sum.aggregationAmount),
		outboundVisited: visited,
	}
	for _, record := range copyFruitRecords(sum.fruitItemMap[clientID]) {
		aggregationID := aggregationFor(clientID, record.Fruit, sum.aggregationAmount)
		progress.partials[aggregationID].records = append(progress.partials[aggregationID].records, record)
	}
	return progress
}

func (sum *Sum) completeFinish(key tokenKey, progress *finishRoundProgress) error {
	if err := sum.publishPartials(key, progress); err != nil {
		return err
	}
	return sum.forwardFinish(key, progress)
}

func (sum *Sum) publishPartials(key tokenKey, progress *finishRoundProgress) error {
	if len(progress.partials) != sum.aggregationAmount || sum.aggregationAmount <= 0 {
		return fmt.Errorf("invalid PARTIAL shard count for %s", key.clientID)
	}
	for aggregationID := range progress.partials {
		if err := sum.publishPartialTo(key, progress, aggregationID); err != nil {
			return err
		}
	}

	sum.mu.Lock()
	defer sum.mu.Unlock()
	if sum.finishRounds[key] != progress || progress.partialPublished {
		return nil
	}
	for _, partial := range progress.partials {
		if !partial.published {
			return fmt.Errorf("PARTIAL still sending for %s", key.clientID)
		}
	}
	delete(sum.fruitItemMap, key.clientID)
	delete(sum.processedCount, key.clientID)
	progress.partialPublished = true
	return nil
}

func (sum *Sum) publishPartialTo(key tokenKey, progress *finishRoundProgress, aggregationID int) error {
	sum.mu.Lock()
	current := sum.finishRounds[key]
	if current != progress {
		sum.mu.Unlock()
		return nil
	}
	partial := &progress.partials[aggregationID]
	if partial.published {
		sum.mu.Unlock()
		return nil
	}
	if partial.sending {
		sum.mu.Unlock()
		return fmt.Errorf("PARTIAL already sending for %s to aggregation %d", key.clientID, aggregationID)
	}
	partial.sending = true
	records := append([]fruititem.FruitItem(nil), partial.records...)
	sum.mu.Unlock()

	message, err := inner.SerializePartialMessage(key.clientID, sum.id, records)
	if err != nil {
		slog.Error("Discarding invalid PARTIAL state", "client_id", key.clientID, "round", key.round, "err", err)
	} else {
		err = sum.outputExchange.SendTo(fmt.Sprintf("%s_%d", sum.aggregationPrefix, aggregationID), *message)
	}

	sum.mu.Lock()
	if current := sum.finishRounds[key]; current == progress {
		partial.sending = false
		if err == nil {
			partial.published = true
			partial.records = nil
		}
	}
	sum.mu.Unlock()
	return err
}

func (sum *Sum) forwardFinish(key tokenKey, progress *finishRoundProgress) error {
	sum.mu.Lock()
	current := sum.finishRounds[key]
	if current != progress || progress.forwarded {
		sum.mu.Unlock()
		return nil
	}
	if progress.forwardSending {
		sum.mu.Unlock()
		return fmt.Errorf("FINISH already sending for %s", key.clientID)
	}
	progress.forwardSending = true
	visited := progress.outboundVisited
	sum.mu.Unlock()

	message, err := inner.SerializeFinishMessage(key.clientID, key.leaderID, key.round, visited)
	if err != nil {
		slog.Error("Discarding invalid FINISH state", "client_id", key.clientID, "round", key.round, "err", err)
	} else {
		err = sum.controlOutput.Send(*message)
	}

	var cancel countRetryCancel
	sum.mu.Lock()
	if current := sum.finishRounds[key]; current == progress {
		progress.forwardSending = false
		if err == nil {
			progress.forwarded = true
			if key.leaderID != sum.id {
				cancel = sum.removeClientStateLocked(key.clientID)
			}
		}
	}
	sum.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	return err
}
