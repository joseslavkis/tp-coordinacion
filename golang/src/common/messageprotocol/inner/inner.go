package inner

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type MessageType string

const (
	MessageTypeData    MessageType = "data"
	MessageTypeEOF     MessageType = "eof"
	MessageTypePartial MessageType = "partial"
	MessageTypeSumDone MessageType = "sum_done"
	MessageTypeResult  MessageType = "result"
	MessageTypeCount   MessageType = "count"
	MessageTypeFinish  MessageType = "finish"
)

type Envelope struct {
	Type          MessageType
	ClientID      string
	Records       []fruititem.FruitItem
	TotalMessages uint64
	SumID         int
	LeaderID      int
	Round         uint64
	Count         uint64
	Visited       uint64
}

type wireEnvelope struct {
	Type          MessageType        `json:"type"`
	ClientID      string             `json:"client_id"`
	Records       *[]json.RawMessage `json:"records,omitempty"`
	TotalMessages *uint64            `json:"total_messages,omitempty"`
	SumID         *int               `json:"sum_id,omitempty"`
	LeaderID      *int               `json:"leader_id,omitempty"`
	Round         *uint64            `json:"round,omitempty"`
	Count         *uint64            `json:"count,omitempty"`
	Visited       *uint64            `json:"visited,omitempty"`
}

type serializableEnvelope struct {
	Type          MessageType       `json:"type"`
	ClientID      string            `json:"client_id"`
	Records       *[][2]interface{} `json:"records,omitempty"`
	TotalMessages *uint64           `json:"total_messages,omitempty"`
	SumID         *int              `json:"sum_id,omitempty"`
	LeaderID      *int              `json:"leader_id,omitempty"`
	Round         *uint64           `json:"round,omitempty"`
	Count         *uint64           `json:"count,omitempty"`
	Visited       *uint64           `json:"visited,omitempty"`
}

func SerializeMessage(messageType MessageType, clientID string, records []fruititem.FruitItem) (*middleware.Message, error) {
	switch messageType {
	case MessageTypeData:
		return SerializeDataMessage(clientID, records)
	case MessageTypeEOF:
		return SerializeEOFMessage(clientID, 0)
	case MessageTypeResult:
		return SerializeResultMessage(clientID, records)
	default:
		return nil, fmt.Errorf("message type %q requires typed metadata", messageType)
	}
}

func SerializeDataMessage(clientID string, records []fruititem.FruitItem) (*middleware.Message, error) {
	return serializeEnvelope(Envelope{Type: MessageTypeData, ClientID: clientID, Records: records})
}

func SerializeEOFMessage(clientID string, totalMessages uint64) (*middleware.Message, error) {
	return serializeEnvelope(Envelope{Type: MessageTypeEOF, ClientID: clientID, TotalMessages: totalMessages})
}

func SerializePartialMessage(clientID string, sumID int, round uint64, records []fruititem.FruitItem) (*middleware.Message, error) {
	return serializeEnvelope(Envelope{Type: MessageTypePartial, ClientID: clientID, Records: records, SumID: sumID, Round: round})
}

func SerializeSumDoneMessage(clientID string, sumID int, round uint64) (*middleware.Message, error) {
	return serializeEnvelope(Envelope{Type: MessageTypeSumDone, ClientID: clientID, SumID: sumID, Round: round})
}

func SerializeResultMessage(clientID string, records []fruititem.FruitItem) (*middleware.Message, error) {
	return serializeEnvelope(Envelope{Type: MessageTypeResult, ClientID: clientID, Records: records})
}

func SerializeCountMessage(clientID string, leaderID int, round, expected, count, visited uint64) (*middleware.Message, error) {
	return serializeEnvelope(Envelope{
		Type:          MessageTypeCount,
		ClientID:      clientID,
		LeaderID:      leaderID,
		Round:         round,
		TotalMessages: expected,
		Count:         count,
		Visited:       visited,
	})
}

func SerializeFinishMessage(clientID string, leaderID int, round, visited uint64) (*middleware.Message, error) {
	return serializeEnvelope(Envelope{
		Type:     MessageTypeFinish,
		ClientID: clientID,
		LeaderID: leaderID,
		Round:    round,
		Visited:  visited,
	})
}

func serializeEnvelope(envelope Envelope) (*middleware.Message, error) {
	if err := validateEnvelope(envelope); err != nil {
		return nil, err
	}

	wire := serializableEnvelope{Type: envelope.Type, ClientID: envelope.ClientID}
	switch envelope.Type {
	case MessageTypeData, MessageTypePartial, MessageTypeResult:
		records := make([][2]interface{}, 0, len(envelope.Records))
		for _, record := range envelope.Records {
			records = append(records, [2]interface{}{record.Fruit, record.Amount})
		}
		wire.Records = &records
	case MessageTypeEOF:
		wire.TotalMessages = &envelope.TotalMessages
	case MessageTypeSumDone:
		wire.SumID = &envelope.SumID
		wire.Round = &envelope.Round
	case MessageTypeCount:
		wire.LeaderID = &envelope.LeaderID
		wire.Round = &envelope.Round
		wire.TotalMessages = &envelope.TotalMessages
		wire.Count = &envelope.Count
		wire.Visited = &envelope.Visited
	case MessageTypeFinish:
		wire.LeaderID = &envelope.LeaderID
		wire.Round = &envelope.Round
		wire.Visited = &envelope.Visited
	}
	if envelope.Type == MessageTypePartial {
		wire.SumID = &envelope.SumID
		wire.Round = &envelope.Round
	}

	body, err := json.Marshal(wire)
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func DeserializeMessage(message *middleware.Message) (Envelope, error) {
	if message == nil {
		return Envelope{}, errors.New("inner message is nil")
	}

	decoder := json.NewDecoder(bytes.NewBufferString(message.Body))
	decoder.DisallowUnknownFields()
	var wire wireEnvelope
	if err := decoder.Decode(&wire); err != nil {
		return Envelope{}, fmt.Errorf("decode inner envelope: %w", err)
	}
	if err := ensureJSONEnd(decoder); err != nil {
		return Envelope{}, err
	}
	if err := validateWireShape(wire); err != nil {
		return Envelope{}, err
	}

	envelope := Envelope{Type: wire.Type, ClientID: wire.ClientID}
	if wire.Records != nil {
		envelope.Records = make([]fruititem.FruitItem, 0, len(*wire.Records))
		for _, rawRecord := range *wire.Records {
			record, err := deserializeRecord(rawRecord)
			if err != nil {
				return Envelope{}, err
			}
			envelope.Records = append(envelope.Records, record)
		}
	}
	copyMetadata(&envelope, wire)
	if err := validateEnvelope(envelope); err != nil {
		return Envelope{}, err
	}
	return envelope, nil
}

func copyMetadata(envelope *Envelope, wire wireEnvelope) {
	if wire.TotalMessages != nil {
		envelope.TotalMessages = *wire.TotalMessages
	}
	if wire.SumID != nil {
		envelope.SumID = *wire.SumID
	}
	if wire.LeaderID != nil {
		envelope.LeaderID = *wire.LeaderID
	}
	if wire.Round != nil {
		envelope.Round = *wire.Round
	}
	if wire.Count != nil {
		envelope.Count = *wire.Count
	}
	if wire.Visited != nil {
		envelope.Visited = *wire.Visited
	}
}

func validateWireShape(wire wireEnvelope) error {
	records := wire.Records != nil
	total := wire.TotalMessages != nil
	sumID := wire.SumID != nil
	leaderID := wire.LeaderID != nil
	round := wire.Round != nil
	count := wire.Count != nil
	visited := wire.Visited != nil

	valid := false
	switch wire.Type {
	case MessageTypeData, MessageTypeResult:
		valid = records && !total && !sumID && !leaderID && !round && !count && !visited
	case MessageTypeEOF:
		valid = !records && total && !sumID && !leaderID && !round && !count && !visited
	case MessageTypePartial:
		valid = records && !total && sumID && !leaderID && round && !count && !visited
	case MessageTypeSumDone:
		valid = !records && !total && sumID && !leaderID && round && !count && !visited
	case MessageTypeCount:
		valid = !records && total && !sumID && leaderID && round && count && visited
	case MessageTypeFinish:
		valid = !records && !total && !sumID && leaderID && round && !count && visited
	default:
		return fmt.Errorf("unknown inner message type %q", wire.Type)
	}
	if !valid {
		return fmt.Errorf("invalid metadata for inner message type %q", wire.Type)
	}
	return nil
}

func deserializeRecord(rawRecord json.RawMessage) (fruititem.FruitItem, error) {
	decoder := json.NewDecoder(bytes.NewReader(rawRecord))
	decoder.UseNumber()
	var pair []interface{}
	if err := decoder.Decode(&pair); err != nil {
		return fruititem.FruitItem{}, fmt.Errorf("decode inner record: %w", err)
	}
	if len(pair) != 2 {
		return fruititem.FruitItem{}, errors.New("inner record must be a fruit and amount pair")
	}

	fruit, ok := pair[0].(string)
	if !ok {
		return fruititem.FruitItem{}, errors.New("inner record fruit must be a string")
	}
	amountNumber, ok := pair[1].(json.Number)
	if !ok {
		return fruititem.FruitItem{}, errors.New("inner record amount must be an integer")
	}
	amount, err := strconv.ParseUint(amountNumber.String(), 10, 32)
	if err != nil {
		return fruititem.FruitItem{}, errors.New("inner record amount must be an unsigned 32-bit integer")
	}

	return fruititem.FruitItem{Fruit: fruit, Amount: uint32(amount)}, nil
}

func validateEnvelope(envelope Envelope) error {
	if envelope.ClientID == "" {
		return errors.New("inner envelope client_id is required")
	}
	switch envelope.Type {
	case MessageTypeData:
		if len(envelope.Records) == 0 {
			return errors.New("data envelope requires at least one record")
		}
	case MessageTypeEOF, MessageTypeResult:
	case MessageTypePartial, MessageTypeSumDone:
		if envelope.SumID < 0 {
			return errors.New("sum_id cannot be negative")
		}
		if envelope.Round == 0 {
			return errors.New("round must be greater than zero")
		}
	case MessageTypeCount, MessageTypeFinish:
		if envelope.LeaderID < 0 {
			return errors.New("leader_id cannot be negative")
		}
		if envelope.Round == 0 {
			return errors.New("round must be greater than zero")
		}
		if envelope.Visited == 0 {
			return errors.New("visited must be greater than zero")
		}
	default:
		return fmt.Errorf("unknown inner message type %q", envelope.Type)
	}
	return nil
}

func ensureJSONEnd(decoder *json.Decoder) error {
	var extra interface{}
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return errors.New("inner envelope contains multiple JSON values")
	}
	return fmt.Errorf("decode inner envelope suffix: %w", err)
}
