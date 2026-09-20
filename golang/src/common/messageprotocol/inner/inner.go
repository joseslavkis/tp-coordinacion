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
	MessageTypeData   MessageType = "data"
	MessageTypeEOF    MessageType = "eof"
	MessageTypeResult MessageType = "result"
)

type Envelope struct {
	Type     MessageType
	ClientID string
	Records  []fruititem.FruitItem
}

type wireEnvelope struct {
	Type     MessageType       `json:"type"`
	ClientID string            `json:"client_id"`
	Records  []json.RawMessage `json:"records"`
}

func SerializeMessage(messageType MessageType, clientID string, records []fruititem.FruitItem) (*middleware.Message, error) {
	if err := validateEnvelope(messageType, clientID, records); err != nil {
		return nil, err
	}

	wireRecords := make([][2]interface{}, 0, len(records))
	for _, record := range records {
		wireRecords = append(wireRecords, [2]interface{}{record.Fruit, record.Amount})
	}

	body, err := json.Marshal(struct {
		Type     MessageType      `json:"type"`
		ClientID string           `json:"client_id"`
		Records  [][2]interface{} `json:"records"`
	}{
		Type:     messageType,
		ClientID: clientID,
		Records:  wireRecords,
	})
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
	if wire.Records == nil {
		return Envelope{}, errors.New("inner envelope records are required")
	}

	records := make([]fruititem.FruitItem, 0, len(wire.Records))
	for _, rawRecord := range wire.Records {
		record, err := deserializeRecord(rawRecord)
		if err != nil {
			return Envelope{}, err
		}
		records = append(records, record)
	}

	if err := validateEnvelope(wire.Type, wire.ClientID, records); err != nil {
		return Envelope{}, err
	}
	return Envelope{Type: wire.Type, ClientID: wire.ClientID, Records: records}, nil
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

func validateEnvelope(messageType MessageType, clientID string, records []fruititem.FruitItem) error {
	if clientID == "" {
		return errors.New("inner envelope client_id is required")
	}
	switch messageType {
	case MessageTypeData:
		if len(records) == 0 {
			return errors.New("data envelope requires at least one record")
		}
	case MessageTypeEOF:
		if len(records) != 0 {
			return errors.New("eof envelope cannot contain records")
		}
	case MessageTypeResult:
	default:
		return fmt.Errorf("unknown inner message type %q", messageType)
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
