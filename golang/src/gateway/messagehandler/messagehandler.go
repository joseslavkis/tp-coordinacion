package messagehandler

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

const clientIDBytes = 16

type MessageHandler struct {
	clientID      string
	totalMessages uint64
}

func NewMessageHandler() MessageHandler {
	randomID := make([]byte, clientIDBytes)
	if _, err := rand.Read(randomID); err != nil {
		panic(fmt.Sprintf("generate message handler client ID: %v", err))
	}
	return MessageHandler{clientID: hex.EncodeToString(randomID)}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	message, err := inner.SerializeDataMessage(messageHandler.clientID, []fruititem.FruitItem{fruitRecord})
	if err != nil {
		return nil, err
	}
	messageHandler.totalMessages++
	return message, nil
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	return inner.SerializeEOFMessage(messageHandler.clientID, messageHandler.totalMessages)
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	envelope, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}
	if envelope.Type != inner.MessageTypeResult {
		return nil, fmt.Errorf("unexpected inner message type %q", envelope.Type)
	}
	if envelope.ClientID != messageHandler.clientID {
		return nil, nil
	}
	return envelope.Records, nil
}
