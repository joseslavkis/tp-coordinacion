package sum

import (
	"errors"
	"fmt"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type aggregationOutputs struct {
	outputs []middleware.Middleware
}

type aggregationOutputFactory func(string, []string, middleware.ConnSettings) (middleware.Middleware, error)

func newAggregationOutputs(prefix string, amount int, settings middleware.ConnSettings) (*aggregationOutputs, error) {
	return newAggregationOutputsWithFactory(prefix, amount, settings, middleware.CreateExchangeMiddleware)
}

func newAggregationOutputsWithFactory(prefix string, amount int, settings middleware.ConnSettings, create aggregationOutputFactory) (*aggregationOutputs, error) {
	if amount <= 0 {
		return nil, errors.New("aggregation amount must be greater than zero")
	}

	result := &aggregationOutputs{outputs: make([]middleware.Middleware, 0, amount)}
	for id := 0; id < amount; id++ {
		output, err := create(prefix, []string{fmt.Sprintf("%s_%d", prefix, id)}, settings)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("create aggregation output %d: %w", id, err), result.Close())
		}
		result.outputs = append(result.outputs, output)
	}
	return result, nil
}

func (outputs *aggregationOutputs) Send(id int, message middleware.Message) error {
	if id < 0 || id >= len(outputs.outputs) {
		return fmt.Errorf("aggregation output id %d is outside [0, %d)", id, len(outputs.outputs))
	}
	return outputs.outputs[id].Send(message)
}

func (outputs *aggregationOutputs) Close() error {
	var closeErrors []error
	for id, output := range outputs.outputs {
		if err := output.Close(); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("close aggregation output %d: %w", id, err))
		}
	}
	return errors.Join(closeErrors...)
}
