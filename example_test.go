package sseeventbus_test

import (
	"context"
	"fmt"

	"github.com/ralscha/sse-eventbus-go"
)

type exampleConnection struct{}

func (exampleConnection) Send(message sseeventbus.Message) error {
	fmt.Printf("%s: %s\n", message.Event, message.Data)
	return nil
}

func (exampleConnection) Close() error { return nil }

func Example() {
	bus, err := sseeventbus.New(
		sseeventbus.WithSynchronousDelivery(),
		sseeventbus.WithoutClientExpiration(),
	)
	if err != nil {
		panic(err)
	}
	defer func() { _ = bus.Close(context.Background()) }()

	if err := bus.Register("client-1", exampleConnection{}, sseeventbus.SubscribeTo("orders")); err != nil {
		panic(err)
	}
	if err := bus.Publish(context.Background(), sseeventbus.NewNamedEventWithData("orders", "ready")); err != nil {
		panic(err)
	}

	// Output:
	// orders: ready
}
