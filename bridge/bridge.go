package bridge

import (
	"context"
)

// Config struct represents the YAML file specification
type Config struct {
	Env       string
	SentryDsn string `yaml:"sentry_dsn"`
	AMQP      AMQPConfig
	Topics    []string
	Mappings  []mappingConfig
}

type mappingConfig struct {
	Topic  string
	Table  string
	Values map[string]string
}

// Bridge represents a Bridge instance
type Bridge struct {
	config     Config
	subscriber *Subscriber
}

// Run executes the Bridge
func (b *Bridge) Run(ctx context.Context) {
	messages := b.subscriber.Subscribe()
	defer b.subscriber.Shutdown()

	go func() {
		for message := range messages {
			message.Ack(true)
		}
	}()

	<-ctx.Done()
}

// NewBridge constructs a new Bridge
func NewBridge(config Config) *Bridge {
	s := NewSubscriber(config.AMQP, config.Topics)

	b := Bridge{
		config:     config,
		subscriber: s,
	}

	return &b
}
