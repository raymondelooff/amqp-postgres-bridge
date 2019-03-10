package bridge

import (
	"sync"

	"github.com/streadway/amqp"
)

// Config struct represents the YAML file specification
type Config struct {
	Env       string         `yaml:"env"`
	SentryDsn string         `yaml:"sentry_dsn"`
	AMQP      AMQPConfig     `yaml:"amqp"`
	Postgres  PostgresConfig `yaml:"postgres"`
	Topics    []string       `yaml:"topics"`
	Mapper    MapperConfig   `yaml:"mapper"`
}

// Message represents a single (mapped) Message
type Message map[string]interface{}

// Bridge represents a Bridge instance
type Bridge struct {
	config     Config
	mapper     *Mapper
	pgClient   *PGClient
	subscriber *Subscriber
}

// Run executes the Bridge
func (b *Bridge) Run(wg *sync.WaitGroup) {
	deliveries := b.subscriber.Subscribe()
	defer b.subscriber.Shutdown()

	go func() {
		for delivery := range deliveries {
			wg.Add(1)
			b.handleDelivery(wg, &delivery)
		}
	}()

	wg.Wait()
}

func (b *Bridge) handleDelivery(wg *sync.WaitGroup, delivery *amqp.Delivery) {
	var err error

	table, message, err := b.mapper.Map(delivery.RoutingKey, delivery.Body)
	if err != nil {
		delivery.Ack(true)
		wg.Done()

		return
	}

	err = b.pgClient.Insert(table, message)
	if err != nil {
		delivery.Ack(false)
		wg.Done()

		return
	}

	delivery.Ack(true)
	wg.Done()

	return
}

// NewBridge constructs a new Bridge
func NewBridge(config Config) *Bridge {
	m := NewMapper(config.Mapper)
	pg := NewPGClient(config.Postgres)
	s := NewSubscriber(config.AMQP, config.Topics)

	b := Bridge{
		config:     config,
		mapper:     m,
		pgClient:   pg,
		subscriber: s,
	}

	return &b
}
