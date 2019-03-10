package bridge

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

// AMQPConfig represents the config of the Subscriber
type AMQPConfig struct {
	Tag      string `yaml:"tag"`
	Exchange string `yaml:"exchange"`
	DSN      string `yaml:"dsn"`
	TLS      bool   `yaml:"tls"`
}

// Subscriber represents an AMQP subscriber
type Subscriber struct {
	config  AMQPConfig
	topics  []string
	tag     string
	conn    *amqp.Connection
	channel *amqp.Channel
	done    chan error
}

// Connect establishes a connection to the broker and declares the queue
func (s *Subscriber) connect() *amqp.Queue {
	var err error

	if s.config.TLS == true {
		s.conn, err = amqp.DialTLS(s.config.DSN, nil)
	} else {
		s.conn, err = amqp.Dial(s.config.DSN)
	}
	if err != nil {
		log.Fatalf("connection: %v", err)
	}

	log.Printf("got Connection, getting Channel")
	s.channel, err = s.conn.Channel()
	if err != nil {
		log.Fatalf("Channel: %v", err)
	}

	var queueName = fmt.Sprintf("amqp-postgres-bridge-%s", s.tag)
	log.Printf("declared Exchange, declaring Queue %q", queueName)
	queue, err := s.channel.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // autoDelete
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		log.Fatalf("Queue declare: %v", err)
	}

	log.Printf("declared Queue %q (%d messages, %d consumers)",
		queue.Name, queue.Messages, queue.Consumers)

	return &queue
}

// Subscribe to the topics defined in the AMQPConfig
func (s *Subscriber) Subscribe() <-chan amqp.Delivery {
	var err error

	queue := s.connect()

	log.Printf("binding %d topics to Exchange", len(s.topics))
	for _, topic := range s.topics {
		log.Printf("binding topic to Exchange (key: %q)", topic)
		err = s.channel.QueueBind(
			queue.Name,        // name
			topic,             // key
			s.config.Exchange, // exchange
			false,             // noWait
			nil,               // arguments
		)
		if err != nil {
			log.Fatalf("Queue bind: %v", err)
		}
	}

	log.Printf("Queue bound to Exchange, starting Consume (consumer tag %q)", s.tag)
	deliveries, err := s.channel.Consume(
		queue.Name, // queue
		s.tag,      // consumer,
		false,      // autoAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		log.Fatalf("Queue consume: %v", err)
	}

	return deliveries
}

// Shutdown the Subscriber
func (s *Subscriber) Shutdown() error {
	log.Printf("Consumer shutting down")

	if err := s.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Printf("Consumer shutdown OK")

	return nil
}

// NewSubscriber constructs a new Subscriber
func NewSubscriber(config AMQPConfig, topics []string) *Subscriber {
	return &Subscriber{
		config:  config,
		topics:  topics,
		tag:     config.Tag,
		conn:    nil,
		channel: nil,
		done:    make(chan error),
	}
}
