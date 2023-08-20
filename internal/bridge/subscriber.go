package bridge

import (
	"fmt"
	"log"
	"sync"

	"github.com/avast/retry-go"
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
	config     AMQPConfig
	topics     []string
	tag        string
	conn       *amqp.Connection
	channel    *amqp.Channel
	deliveries chan amqp.Delivery
	done       chan error
}

// Dial the server
func (s *Subscriber) dial() error {
	var err error

	if s.config.TLS == true {
		s.conn, err = amqp.DialTLS(s.config.DSN, nil)
	} else {
		s.conn, err = amqp.Dial(s.config.DSN)
	}
	if err != nil {
		log.Printf("connection: %v", err)

		return err
	}

	log.Printf("connection: connection established")

	return nil
}

// Connect establishes a connection to the broker and declares the queue
func (s *Subscriber) connect() (*amqp.Queue, error) {
	err := s.dial()
	if err != nil {
		return nil, err
	}

	log.Printf("got Connection, getting Channel")
	s.channel, err = s.conn.Channel()
	if err != nil {
		log.Printf("Channel: %v", err)

		return nil, err
	}

	queueName := fmt.Sprintf("amqp-postgres-bridge-%s", s.tag)
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
		log.Printf("Queue declare: %v", err)

		return nil, err
	}

	log.Printf("declared Queue %q (%d messages, %d consumers)",
		queue.Name, queue.Messages, queue.Consumers)

	return &queue, nil
}

// Subscribe to the topics defined in the AMQPConfig
func (s *Subscriber) Subscribe() chan amqp.Delivery {
	go func() {
		for {
			retry.Do(
				func() error {
					queue, err := s.connect()
					if err != nil {
						return err
					}

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
							log.Printf("Queue bind: %v", err)

							return err
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
						log.Printf("Queue consume: %v", err)

						return err
					}

					for delivery := range deliveries {
						s.deliveries <- delivery
					}

					return nil
				},
			)

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				<-s.conn.NotifyClose(make(chan *amqp.Error))

				log.Printf("connection: closing Connection")
				wg.Done()
			}()

			go func() {
				<-s.channel.NotifyClose(make(chan *amqp.Error))

				log.Printf("channel: closing Channel")
				wg.Done()
			}()

			wg.Wait()
		}
	}()

	return s.deliveries
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
		config:     config,
		topics:     topics,
		tag:        config.Tag,
		conn:       nil,
		channel:    nil,
		deliveries: make(chan amqp.Delivery),
		done:       make(chan error),
	}
}
