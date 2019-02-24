package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/streadway/amqp"

	"github.com/getsentry/raven-go"
	"gopkg.in/yaml.v2"
)

// Config struct represents the YAML file specification
type Config struct {
	Env       string
	SentryDsn string `yaml:"sentry_dsn"`
	AMQP      amqpConfig
	Topics    []string
	Mappings  []mappingConfig
}

type amqpConfig struct {
	Tag      string
	Exchange string
	DSN      string
	TLS      bool
}

type mappingConfig struct {
	Topic  string
	Table  string
	Values map[string]string
}

// Message represents a AMQP message
type Message struct {
	timestamp   time.Time
	observation string
	value       string
}

// Bridge represents the instance
type Bridge struct {
	config  Config
	tag     string
	conn    *amqp.Connection
	channel *amqp.Channel
	done    chan error
}

func (b *Bridge) connect(config *amqpConfig) <-chan amqp.Delivery {
	var err error

	if config.TLS == true {
		b.conn, err = amqp.DialTLS(config.DSN, nil)
	} else {
		b.conn, err = amqp.Dial(config.DSN)
	}
	handleErrorIfNotNil(err, "connection")

	log.Printf("got Connection, getting Channel")
	b.channel, err = b.conn.Channel()
	handleErrorIfNotNil(err, "channel")

	var queueName = fmt.Sprintf("amqp-postgres-bridge-%s", b.tag)
	log.Printf("declared Exchange, declaring Queue %q", queueName)
	queue, err := b.channel.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // autoDelete
		false,     // exclusive
		false,     // noWait
		nil,       // arguments
	)
	handleErrorIfNotNil(err, "queue declare")

	log.Printf("declared Queue (%q %d messages, %d consumers)",
		queue.Name, queue.Messages, queue.Consumers)

	log.Printf("binding %d topics to Exchange", len(b.config.Topics))
	for _, topic := range b.config.Topics {
		log.Printf("binding topic to Exchange (key: %q)", topic)
		err = b.channel.QueueBind(
			queue.Name,             // name
			topic,                  // key
			b.config.AMQP.Exchange, // exchange
			false,                  // noWait
			nil,                    // arguments
		)
		handleErrorIfNotNil(err, "queue bind")
	}

	log.Printf("Queue bound to Exchange, starting Consume (consumer tag %q)", b.tag)
	deliveries, err := b.channel.Consume(
		queue.Name, // queue
		b.tag,      // consumer,
		false,      // autoAck
		false,      // exclusive
		false,      // noLocal
		false,      // noWait
		nil,        // arguments
	)
	handleErrorIfNotNil(err, "queue consume")

	return deliveries
}

// Run the Bridge
func (b *Bridge) Run() {
	var deliveries = b.connect(&b.config.AMQP)

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

	go handle(deliveries, b.done)

	<-exit
}

// Shutdown the Bridge
func (b *Bridge) Shutdown() error {
	// will close() the deliveries channel
	if err := b.channel.Cancel(b.tag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	if err := b.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	defer log.Printf("AMQP shutdown OK")

	// wait for handle() to exit
	return <-b.done
}

func handle(deliveries <-chan amqp.Delivery, done chan error) {
	log.Printf("start handling deliveries")

	for d := range deliveries {
		log.Printf(
			"got %dB delivery: [%v] %q",
			len(d.Body),
			d.DeliveryTag,
			d.Body,
		)
		d.Ack(true)
	}

	log.Printf("handle: deliveries channel closed")
	done <- nil
}

func handleErrorIfNotNil(err error, prefix string) {
	if err != nil {
		if len(prefix) == 0 {
			prefix = "error"
		}

		log.Fatalf("%s: %v", prefix, err)
	}
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("error: config file location not specified")
	}

	f, err := ioutil.ReadFile(os.Args[1])
	handleErrorIfNotNil(err, "config")

	c := Config{}
	err = yaml.Unmarshal(f, &c)
	handleErrorIfNotNil(err, "config")

	raven.SetDSN(c.SentryDsn)
	raven.SetEnvironment(c.Env)

	b := Bridge{
		config:  c,
		tag:     c.AMQP.Tag,
		conn:    nil,
		channel: nil,
		done:    make(chan error),
	}

	b.Run()

	log.Printf("shutting down")

	if err := b.Shutdown(); err != nil {
		log.Fatalf("error during shutdown: %s", err)
	}
}
