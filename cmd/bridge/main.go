package main

import (
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/raymondelooff/amqp-postgres-bridge/bridge"
	"gopkg.in/yaml.v2"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("error: config file location not specified")
	}

	f, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	c := bridge.Config{}
	err = yaml.Unmarshal(f, &c)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	err = sentry.Init(sentry.ClientOptions{
		Dsn:         c.SentryDsn,
		Environment: c.Env,
	})
	if err != nil {
		log.Fatalf("sentry.Init: %s", err)
	}
	// Flush buffered events before the program terminates.
	// Set the timeout to the maximum duration the program can afford to wait.
	defer sentry.Flush(2 * time.Second)

	b, err := bridge.NewBridge(c)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		exit := make(chan os.Signal, 1)
		signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

		<-exit

		log.Println("Bridge shutting down")
		wg.Done()
	}()

	b.Run(&wg)
	log.Println("Bridge shutdown OK")
}
