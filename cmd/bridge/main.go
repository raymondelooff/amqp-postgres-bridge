package main

import (
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/getsentry/raven-go"
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

	raven.SetDSN(c.SentryDsn)
	raven.SetEnvironment(c.Env)

	b := bridge.NewBridge(c)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		exit := make(chan os.Signal, 1)
		signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

		<-exit

		log.Println("shutting down")
		wg.Done()
	}()

	b.Run(&wg)
	log.Println("shutdown successfull")
}
