package main

import (
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/raymondelooff/amqp-postgres-bridge/bridge"
	"gopkg.in/yaml.v3"
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

	s := bridge.NewSubscriber(c.AMQP, c.Topics)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		exit := make(chan os.Signal, 1)
		signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

		<-exit

		s.Shutdown()
		wg.Done()
	}()

	deliveries := s.Subscribe()

	go func() {
		for delivery := range deliveries {
			wg.Add(1)

			log.Println(string(delivery.Body[:]))
			delivery.Ack(true)

			wg.Done()
		}
	}()

	wg.Wait()
}
