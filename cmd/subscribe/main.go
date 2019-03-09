package main

import (
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

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

	s := bridge.NewSubscriber(c.AMQP, c.Topics)

	var wg sync.WaitGroup

	go func() {
		wg.Add(1)

		exit := make(chan os.Signal, 1)
		signal.Notify(exit, syscall.SIGINT, syscall.SIGTERM)

		<-exit

		s.Shutdown()
		wg.Done()
	}()

	messages := s.Subscribe()

	go func() {
		for message := range messages {
			wg.Add(1)

			log.Println(string(message.Body[:]))
			message.Ack(true)

			wg.Done()
		}
	}()

	wg.Wait()
}
