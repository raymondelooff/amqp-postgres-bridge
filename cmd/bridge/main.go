package main

import (
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/getsentry/raven-go"
	"github.com/pkg/profile"
	"github.com/raymondelooff/amqp-postgres-bridge/bridge"
	"gopkg.in/yaml.v2"
)

func main() {
	// Memory profiling
	defer profile.Start(profile.MemProfile).Stop()

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
