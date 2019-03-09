package main

import (
	"io/ioutil"
	"log"
	"os"

	"github.com/raymondelooff/amqp-postgres-bridge/bridge"
	"gopkg.in/yaml.v2"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("error: config file location not specified")
	}

	if len(os.Args) < 3 {
		log.Fatalf("error: table not specified")
	}

	if len(os.Args) < 4 {
		log.Fatalf("error: JSON data not specified")
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

	m := bridge.NewMapper(c.Mapper)
	p := bridge.NewPGClient(c.Postgres)
	defer p.Close()

	table, message, err := m.Map(os.Args[2], os.Args[3])
	if err != nil {
		log.Fatalf("could not map data: %v", err)
	}

	if err := p.Insert(table, message); err != nil {
		log.Fatalf("could not insert row: %v", err)
	}

	log.Println("row inserted")
}
