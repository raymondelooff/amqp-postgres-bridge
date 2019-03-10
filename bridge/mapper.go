package bridge

import (
	"encoding/json"
	"fmt"
	"log"
	"regexp"
)

// MapperConfig represents the config for a Mapper
type MapperConfig struct {
	Mappings []mapping `yaml:"mappings"`
}

type mapping struct {
	Topic  string            `yaml:"topic"`
	Table  string            `yaml:"table"`
	Values map[string]string `yaml:"values"`
}

// Mapper represents a mapper that is able to convert
// incoming messages to the desired format.
type Mapper struct {
	config  MapperConfig
	regexes []*regexp.Regexp
}

// Initialize regexes from the config spec
func (m *Mapper) Initialize() {
	for i, mapping := range m.config.Mappings {
		var err error

		if len(mapping.Topic) == 0 {
			log.Printf("warning: mapping on index %d does not contain a topic", i)

			continue
		}

		m.regexes[i], err = regexp.Compile(mapping.Topic)
		if err != nil {
			log.Printf("warning: topic of mapping on index %d is not a valid regular expression: %v", i, err)
		}
	}
}

func (m *Mapper) mapKv(k string, v string, data *map[string]interface{}) interface{} {
	vl := len(v)

	if vl == 0 {
		return v
	}

	// Check if the value specification begins and ends with a percentage sign,
	// if so, use this value as a key for the data map.
	if v[0] == '%' && v[vl-1] == '%' {
		// Use the value as a key, strip first and last character
		vk := v[1 : vl-1]

		if val, ok := (*data)[vk]; ok {
			return val
		}

		return nil
	}

	return v
}

// Map the given JSON data into a Message
func (m *Mapper) Map(topic string, jsonData string) (string, *Message, error) {
	var data map[string]interface{}

	if err := json.Unmarshal([]byte(jsonData), &data); err != nil {
		return "", nil, err
	}

	var mapping *mapping
	for i, r := range m.regexes {
		if r == nil {
			continue
		}

		matches := r.FindStringSubmatch(topic)
		if matches == nil {
			continue
		}

		groups := r.SubexpNames()
		for j := 1; j < len(matches); j++ {
			if len(matches[j]) == 0 {
				continue
			}

			// Assign matched value with the group name to the data
			data[groups[j]] = matches[j]
		}

		mapping = &m.config.Mappings[i]
	}

	if mapping == nil {
		return "", nil, fmt.Errorf("mapping not found")
	}

	message := Message{}
	for k, v := range mapping.Values {
		message[k] = m.mapKv(k, v, &data)
	}

	return mapping.Table, &message, nil
}

// NewMapper creates a new Mapper instance
func NewMapper(config MapperConfig) *Mapper {
	m := Mapper{
		config:  config,
		regexes: make([]*regexp.Regexp, len(config.Mappings)),
	}

	m.Initialize()

	return &m
}
