package bridge

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/lib/pq" // PostgreSQL driver
)

// PostgresConfig represents the config of the PostgreSQL client
type PostgresConfig struct {
	DSN string `yaml:"dsn"`
}

// PGClient represents a PostgreSQL client
type PGClient struct {
	config PostgresConfig
	db     *sql.DB
}

// prepareInsert prepares a single statement per table name
func (c *PGClient) prepareInsert(table string, columns []string) (*sql.Stmt, error) {
	nColumns := len(columns)
	values := make([]string, nColumns)
	for i := 0; i < nColumns; i++ {
		values[i] = fmt.Sprintf("$%d", i+1)
	}

	sql := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		table,
		strings.Join(columns, ", "),
		strings.Join(values, ", "),
	)

	var err error
	stmt, err := c.db.Prepare(sql)
	if err != nil {
		return nil, err
	}

	return stmt, nil
}

// Insert the given values into the given table
func (c *PGClient) Insert(table string, message *Message) error {
	messageLength := len(*message)
	columns := make([]string, messageLength)
	values := make([]interface{}, messageLength)

	i := 0
	for k, v := range *message {
		columns[i] = k
		values[i] = v

		i++
	}

	stmt, err := c.prepareInsert(table, columns)
	if err != nil {
		return err
	}

	_, err = stmt.Exec(values...)
	stmt.Close()

	if err != nil {
		return err
	}

	return nil
}

// Close disconnects the client from the server
func (c *PGClient) Close() error {
	log.Printf("Disconnecting from PostgreSQL")

	if err := c.db.Close(); err != nil {
		return fmt.Errorf("PostgreSQL connection close error: %s", err)
	}

	defer log.Printf("PostgreSQL disconnect OK")

	return nil
}

// NewPGClient constructs a new PGClient
func NewPGClient(config PostgresConfig) (*PGClient, error) {
	db, err := sql.Open("postgres", config.DSN)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	pg := &PGClient{
		config: config,
		db:     db,
	}

	return pg, nil
}
