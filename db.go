package main

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

const TABLE = "transactions"

type PostgresDBParams struct {
	dbName   string
	host     string
	user     string
	password string
}

type PostgresTransactionLogger struct {
	events chan<- Event
	errors <-chan error
	db     *sql.DB
}

func NewPostgresTransactionLogger(config PostgresDBParams) (TransactionLogger, error) {
	connStr := fmt.Sprintf(
		"host=%s dbname=%s user=%s password=%s",
		config.host, config.dbName, config.user, config.password,
	)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to open db connection: %w", err)
	}

	logger := &PostgresTransactionLogger{db: db}

	exists, err := logger.verifyTableExists()
	if err != nil {
		return nil, fmt.Errorf("failed to verify table exists: %w", err)
	}
	if !exists {
		if err = logger.createTable(); err != nil {
			return nil, fmt.Errorf("failed to create table: %w", err)
		}
	}

	return logger, nil
}

func (l *PostgresTransactionLogger) WritePut(key, value string) {
	l.events <- Event{EventType: EventPut, Key: key, Value: value}
}

func (l *PostgresTransactionLogger) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, Key: key}
}

func (l *PostgresTransactionLogger) Err() <-chan error {
	return l.errors
}

func (l *PostgresTransactionLogger) verifyTableExists() (bool, error) {
	var result string

	query := fmt.Sprintf("SELECT to_regclass('public.%s');", TABLE)
	rows, err := l.db.Query(query)
	defer rows.Close()
	if err != nil {
		return false, err
	}

	for rows.Next() && result != TABLE {
		rows.Scan(&result)
	}

	return result == TABLE, rows.Err()
}

func (l *PostgresTransactionLogger) createTable() error {
	var err error

	createCommand := `CREATE TABLE %s(
		sequence BIGSERIAL PRIMARY KEY,
		event_type SMALLINT,
		key TEXT,
		value TEXT,
	);`

	createQuery := fmt.Sprintf(createCommand, TABLE)

	_, err = l.db.Exec(createQuery)

	return err
}

func (l *PostgresTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	outEvent := make(chan Event)
	outError := make(chan error, 1)

	go func() {
		defer close(outEvent)
		defer close(outError)

		queryStr := `SELECT sequence, event_type, key, value FROM %s ORDER BY sequence`
		query := fmt.Sprintf(queryStr, TABLE)

		rows, err := l.db.Query(query)
		if err != nil {
			outError <- fmt.Errorf("sql query error: %w", err)
			return
		}
		defer rows.Close()

		e := Event{}

		for rows.Next() {
			err = rows.Scan(&e.Sequence, &e.EventType, &e.Key, &e.Value)

			if err != nil {
				outError <- fmt.Errorf("error reading row: %w", err)
				return
			}

			outEvent <- e
		}

		err = rows.Err()
		if err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
		}
	}()

	return outEvent, outError
}

func (l *PostgresTransactionLogger) Run() {
	events := make(chan Event, 16)
	errors := make(chan error, 1)

	l.events = events
	l.errors = errors

	go func() {
		queryStr := `INSERT INTO %s
			(event_type, key, value)
			VALUES ($1, $2, $3)
		`
		query := fmt.Sprintf(queryStr, TABLE)

		for e := range events {
			_, err := l.db.Exec(
				query,
				e.EventType, e.Key, e.Value,
			)

			if err != nil {
				errors <- err
			}
		}
	}()
}
