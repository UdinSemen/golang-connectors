package rabbit

import (
	"context"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	textContentType = "text/plain"
)

type Config struct {
	Host     string `form:"host"`
	Port     string `form:"port"`
	Username string `form:"username"`
	Password string `form:"password"`
	Path     string `form:"path"`
}

type Connector interface {
	GetConnection() *amqp.Connection
	GetChannel() *amqp.Channel
	CloseConnection() error
	Consume(
		queryName string,
		consumerFunc func(amqp.Delivery),
		opts ...ConfigOption,
	) error
	Publish(
		ctx context.Context,
		queryName string,
		body []byte,
		queueOpts ...ConfigOption,
	) error
}

type ConnectorImpl struct {
	mu         sync.Mutex
	connection *amqp.Connection
	channel    *amqp.Channel
}

func NewConnector(config *Config) (*ConnectorImpl, error) {
	if config == nil {
		return nil, fmt.Errorf("config is empty")
	}

	conn, err := amqp.Dial(fmt.Sprintf(`amqp://%v:%v@%v:%v/%v`,
		config.Username,
		config.Password,
		config.Host,
		config.Port,
		config.Path),
	)
	if err != nil {
		return nil, fmt.Errorf("error with connection to rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("error with opening channel: %w", err)
	}

	return &ConnectorImpl{
		connection: conn,
		channel:    ch,
	}, nil
}

func NewConnectorWithConfig(config *Config, amqpConfig amqp.Config) (*ConnectorImpl, error) {
	if config == nil {
		return nil, fmt.Errorf("config is empty")
	}

	conn, err := amqp.DialConfig(fmt.Sprintf(`amqp://%v:%v@%v:%v/%v`,
		config.Username,
		config.Password,
		config.Host,
		config.Port,
		config.Path),
		amqpConfig,
	)
	if err != nil {
		return nil, fmt.Errorf("error with connection to rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("error with opening channel: %w", err)
	}

	return &ConnectorImpl{
		connection: conn,
		channel:    ch,
	}, nil
}

func (c *ConnectorImpl) GetConnection() *amqp.Connection {
	return c.connection
}

func (c *ConnectorImpl) GetChannel() *amqp.Channel {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.channel
}

func (c *ConnectorImpl) CloseConnection() error {
	return c.connection.Close()
}
