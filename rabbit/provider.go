package rabbit

import (
	"context"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (c *ConnectorImpl) Publish(
	ctx context.Context,
	queryName string,
	body []byte,
	queueOpts ...ConfigOption,
) error {
	conf := defaultPublishConfig(queryName, body)

	for _, opt := range queueOpts {
		conf = opt.apply(conf)
	}

	q, err := c.queueDeclare(conf)
	if err != nil {
		return fmt.Errorf("error with queue declare: %w", err)
	}

	return c.GetChannel().PublishWithContext(
		ctx,
		"",
		q.Name,
		conf.publishConfig.mandatory,
		conf.publishConfig.immediate,
		conf.publishConfig.publishing,
	)
}

func (c *ConnectorImpl) PublishWithNewCh(
	ctx context.Context,
	queryName string,
	body []byte,
	queueOpts ...ConfigOption,
) error {
	conf := defaultPublishConfig(queryName, body)

	for _, opt := range queueOpts {
		conf = opt.apply(conf)
	}

	ch, err := c.connection.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel: %w", err)
	}
	defer func(ch *amqp.Channel) {
		if errCl := ch.Close(); err != nil {
			log.Printf("failed to close channel: %v", errCl)
		}
	}(ch)

	q, err := c.queueDeclareWithCh(ch, conf)
	if err != nil {
		return fmt.Errorf("error with queue declare: %w", err)
	}

	return ch.PublishWithContext(
		ctx,
		"",
		q.Name,
		conf.publishConfig.mandatory,
		conf.publishConfig.immediate,
		conf.publishConfig.publishing,
	)
}
