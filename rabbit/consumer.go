package rabbit

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (c ConnectorImpl) Consume(
	queryName string,
	consumerFunc func(amqp.Delivery),
	opts ...ConfigOption,
) error {
	conf := defaultConsumerConfig(queryName)

	for _, opt := range opts {
		conf = opt.apply(conf)
	}

	q, err := c.queueDeclare(conf)
	if err != nil {
		return fmt.Errorf("error with queue declare: %w", err)
	}

	if conf.qos != nil {
		if err = c.channel.Qos(
			conf.qos.prefetchCount,
			conf.qos.prefetchSize,
			conf.qos.global,
		); err != nil {
			return fmt.Errorf("error with qos: %w", err)
		}
	}

	delivery, err := c.GetChannel().Consume(
		q.Name,
		conf.consumerConfig.consumer,
		conf.consumerConfig.autoAck,
		conf.queueConfig.exclusive,
		conf.consumerConfig.noLocal,
		conf.queueConfig.noWait,
		conf.queueConfig.args,
	)
	if err != nil {
		return fmt.Errorf("error with consume: %w", err)
	}

	go func() {
		for d := range delivery {
			if conf.publishConfig.publishing.CorrelationId != "" && d.CorrelationId != conf.publishConfig.publishing.CorrelationId {
				continue
			}
			if !conf.synchronous {
				go consumerFunc(d)
			} else {
				consumerFunc(d)
			}
		}
	}()

	return nil
}
