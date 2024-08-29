package rabbit

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type config struct {
	synchronous    bool
	queueConfig    *queueConfig
	consumerConfig *consumerConfig
	qos            *qosConfig
	publishConfig  *providerConfig
}

type queueConfig struct {
	queueName    string
	durableQueue bool
	autoDelete   bool
	exclusive    bool
	noWait       bool
	args         amqp.Table
}

type consumerConfig struct {
	consumer string
	autoAck  bool
	noLocal  bool
}

// providerConfig describes how to publish a message
type providerConfig struct {
	exchange   string
	routingKey string
	mandatory  bool
	immediate  bool
	publishing amqp.Publishing
}

// qosConfig describes how to receive message
type qosConfig struct {
	prefetchCount int
	prefetchSize  int
	global        bool
}

func defaultConfig(queueName string) config {
	defaultConf := config{
		synchronous: false,
	}

	defaultConf.queueConfig = &queueConfig{
		queueName:    queueName,
		durableQueue: false,
		autoDelete:   false,
		exclusive:    false,
		noWait:       false,
		args:         nil,
	}

	return defaultConf
}

func defaultConsumerConfig(queryName string) config {
	conf := defaultConfig(queryName)

	conf.consumerConfig = &consumerConfig{
		consumer: "",
		autoAck:  true,
		noLocal:  false,
	}
	return conf
}

// defaultPublishConfig returns a default publish configuration
//
// default content type is text/plain
func defaultPublishConfig(queryName string, body []byte) config {
	conf := defaultConfig(queryName)
	conf.publishConfig = &providerConfig{
		exchange:   "",
		routingKey: queryName,
		mandatory:  false,
		immediate:  false,
		publishing: amqp.Publishing{
			ContentType: textContentType,
			Body:        body,
		},
	}
	return conf
}

type ConfigOption interface {
	apply(config) config
}

type queueOptionFunc func(config) config

func (fn queueOptionFunc) apply(cfg config) config {
	return fn(cfg)
}
func WithConsumer(consumer string) ConfigOption {
	return queueOptionFunc(func(cfg config) config {
		if cfg.consumerConfig == nil {
			panic("isn't configure to consume")
		}
		cfg.consumerConfig.consumer = consumer
		return cfg
	})
}

func WithAutoAck(autoAck bool) ConfigOption {
	return queueOptionFunc(func(cfg config) config {
		if cfg.consumerConfig == nil {
			panic("isn't configure to consume")
		}
		cfg.consumerConfig.autoAck = autoAck
		return cfg
	})
}

func WithExclusive(exclusive bool) ConfigOption {
	return queueOptionFunc(func(cfg config) config {
		cfg.queueConfig.exclusive = exclusive
		return cfg
	})
}

func WithNoLocal(noLocal bool) ConfigOption {
	return queueOptionFunc(func(cfg config) config {
		if cfg.consumerConfig == nil {
			panic("isn't configure to consume")
		}
		cfg.consumerConfig.noLocal = noLocal
		return cfg
	})
}

func WithNoWait(noWait bool) ConfigOption {
	return queueOptionFunc(func(cfg config) config {
		cfg.queueConfig.noWait = noWait
		return cfg
	})
}

func WithArgs(args amqp.Table) ConfigOption {
	return queueOptionFunc(func(cfg config) config {
		cfg.queueConfig.args = args
		return cfg
	})
}

func WithDurableQueue(durableQueue bool) ConfigOption {
	return queueOptionFunc(func(cfg config) config {
		cfg.queueConfig.durableQueue = durableQueue
		return cfg
	})
}

func WithSynchronous() ConfigOption {
	return queueOptionFunc(func(cfg config) config {
		cfg.synchronous = true
		return cfg
	})
}

func WithDeliveryMode(deliveryMode uint8) ConfigOption {
	return queueOptionFunc(func(cfg config) config {
		if cfg.publishConfig == nil {
			panic("isn't configure to publish")
		}
		cfg.publishConfig.publishing.DeliveryMode = deliveryMode
		return cfg
	})
}

func WithContentType(contentType string) ConfigOption {
	return queueOptionFunc(func(cfg config) config {
		if cfg.publishConfig == nil {
			panic("isn't configure to publish")
		}
		cfg.publishConfig.publishing.ContentType = contentType
		return cfg
	})
}

func WithReplyTo(replyTo string) ConfigOption {
	return queueOptionFunc(func(cfg config) config {
		if cfg.publishConfig == nil {
			panic("isn't configure to publish")
		}
		cfg.publishConfig.publishing.ReplyTo = replyTo
		return cfg
	})
}

func WithCorrelationId(correlationID string) ConfigOption {
	return queueOptionFunc(func(cfg config) config {
		if cfg.publishConfig == nil {
			panic("isn't configure to publish")
		}
		cfg.publishConfig.publishing.CorrelationId = correlationID
		return cfg
	})
}

func WithExchange() {
	//	TODO implement
}

func WithQueueBinding() {
	//	TODO implement
}

func WithQos(
	prefetchCount int,
	prefetchSize int,
	global bool,
) ConfigOption {
	return queueOptionFunc(func(cfg config) config {
		if prefetchCount <= 0 {
			panic(fmt.Sprintf("prefetchCount = %v, must be positive", prefetchCount))
		}

		if prefetchSize < 0 {
			panic(fmt.Sprintf("prefetchSize = %v, must be non-negative", prefetchCount))
		}

		cfg.qos = &qosConfig{
			prefetchCount: prefetchCount,
			prefetchSize:  prefetchSize,
			global:        global,
		}
		return cfg
	})
}
