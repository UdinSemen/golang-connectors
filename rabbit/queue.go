package rabbit

import amqp "github.com/rabbitmq/amqp091-go"

func (c ConnectorImpl) queueDeclare(conf config) (amqp.Queue, error) {
	return c.GetChannel().QueueDeclare(
		conf.queueConfig.queueName,
		conf.queueConfig.durableQueue,
		conf.queueConfig.autoDelete,
		conf.queueConfig.exclusive,
		conf.queueConfig.noWait,
		conf.queueConfig.args,
	)
}
