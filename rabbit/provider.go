package rabbit

import (
	"context"
	"fmt"
)

func (c ConnectorImpl) Publish(
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

	return c.channel.PublishWithContext(
		ctx,
		"",
		q.Name,
		conf.publishConfig.mandatory,
		conf.publishConfig.immediate,
		conf.publishConfig.publishing,
	)
}
