package pubsub

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const metricPrefix = "libp2p_gossipsub_"

type metrics struct {
	metric.MeterProvider
	// time spent to read message over network and parse it into an RPC.
	messageReceivedTime metric.Int64Histogram
	// time taken for a heartbeat to complete
	heartbeatTime metric.Int64Histogram
	// The time spent waiting by an RPC to be sent to the incoming channel in handleNewStream
	rpcIncomingChannelContentionTime metric.Int64Histogram
	// The time spent in `sendMsgBlocking` to send a message for publishing to the sendMsg channel
	sendMsgChannelContentionTime metric.Int64Histogram
	// Time taken for a message to be published to peers from the time it is received
	messagePublishTime metric.Int64Histogram

	// total number of topics subscribed to
	totalTopicCount metric.Int64Gauge
	// total number of subscriptions active in the pubsub router per topic
	totalSubscriptionCount metric.Int64Gauge
	// depth of the incoming queue in the event loop
	incomingQueueDepth metric.Int64Histogram
	// depth of the sendMsg queue in the event loop
	sendMsgQueueDepth metric.Int64Histogram

	// distribution of message sizes
	messageSize metric.Int64Histogram
	// mesh size per topic
	meshMemberCount metric.Int64Gauge
	// fanout network size per topic
	fanoutMemberCount metric.Int64Gauge

	// time spent by different events in the event loop before being picked up
	// for processing.
	eventLoopWaitTime metric.Int64Histogram
	// time spent by different events
	eventProcessingTime metric.Int64Histogram
	// the count of the number times each branch executed in the event loop
	eventCount metric.Int64Counter

	// the number of times, we have received an IDONTWANT message
	iDontWantMsgRecvd metric.Int64Counter
	// the number of times, we have sent an IDONTWANT message
	iDontWantMsgSent metric.Int64Counter
	// the number of IWantMsgs sent
	iWantMsgSent metric.Int64Counter
	// the number of IWantMsgs recvd
	iWantMsgRecvd metric.Int64Counter
	// the number of prune msgs recvd per topic
	pruneMsgRecvdPerTopic metric.Int64Counter
	// the number of graft msgs recvd per topic
	graftMsgRecvdPerTopic metric.Int64Counter
	// the number of prune msgs sent per topic
	pruneMsgSentPerTopic metric.Int64Counter
	// the number of graft msgs sent per topic
	graftMsgSentPerTopic metric.Int64Counter

	// the number of messages sent per topic
	topicMsgSent metric.Int64Counter
	// the bytes sent per topic
	topicBytesSent metric.Int64Counter
	// the number of messages published per topic
	topicMsgPublished metric.Int64Counter

	// the number of messages received per topic
	topicMsgRecvd metric.Int64Counter
	// the bytes received per topic
	topicBytesRecvd metric.Int64Counter
	// the number of messages received per topic unfiltered(with duplicates)
	topicMsgRecvdUnfiltered metric.Int64Counter

	// the size of the priority outgoing rpc queue
	outGoingPriorityRpcQueueSize metric.Int64Histogram
	// the size of the normal outgoing rpc queue
	outGoingNormalRpcQueueSize metric.Int64Histogram
	rpcsDropped                metric.Int64Counter
	// number of times a rpc had to be split
	rpcSplitCount metric.Int64Counter

	duplicateMessages        metric.Int64Counter
	rejectedMessages         metric.Int64Counter
	ignoredMessages          metric.Int64Counter
	inlineValidationDuration metric.Int64Histogram

	asyncValidationDuration  metric.Int64Histogram
	asyncValidationThrottled metric.Int64Counter

	validationDuration metric.Int64Histogram

	lateIDONTWANTs      metric.Int64Counter
	effectiveIDONTWANTs metric.Int64Counter
}

func WithMeterProvider(meterProvider metric.MeterProvider) Option {
	return func(ps *PubSub) error {
		ps.metrics.MeterProvider = meterProvider
		return nil
	}
}

func InitMetrics(ps *PubSub) error {
	meter := ps.metrics.MeterProvider.Meter("libp2p-pubsub")

	var err error
	if ps.metrics.messageReceivedTime, err = meter.Int64Histogram(
		metricPrefix+"message_received_time",
		metric.WithDescription("The duration of parsing a message received from the network stream to an RPC"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 5_000_000, 10_000_000),
	); err != nil {
		return err
	}

	if ps.metrics.heartbeatTime, err = meter.Int64Histogram(
		metricPrefix+"heartbeat_time.duration",
		metric.WithDescription("The duration of the heartbeat"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 5_000_000, 10_000_000),
	); err != nil {
		return err
	}

	if ps.metrics.rpcIncomingChannelContentionTime, err = meter.Int64Histogram(
		metricPrefix+"rpc_incoming_channel_contention_time",
		metric.WithDescription("The time spent waiting by an RPC to be sent to the incoming channel in handleNewStream"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 5_000_000, 10_000_000),
	); err != nil {
		return err
	}

	if ps.metrics.sendMsgChannelContentionTime, err = meter.Int64Histogram(
		metricPrefix+"send_msg_channel_contention_time",
		metric.WithDescription("The time spent waiting by a message to be sent to the sendMsg channel in SendMsgBlocking"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 5_000_000, 10_000_000),
	); err != nil {
		return err
	}

	if ps.metrics.messagePublishTime, err = meter.Int64Histogram(
		metricPrefix+"message_publish_time",
		metric.WithDescription("The time taken for a message to be published to peers from the time it is received"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 5_000_000, 10_000_000),
	); err != nil {
		return err
	}

	if ps.metrics.outGoingPriorityRpcQueueSize, err = meter.Int64Histogram(
		metricPrefix+"outgoing_priority_rpc_queue_size",
		metric.WithDescription("The size of the priority outgoing rpc queue"),
		metric.WithExplicitBucketBoundaries(1, 5, 10, 20, 30, 50, 70, 100, 500, 1000),
	); err != nil {
		return err
	}

	if ps.metrics.outGoingNormalRpcQueueSize, err = meter.Int64Histogram(
		metricPrefix+"outgoing_normal_rpc_queue_size",
		metric.WithDescription("The size of the normal outgoing rpc queue"),
		metric.WithExplicitBucketBoundaries(1, 5, 10, 20, 30, 50, 70, 100, 500, 1000),
	); err != nil {
		return err
	}

	if ps.metrics.rpcSplitCount, err = meter.Int64Counter(
		metricPrefix+"rpc_split_count",
		metric.WithDescription("The number of times a rpc had to be split"),
	); err != nil {
		return err
	}

	if ps.metrics.rpcsDropped, err = meter.Int64Counter(
		metricPrefix+"rpcs_dropped",
		metric.WithDescription("The number of times a rpc had to be dropped"),
	); err != nil {
		return err
	}

	if ps.metrics.totalTopicCount, err = meter.Int64Gauge(
		metricPrefix+"total_topic_count",
		metric.WithDescription("The total number of topics that are subscribed to this pubsub router"),
	); err != nil {
		return err
	}

	if ps.metrics.totalSubscriptionCount, err = meter.Int64Gauge(
		metricPrefix+"total_subscription_count",
		metric.WithDescription("The total number of subscriptions that are active in this pubsub router"),
	); err != nil {
		return err
	}

	if ps.metrics.incomingQueueDepth, err = meter.Int64Histogram(
		metricPrefix+"incoming_queue_depth",
		metric.WithDescription("The depth of the incoming queue in the event loop"),
		metric.WithExplicitBucketBoundaries(1, 5, 10, 15, 20, 25, 30, 32),
	); err != nil {
		return err
	}

	if ps.metrics.sendMsgQueueDepth, err = meter.Int64Histogram(
		metricPrefix+"sendmsg_queue_depth",
		metric.WithDescription("The depth of the send message queue in the event loop"),
		metric.WithExplicitBucketBoundaries(1, 5, 10, 15, 20, 25, 30, 32, 64),
	); err != nil {
		return err
	}

	if ps.metrics.messageSize, err = meter.Int64Histogram(
		metricPrefix+"message_size",
		metric.WithDescription("The size of the messages received by the peer in bytes"),
		metric.WithUnit("bytes"),
	); err != nil {
		return err
	}

	if ps.metrics.meshMemberCount, err = meter.Int64Gauge(
		metricPrefix+"mesh_member_count",
		metric.WithDescription("The number of mesh members per topic"),
	); err != nil {
		return err
	}

	if ps.metrics.fanoutMemberCount, err = meter.Int64Gauge(
		metricPrefix+"fanout_member_count",
		metric.WithDescription("The number of fanout members per topic"),
	); err != nil {
		return err
	}

	if ps.metrics.eventLoopWaitTime, err = meter.Int64Histogram(
		metricPrefix+"event_loop_wait_time",
		metric.WithDescription("The time spent by different events in the event loop before being picked up for processing"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 5_000_000, 10_000_000),
	); err != nil {
		return err
	}

	if ps.metrics.eventProcessingTime, err = meter.Int64Histogram(
		metricPrefix+"event_processing_time",
		metric.WithDescription("The time spent processing different events in the event loop"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 5_000_000, 10_000_000),
	); err != nil {
		return err
	}

	if ps.metrics.eventCount, err = meter.Int64Counter(
		metricPrefix+"event_count",
		metric.WithDescription("The count of the number times each branch executed in the event loop"),
	); err != nil {
		return err
	}

	if ps.metrics.iDontWantMsgRecvd, err = meter.Int64Counter(
		metricPrefix+"idont_want_msg_recvd",
		metric.WithDescription("The number of times we have received an IDONTWANT message"),
	); err != nil {
		return err
	}

	if ps.metrics.iDontWantMsgSent, err = meter.Int64Counter(
		metricPrefix+"idont_want_msg_sent",
		metric.WithDescription("The number of times we have sent an IDONTWANT message"),
	); err != nil {
		return err
	}

	if ps.metrics.iWantMsgSent, err = meter.Int64Counter(
		metricPrefix+"iwant_msg_sent",
		metric.WithDescription("The number of IWANT msgs sent"),
	); err != nil {
		return err
	}

	if ps.metrics.iWantMsgRecvd, err = meter.Int64Counter(
		metricPrefix+"iwant_msg_recvd",
		metric.WithDescription("The number of IWANT msgs received"),
	); err != nil {
		return err
	}

	if ps.metrics.pruneMsgRecvdPerTopic, err = meter.Int64Counter(
		metricPrefix+"prune_msg_recvd_per_topic",
		metric.WithDescription("The number of prune msgs received per topic"),
	); err != nil {
		return err
	}

	if ps.metrics.graftMsgRecvdPerTopic, err = meter.Int64Counter(
		metricPrefix+"graft_msg_recvd_per_topic",
		metric.WithDescription("The number of graft msgs received per topic"),
	); err != nil {
		return err
	}

	if ps.metrics.pruneMsgSentPerTopic, err = meter.Int64Counter(
		metricPrefix+"prune_msg_sent_per_topic",
		metric.WithDescription("The number of prune msgs sent per topic"),
	); err != nil {
		return err
	}

	if ps.metrics.graftMsgSentPerTopic, err = meter.Int64Counter(
		metricPrefix+"graft_msg_sent_per_topic",
		metric.WithDescription("The number of graft msgs sent per topic"),
	); err != nil {
		return err
	}

	if ps.metrics.topicMsgSent, err = meter.Int64Counter(
		metricPrefix+"topic_msg_sent",
		metric.WithDescription("The number of messages sent per topic"),
	); err != nil {
		return err
	}

	if ps.metrics.topicBytesSent, err = meter.Int64Counter(
		metricPrefix+"topic_bytes_sent",
		metric.WithDescription("The bytes sent per topic"),
	); err != nil {
		return err
	}

	if ps.metrics.topicMsgPublished, err = meter.Int64Counter(
		metricPrefix+"topic_msg_published",
		metric.WithDescription("The number of messages published per topic"),
	); err != nil {
		return err
	}

	if ps.metrics.topicMsgRecvd, err = meter.Int64Counter(
		metricPrefix+"topic_msg_recvd",
		metric.WithDescription("The number of messages received per topic"),
	); err != nil {
		return err
	}

	if ps.metrics.topicBytesRecvd, err = meter.Int64Counter(
		metricPrefix+"topic_bytes_recvd",
		metric.WithDescription("The bytes received per topic"),
	); err != nil {
		return err
	}

	if ps.metrics.topicMsgRecvdUnfiltered, err = meter.Int64Counter(
		metricPrefix+"topic_msg_recvd_unfiltered",
		metric.WithDescription("The number of messages received per topic unfiltered(with duplicates)"),
	); err != nil {
		return err
	}

	if ps.metrics.lateIDONTWANTs, err = meter.Int64Counter(
		"late_idontwant.count",
		metric.WithDescription("The number of late IDONTWANT messages"),
	); err != nil {
		return err
	}

	if ps.metrics.effectiveIDONTWANTs, err = meter.Int64Counter(
		"effective_idontwant.count",
		metric.WithDescription("The number of effective IDONTWANT messages"),
	); err != nil {
		return err
	}

	if ps.metrics.duplicateMessages, err = meter.Int64Counter(
		"duplicate_messages",
		metric.WithDescription("The number of duplicate messages"),
	); err != nil {
		return err
	}

	if ps.metrics.ignoredMessages, err = meter.Int64Counter(
		"ignored_messages",
		metric.WithDescription("The number of ignored messages"),
	); err != nil {
		return err
	}

	if ps.metrics.rejectedMessages, err = meter.Int64Counter(
		"rejected_messages",
		metric.WithDescription("The number of rejected messages"),
	); err != nil {
		return err
	}

	if ps.metrics.inlineValidationDuration, err = meter.Int64Histogram(
		"inline_validation_duration",
		metric.WithDescription("The duration for inline validation"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 5_000_000, 10_000_000),
	); err != nil {
		return err
	}

	if ps.metrics.asyncValidationDuration, err = meter.Int64Histogram(
		metricPrefix+"async_validation_duration",
		metric.WithDescription("The duration for async validation"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 5_000_000, 10_000_000, 20_000_000, 30_000_000),
	); err != nil {
		return err
	}

	if ps.metrics.asyncValidationThrottled, err = meter.Int64Counter(
		metricPrefix+"async_validation_throttled",
		metric.WithDescription("The number of times async validation was throttled"),
	); err != nil {
		return err
	}

	if ps.metrics.validationDuration, err = meter.Int64Histogram(
		metricPrefix+"validation_duration",
		metric.WithDescription("The duration for validation"),
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(100, 500, 1_000, 5_000, 10_000, 50_000, 100_000, 250_000, 500_000, 1_000_000, 5_000_000, 10_000_000, 20_000_000, 30_000_000, 40_000_000),
	); err != nil {
		return err
	}

	return nil
}

func (m *metrics) IncrementEventCount(eventType string, evalMethodName string) {
	attrs := []attribute.KeyValue{}
	if evalMethodName != "" {
		attrs = append(attrs, attribute.String("eval_method_name", evalMethodName))
	}
	attrs = append(attrs, attribute.String("event_type", eventType))
	m.eventCount.Add(context.Background(), 1, metric.WithAttributes(attrs...))
}

func (m *metrics) RecordEventLoopWaitTeam(waitTime time.Duration, eventType string, evalMethodName string) {
	attrs := []attribute.KeyValue{}
	if evalMethodName != "" {
		attrs = append(attrs, attribute.String("eval_method_name", evalMethodName))
	}
	attrs = append(attrs, attribute.String("event_type", eventType))
	m.eventLoopWaitTime.Record(context.Background(), waitTime.Microseconds(), metric.WithAttributes(attrs...))
}

func (m *metrics) RecordEventProcessingTime(processingTime time.Duration, eventType string, evalMethodName string) {
	attrs := []attribute.KeyValue{}
	if evalMethodName != "" {
		attrs = append(attrs, attribute.String("eval_method_name", evalMethodName))
	}
	attrs = append(attrs, attribute.String("event_type", eventType))
	m.eventProcessingTime.Record(context.Background(), processingTime.Microseconds(), metric.WithAttributes(attrs...))
}
