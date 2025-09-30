package pubsub

import (
	"context"
	"fmt"

	// "fmt"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

func getDefaultHosts(t *testing.T, n int) []host.Host {
	var out []host.Host

	for i := 0; i < n; i++ {
		h, err := libp2p.New(libp2p.ResourceManager(&network.NullResourceManager{}))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { h.Close() })
		out = append(out, h)
	}

	return out
}

// See https://github.com/libp2p/go-libp2p-pubsub/issues/426
func TestPubSubRemovesBlacklistedPeer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	hosts := getDefaultHosts(t, 2)

	bl := NewMapBlacklist()

	psubs0 := getPubsub(ctx, hosts[0])
	psubs1 := getPubsub(ctx, hosts[1], WithBlacklist(bl))
	connect(t, hosts[0], hosts[1])

	// Bad peer is blacklisted after it has connected.
	// Calling p.BlacklistPeer directly does the right thing but we should also clean
	// up the peer if it has been added the the blacklist by another means.
	withRouter(psubs1, func(r PubSubRouter) {
		bl.Add(hosts[0].ID())
	})

	_, err := psubs0.Subscribe("test")
	if err != nil {
		t.Fatal(err)
	}

	sub1, err := psubs1.Subscribe("test")
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(time.Millisecond * 100)

	psubs0.Publish("test", []byte("message"))

	wctx, cancel2 := context.WithTimeout(ctx, 1*time.Second)
	defer cancel2()

	_, _ = sub1.Next(wctx)

	// Explicitly cancel context so PubSub cleans up peer channels.
	// Issue 426 reports a panic due to a peer channel being closed twice.
	cancel()
	time.Sleep(time.Millisecond * 100)
}

// getDefaultHostsForBench creates default hosts for benchmarking
func getDefaultHostsForBench(b *testing.B, n int) []host.Host {
	var out []host.Host

	for i := 0; i < n; i++ {
		h, err := libp2p.New(libp2p.ResourceManager(&network.NullResourceManager{}))
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { h.Close() })
		out = append(out, h)
	}

	return out
}

// BenchmarkHandleIncomingRPC benchmarks the performance of handleIncomingRPC
// with different types of RPC messages (subscriptions, publications, control messages)
func BenchmarkHandleIncomingRPC(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create test hosts
	hosts := getDefaultHostsForBench(b, 2)

	// Create PubSub instances - using getPubsub from floodsub_test.go pattern
	// but adapted for GossipSub
	ps1, err := NewGossipSub(ctx, hosts[0])
	if err != nil {
		b.Fatal(err)
	}

	_, err = NewGossipSub(ctx, hosts[1])
	if err != nil {
		b.Fatal(err)
	}

	// Connect the hosts
	err = hosts[1].Connect(ctx, peer.AddrInfo{ID: hosts[0].ID(), Addrs: hosts[0].Addrs()})
	if err != nil {
		b.Fatal(err)
	}

	// Wait for connection to establish
	time.Sleep(100 * time.Millisecond)

	// Create test peer ID
	testPeer := hosts[1].ID()

	b.Run("PublishRPC", func(b *testing.B) {
		// Subscribe to topic first so messages are processed
		_, err := ps1.Subscribe("bench-topic")
		if err != nil {
			b.Fatal(err)
		}

		// Create RPC with publish messages
		testData := []byte("benchmark test message data")
		rpc := &RPC{
			RPC: pb.RPC{
				Publish: []*pb.Message{
					{
						From:  []byte(testPeer),
						Data:  testData,
						Topic: stringPtr("bench-topic"),
						Seqno: []byte{0, 0, 0, 1},
					},
				},
			},
			from:       testPeer,
			receivedAt: time.Now(),
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Create new RPC for each iteration to avoid deduplication
			rpc.RPC.Publish[0].Seqno = []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
			ps1.handleIncomingRPC(rpc)
		}
	})

	b.Run("LargeMessageRPC", func(b *testing.B) {
		// Subscribe to topic first
		_, err := ps1.Subscribe("large-topic")
		if err != nil {
			b.Fatal(err)
		}

		// Create RPC with large message (1MB)
		largeData := make([]byte, 1024*1024)
		for i := range largeData {
			largeData[i] = byte(i % 256)
		}

		rpc := &RPC{
			RPC: pb.RPC{
				Publish: []*pb.Message{
					{
						From:  []byte(testPeer),
						Data:  largeData,
						Topic: stringPtr("large-topic"),
						Seqno: []byte{0, 0, 0, 1},
					},
				},
			},
			from:       testPeer,
			receivedAt: time.Now(),
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Update sequence number to avoid deduplication
			rpc.RPC.Publish[0].Seqno = []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
			ps1.handleIncomingRPC(rpc)
		}
	})

	b.Run("SubscriptionRPC", func(b *testing.B) {
		// Create RPC with subscription messages
		rpc := &RPC{
			RPC: pb.RPC{
				Subscriptions: []*pb.RPC_SubOpts{
					{
						Subscribe: boolPtr(true),
						Topicid:   stringPtr("bench-sub-topic"),
					},
				},
			},
			from:       testPeer,
			receivedAt: time.Now(),
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			ps1.handleIncomingRPC(rpc)
		}
	})

	b.Run("UnsubscriptionRPC", func(b *testing.B) {
		// First subscribe to have something to unsubscribe from
		_, err := ps1.Subscribe("bench-unsub-topic")
		if err != nil {
			b.Fatal(err)
		}

		// Create RPC with unsubscription messages
		rpc := &RPC{
			RPC: pb.RPC{
				Subscriptions: []*pb.RPC_SubOpts{
					{
						Subscribe: boolPtr(false),
						Topicid:   stringPtr("bench-unsub-topic"),
					},
				},
			},
			from:       testPeer,
			receivedAt: time.Now(),
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			ps1.handleIncomingRPC(rpc)
		}
	})

	b.Run("IHaveControlRPC", func(b *testing.B) {
		// Subscribe to topic first so we can receive IHAVE
		_, err := ps1.Subscribe("ihave-topic")
		if err != nil {
			b.Fatal(err)
		}

		// Create RPC with IHAVE control messages
		messageIDs := []string{"msg1", "msg2", "msg3", "msg4", "msg5"}
		rpc := &RPC{
			RPC: pb.RPC{
				Control: &pb.ControlMessage{
					Ihave: []*pb.ControlIHave{
						{
							TopicID:    stringPtr("ihave-topic"),
							MessageIDs: messageIDs,
						},
					},
				},
			},
			from:       testPeer,
			receivedAt: time.Now(),
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			ps1.handleIncomingRPC(rpc)
		}
	})

	b.Run("IWantControlRPC", func(b *testing.B) {
		// Create RPC with IWANT control messages
		messageIDs := []string{"want1", "want2", "want3", "want4", "want5"}
		rpc := &RPC{
			RPC: pb.RPC{
				Control: &pb.ControlMessage{
					Iwant: []*pb.ControlIWant{
						{
							MessageIDs: messageIDs,
						},
					},
				},
			},
			from:       testPeer,
			receivedAt: time.Now(),
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			ps1.handleIncomingRPC(rpc)
		}
	})

	b.Run("GraftControlRPC", func(b *testing.B) {
		// Subscribe to topic first so GRAFT makes sense
		_, err := ps1.Subscribe("graft-topic")
		if err != nil {
			b.Fatal(err)
		}

		// Create RPC with GRAFT control messages
		rpc := &RPC{
			RPC: pb.RPC{
				Control: &pb.ControlMessage{
					Graft: []*pb.ControlGraft{
						{
							TopicID: stringPtr("graft-topic"),
						},
					},
				},
			},
			from:       testPeer,
			receivedAt: time.Now(),
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			ps1.handleIncomingRPC(rpc)
		}
	})

	b.Run("PruneControlRPC", func(b *testing.B) {
		// Subscribe to topic first so PRUNE makes sense
		_, err := ps1.Subscribe("prune-topic")
		if err != nil {
			b.Fatal(err)
		}

		// Create RPC with PRUNE control messages (with PX peers)
		peerInfos := []*pb.PeerInfo{
			{
				PeerID: []byte("peer1"),
			},
			{
				PeerID: []byte("peer2"),
			},
		}

		rpc := &RPC{
			RPC: pb.RPC{
				Control: &pb.ControlMessage{
					Prune: []*pb.ControlPrune{
						{
							TopicID: stringPtr("prune-topic"),
							Peers:   peerInfos,
							Backoff: uint64Ptr(60), // 60 second backoff
						},
					},
				},
			},
			from:       testPeer,
			receivedAt: time.Now(),
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			ps1.handleIncomingRPC(rpc)
		}
	})

	b.Run("MixedControlRPC", func(b *testing.B) {
		// Subscribe to topics first
		_, err := ps1.Subscribe("mixed-topic1")
		if err != nil {
			b.Fatal(err)
		}
		_, err = ps1.Subscribe("mixed-topic2")
		if err != nil {
			b.Fatal(err)
		}

		// Create RPC with multiple control message types
		rpc := &RPC{
			RPC: pb.RPC{
				Control: &pb.ControlMessage{
					Ihave: []*pb.ControlIHave{
						{
							TopicID:    stringPtr("mixed-topic1"),
							MessageIDs: []string{"ihave1", "ihave2"},
						},
					},
					Iwant: []*pb.ControlIWant{
						{
							MessageIDs: []string{"iwant1", "iwant2"},
						},
					},
					Graft: []*pb.ControlGraft{
						{
							TopicID: stringPtr("mixed-topic2"),
						},
					},
					Idontwant: []*pb.ControlIDontWant{
						{
							MessageIDs: []string{"dontwant1"},
						},
					},
				},
			},
			from:       testPeer,
			receivedAt: time.Now(),
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			ps1.handleIncomingRPC(rpc)
		}
	})

	b.Run("MultipleMessagesRPC", func(b *testing.B) {
		// Subscribe to topic first
		_, err := ps1.Subscribe("multi-topic")
		if err != nil {
			b.Fatal(err)
		}

		// Create RPC with multiple messages
		messages := make([]*pb.Message, 10)
		for i := 0; i < 10; i++ {
			messages[i] = &pb.Message{
				From:  []byte(testPeer),
				Data:  []byte(fmt.Sprintf("message %d", i)),
				Topic: stringPtr("multi-topic"),
				Seqno: []byte{0, 0, byte(i >> 8), byte(i)},
			}
		}

		rpc := &RPC{
			RPC: pb.RPC{
				Publish: messages,
			},
			from:       testPeer,
			receivedAt: time.Now(),
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Update sequence numbers to avoid deduplication
			for j, msg := range rpc.RPC.Publish {
				msg.Seqno = []byte{byte(i >> 8), byte(i), byte(j >> 8), byte(j)}
			}
			ps1.handleIncomingRPC(rpc)
		}
	})

	b.Run("ComplexMixedRPC", func(b *testing.B) {
		// Subscribe to topics first
		_, err := ps1.Subscribe("complex-topic")
		if err != nil {
			b.Fatal(err)
		}

		// Create RPC with publications, subscriptions, and control messages
		rpc := &RPC{
			RPC: pb.RPC{
				Subscriptions: []*pb.RPC_SubOpts{
					{
						Subscribe: boolPtr(true),
						Topicid:   stringPtr("complex-new-topic"),
					},
				},
				Publish: []*pb.Message{
					{
						From:  []byte(testPeer),
						Data:  []byte("complex message data"),
						Topic: stringPtr("complex-topic"),
						Seqno: []byte{0, 0, 0, 1},
					},
				},
				Control: &pb.ControlMessage{
					Ihave: []*pb.ControlIHave{
						{
							TopicID:    stringPtr("complex-topic"),
							MessageIDs: []string{"complex1", "complex2"},
						},
					},
					Graft: []*pb.ControlGraft{
						{
							TopicID: stringPtr("complex-topic"),
						},
					},
				},
			},
			from:       testPeer,
			receivedAt: time.Now(),
		}

		b.ResetTimer()
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			// Update sequence number to avoid deduplication
			rpc.RPC.Publish[0].Seqno = []byte{byte(i >> 24), byte(i >> 16), byte(i >> 8), byte(i)}
			ps1.handleIncomingRPC(rpc)
		}
	})
}

// Helper functions for benchmark
func stringPtr(s string) *string {
	return &s
}

func boolPtr(b bool) *bool {
	return &b
}

func uint64Ptr(u uint64) *uint64 {
	return &u
}

// Global variables to prevent compiler optimization in benchmarks
var (
	benchSinkString            string
	benchSinkContext           context.Context
	benchSinkAttributeSet      attribute.Set
	benchSinkMeasurementOption metric.MeasurementOption
)

func BenchmarkIsolateMetricAllocations(b *testing.B) {
	// Setup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hosts := getDefaultHostsForBench(b, 1)
	ps1, err := NewGossipSub(ctx, hosts[0])
	if err != nil {
		b.Fatal(err)
	}
	metricCtx := context.Background()

	// Create test message
	testData := []byte("benchmark test message data")
	pmsg := &pb.Message{
		From:  []byte(hosts[0].ID()),
		Data:  testData,
		Topic: stringPtr("bench-topic"),
		Seqno: []byte{0, 0, 0, 1},
	}

	b.Run("JustGetTopic", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchSinkString = pmsg.GetTopic() // Prevent optimization
		}
	})

	b.Run("JustDirectTopicAccess", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchSinkString = *pmsg.Topic // Direct field access - should be faster
		}
	})

	b.Run("JustContextBackground", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchSinkContext = context.Background() // Prevent optimization
		}
	})

	b.Run("WithAttributeSet", func(b *testing.B) {
		b.ReportAllocs()
		topic := pmsg.GetTopic()
		attrSet := ps1.metrics.GetAttributeSet(topic, "", "")
		for i := 0; i < b.N; i++ {
			benchSinkMeasurementOption = metric.WithAttributeSet(attrSet) // Prevent optimization
		}
	})

	b.Run("JustGetAttributeSet", func(b *testing.B) {
		b.ReportAllocs()
		topic := pmsg.GetTopic()
		for i := 0; i < b.N; i++ {
			benchSinkAttributeSet = ps1.metrics.GetAttributeSet(topic, "", "")
		}
	})

	b.Run("JustMetricAdd", func(b *testing.B) {
		b.ReportAllocs()
		topic := pmsg.GetTopic()
		attrs := ps1.metrics.GetAttributeSet(topic, "", "")
		ctx := context.Background()
		for i := 0; i < b.N; i++ {
			ps1.metrics.topicMsgRecvdUnfiltered.Add(ctx, 1, metric.WithAttributeSet(attrs))
		}
	})

	b.Run("OptimizedVersion", func(b *testing.B) {
		b.ReportAllocs()
		topic := *pmsg.Topic                                // Extract once
		attrs := ps1.metrics.GetAttributeSet(topic, "", "") // Cache hit after first call
		for i := 0; i < b.N; i++ {
			ps1.metrics.topicMsgRecvdUnfiltered.Add(metricCtx, 1, metric.WithAttributeSet(attrs))
		}
	})
}
