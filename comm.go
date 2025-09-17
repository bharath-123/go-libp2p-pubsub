package pubsub

import (
	"context"
	"encoding/binary"
	"io"
	"time"

	"github.com/gogo/protobuf/proto"
	pool "github.com/libp2p/go-buffer-pool"
	"github.com/multiformats/go-varint"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-msgio"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	pb "github.com/libp2p/go-libp2p-pubsub/pb"
)

// get the initial RPC containing all of our subscriptions to send to new peers
func (p *PubSub) getHelloPacket() *RPC {
	var rpc RPC

	subscriptions := make(map[string]bool)

	for t := range p.mySubs {
		subscriptions[t] = true
	}

	for t := range p.myRelays {
		subscriptions[t] = true
	}

	for t := range subscriptions {
		as := &pb.RPC_SubOpts{
			Topicid:   proto.String(t),
			Subscribe: proto.Bool(true),
		}
		rpc.Subscriptions = append(rpc.Subscriptions, as)
	}
	return &rpc
}

func (p *PubSub) handleNewStream(s network.Stream) {
	_, handleNetworkStreamSpan := otelTracer.Start(context.Background(), "pubsub.handle_network_stream")
	peer := s.Conn().RemotePeer()
	
	p.inboundStreamsMx.Lock()
	other, dup := p.inboundStreams[peer]
	if dup {
		log.Debugf("duplicate inbound stream from %s; resetting other stream", peer)
		other.Reset()
	}
	p.inboundStreams[peer] = s
	p.inboundStreamsMx.Unlock()

	defer func() {		
		handleNetworkStreamSpan.End()		
		p.inboundStreamsMx.Lock()
		if p.inboundStreams[peer] == s {
			delete(p.inboundStreams, peer)
		}
		p.inboundStreamsMx.Unlock()
	}()

	var rpcSpan trace.Span
	defer func() {
		if rpcSpan != nil {
			rpcSpan.End()
		}
	}()

	r := msgio.NewVarintReaderSize(s, p.maxMessageSize)
	for {
		// Create span for each message read operation
		_, msgSpan := otelTracer.Start(context.Background(), "pubsub.incoming.msg")
		msgSpan.SetAttributes(
			attribute.String("peer_id", peer.String()),
		)

		// ignore the values. We only want to know when the first bytes came in.
		_, _ = r.NextMsgLen()
		// Start the time once we've received the message length
		timeWhenMessageReceived := time.Now()
		msgbytes, err := r.ReadMsg()
		if err != nil {
			r.ReleaseMsg(msgbytes)
			msgSpan.SetAttributes(attribute.String("error", err.Error()))
			msgSpan.End()
			if err != io.EOF {
				s.Reset()
				log.Debugf("error reading rpc from %s: %s", s.Conn().RemotePeer(), err)
			} else {
				// Just be nice. They probably won't read this
				// but it doesn't hurt to send it.
				s.Close()
			}

			return
		}
		if len(msgbytes) == 0 {
			msgSpan.SetAttributes(attribute.String("result", "empty_message"))
			msgSpan.End()
			continue
		}

		messageSize := len(msgbytes)
		
		// Parse RPC and analyze content
		rpc := new(RPC)
		err = rpc.Unmarshal(msgbytes)
		r.ReleaseMsg(msgbytes)
		if err != nil {
			msgSpan.SetAttributes(
				attribute.String("result", "parse_error"), 
				attribute.String("error", err.Error()),
				attribute.Int("msgSize", messageSize),
			)
			msgSpan.End()
			s.Reset()
			log.Warnf("bogus rpc from %s: %s", s.Conn().RemotePeer(), err)
			return
		}

		msgSpan.SetAttributes(attribute.Int64("time_to_read_rpc", time.Since(timeWhenMessageReceived).Milliseconds()))

		rpc.receivedAt = timeWhenMessageReceived
		rpc.ctx, rpcSpan = otelTracer.Start(context.Background(), "pubsub.incoming.rpc")

		// Analyze RPC content for detailed metrics
		messageCount := len(rpc.GetPublish())
		subscriptionCount := len(rpc.GetSubscriptions())
		ihaveCount, iwantCount, graftCount, pruneCount, idontwantCount := 0, 0, 0, 0, 0
		if rpc.Control != nil {
			ihaveCount = len(rpc.Control.GetIhave())
			iwantCount = len(rpc.Control.GetIwant())
			graftCount = len(rpc.Control.GetGraft())
			pruneCount = len(rpc.Control.GetPrune())
			idontwantCount = len(rpc.Control.GetIdontwant())
		}

		// Queue to event loop
		rpc.from = peer

		queueSpanCtx, queueSpan := otelTracer.Start(rpc.ctx,"pubsub.incoming.rpc.queued")
		rpc.queuedCtx = queueSpanCtx

		select {
		case p.incoming <- rpc:
		case <-p.ctx.Done():
			queueSpan.End()
			msgSpan.SetAttributes(attribute.String("result", "stream_done"))
			msgSpan.End()
			// Close is useless because the other side isn't reading.
			s.Reset()
			return
		}

		// Set comprehensive message attributes
		msgSpan.SetAttributes(
			attribute.String("result", "sent_to_event_loop"),
			attribute.Int("message_size_bytes", messageSize),
			attribute.Int("message_count", messageCount),
			attribute.Int("subscription_count", subscriptionCount),
			attribute.Int("ihave_count", ihaveCount),
			attribute.Int("iwant_count", iwantCount),
			attribute.Int("graft_count", graftCount),
			attribute.Int("prune_count", pruneCount),
			attribute.Int("idontwant_count", idontwantCount),
		)
		
		msgSpan.End()
	}
}

func (p *PubSub) notifyPeerDead(pid peer.ID) {
	p.peerDeadPrioLk.RLock()
	p.peerDeadMx.Lock()
	p.peerDeadPend[pid] = struct{}{}
	p.peerDeadMx.Unlock()
	p.peerDeadPrioLk.RUnlock()

	select {
	case p.peerDead <- struct{}{}:
	default:
	}
}

func (p *PubSub) handleNewPeer(ctx context.Context, pid peer.ID, outgoing *rpcQueue) {
	s, err := p.host.NewStream(p.ctx, pid, p.rt.Protocols()...)
	if err != nil {
		log.Debug("opening new stream to peer: ", err, pid)

		select {
		case p.newPeerError <- pid:
		case <-ctx.Done():
		}

		return
	}

	go p.handleSendingMessages(ctx, s, outgoing)
	go p.handlePeerDead(s)
	select {
	case p.newPeerStream <- s:
	case <-ctx.Done():
	}
}

func (p *PubSub) handleNewPeerWithBackoff(ctx context.Context, pid peer.ID, backoff time.Duration, outgoing *rpcQueue) {
	select {
	case <-time.After(backoff):
		p.handleNewPeer(ctx, pid, outgoing)
	case <-ctx.Done():
		return
	}
}

func (p *PubSub) handlePeerDead(s network.Stream) {
	pid := s.Conn().RemotePeer()

	_, err := s.Read([]byte{0})
	if err == nil {
		log.Debugf("unexpected message from %s", pid)
	}

	s.Reset()
	p.notifyPeerDead(pid)
}

func (p *PubSub) handleSendingMessages(ctx context.Context, s network.Stream, outgoing *rpcQueue) {
	writeRpc := func(rpc *RPC) error {
		size := uint64(rpc.Size())

		buf := pool.Get(varint.UvarintSize(size) + int(size))
		defer pool.Put(buf)

		n := binary.PutUvarint(buf, size)
		_, err := rpc.MarshalTo(buf[n:])
		if err != nil {
			return err
		}

		_, err = s.Write(buf)
		return err
	}

	defer s.Close()
	for ctx.Err() == nil {
		rpc, err := outgoing.Pop(ctx)
		if err != nil {
			log.Debugf("popping message from the queue to send to %s: %s", s.Conn().RemotePeer(), err)
			return
		}

		err = writeRpc(rpc)
		if err != nil {
			s.Reset()
			log.Debugf("writing message to %s: %s", s.Conn().RemotePeer(), err)
			return
		}
	}
}

func rpcWithSubs(subs ...*pb.RPC_SubOpts) *RPC {
	return &RPC{
		RPC: pb.RPC{
			Subscriptions: subs,
		},
	}
}

func rpcWithMessages(msgs ...*pb.Message) *RPC {
	return &RPC{RPC: pb.RPC{Publish: msgs}}
}

func rpcWithControl(msgs []*pb.Message,
	ihave []*pb.ControlIHave,
	iwant []*pb.ControlIWant,
	graft []*pb.ControlGraft,
	prune []*pb.ControlPrune,
	idontwant []*pb.ControlIDontWant) *RPC {
	return &RPC{
		RPC: pb.RPC{
			Publish: msgs,
			Control: &pb.ControlMessage{
				Ihave:     ihave,
				Iwant:     iwant,
				Graft:     graft,
				Prune:     prune,
				Idontwant: idontwant,
			},
		},
	}
}

func copyRPC(rpc *RPC) *RPC {
	res := new(RPC)
	*res = *rpc
	if rpc.Control != nil {
		res.Control = new(pb.ControlMessage)
		*res.Control = *rpc.Control
	}
	return res
}
