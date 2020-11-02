package pulsar

import (
	log "github.com/apache/pulsar-client-go/pulsar/log"
)

type cumulativeAcksTracker struct {
	requestCh          chan interface{}
	trackedMessageList []*trackingMessageID
	closeCh            chan struct{}
	log                log.Logger
}

// requests
type messagesReceivedReq struct {
	msgIDs []*trackingMessageID
}

func (c *cumulativeAcksTracker) messagesReceived(msgIDs []*trackingMessageID) {
	c.requestCh <- &messagesReceivedReq{
		msgIDs: msgIDs,
	}
}

type ackSentReq struct {
	msgID *trackingMessageID
}

func (c *cumulativeAcksTracker) ackSent(msgID *trackingMessageID) {
	c.requestCh <- &ackSentReq{
		msgID: msgID,
	}
}

type cumulativeAckSentReq struct {
	msgID *trackingMessageID
}

func (c *cumulativeAcksTracker) cumulativeAckSent(msgID *trackingMessageID) {
	c.requestCh <- &cumulativeAckSentReq{
		msgID: msgID,
	}
}

type getCumulativeMessageIDReq struct {
	msgID    *trackingMessageID
	ackMsgID chan *trackingMessageID
}

func (c *cumulativeAcksTracker) getCumulativeMessageID(msgID *trackingMessageID) *trackingMessageID {
	ackMsgID := make(chan *trackingMessageID)

	c.requestCh <- &getCumulativeMessageIDReq{
		msgID:    msgID,
		ackMsgID: ackMsgID,
	}

	return <-ackMsgID
}

func newCumulativeAcksTracker(receiverQueueSize int, logger log.Logger) *cumulativeAcksTracker {
	c := &cumulativeAcksTracker{
		requestCh:          make(chan interface{}),
		trackedMessageList: make([]*trackingMessageID, 0, receiverQueueSize),
		closeCh:            make(chan struct{}),
		log:                logger,
	}

	logger.Debug("Created new cumulative acks tracker")

	go c.process()

	return c
}

func (c *cumulativeAcksTracker) process() {
	c.log.Debug("Starting cumulative acks tracker request processor")

	defer func() {
		c.log.Debug("Stopped cumulative acks tracker request processor")
	}()

	for {
		select {
		case <-c.closeCh:
			return
		case req := <-c.requestCh:
			switch p := req.(type) {
			case *messagesReceivedReq:
				c.trackedMessageList = append(c.trackedMessageList, p.msgIDs...)
			case *ackSentReq:
				c.removeMessageID(p.msgID)
			case *cumulativeAckSentReq:
				c.removeUntilMessageID(p.msgID)
			case *getCumulativeMessageIDReq:
				p.ackMsgID <- c.getGreatestCumulativeMessageID(p.msgID)
			}
		}
	}
}

func (c *cumulativeAcksTracker) getMessageIDIndex(msgID *trackingMessageID) int {
	for i := 0; i < len(c.trackedMessageList); i++ {
		if *msgID == *c.trackedMessageList[i] {
			return i
		}
	}

	return -1
}

func (c *cumulativeAcksTracker) removeMessageID(msgID *trackingMessageID) bool {
	idx := c.getMessageIDIndex(msgID)
	if idx == -1 {
		return false
	}

	c.trackedMessageList = append(c.trackedMessageList[:idx], c.trackedMessageList[idx+1:]...)

	return true
}

func (c *cumulativeAcksTracker) removeUntilMessageID(msgID *trackingMessageID) bool {
	idx := c.getMessageIDIndex(msgID)
	if idx == -1 {
		return false
	}

	c.trackedMessageList = c.trackedMessageList[idx+1:]

	return true
}

func (c *cumulativeAcksTracker) getGreatestCumulativeMessageID(msgID *trackingMessageID) *trackingMessageID {
	idx := c.getMessageIDIndex(msgID)
	if idx == -1 {
		return nil
	}

	// test if the message at index can be ACK'ed
	// this won't go through if this is a batch message and the batch is not completly ACK'ed (which is
	// basically the same as this message is not the last from the batch)
	if c.trackedMessageList[idx].ackCumulative() {
		return c.trackedMessageList[idx]
	}

	// specified message can't be ACK'ed so we'll assume that the previous message (if any) has been
	// completly consumed and ACK that instead
	for idx > 0 {
		return c.trackedMessageList[idx-1]
	}

	// no message can be ACK'ed
	return nil
}
