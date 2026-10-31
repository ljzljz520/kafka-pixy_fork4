package offsetmgr

import (
	"net"
	"sync"
	"testing"
	"time"

	. "gopkg.in/check.v1"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/testhelpers"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func Test(t *testing.T) {
	TestingT(t)
}

type OffsetMgrSuite struct {
	ns *actor.Descriptor
}

var _ = Suite(&OffsetMgrSuite{})

func (s *OffsetMgrSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging()
	testReportErrors = true
}

func (s *OffsetMgrSuite) TearDownSuite(c *C) {
	testReportErrors = false
}

func (s *OffsetMgrSuite) SetUpTest(c *C) {
	s.ns = actor.Root().NewChild("T")
}

// When a partition consumer is created, then an initial offset is sent down
// the InitialOffset() channel.
func (s *OffsetMgrSuite) TestInitialOffset(c *C) {
	// Given
	broker1 := sarama.NewMockBroker(c, 101)
	defer broker1.Close()
	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(c).
			SetCoordinator(sarama.CoordinatorGroup, "g1", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 1000, "foo", sarama.ErrNoError).
			SetOffset("g1", "t1", 8, 2000, "bar", sarama.ErrNoError).
			SetOffset("g1", "t2", 9, 3000, "bazz", sarama.ErrNoError),
	})

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, nil)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()

	// When
	om, err := f.Spawn(s.ns.NewChild("g1", "t1", 8), "g1", "t1", 8)
	c.Assert(err, IsNil)
	defer om.Stop()

	// Then
	initialOffset := <-om.CommittedOffsets()
	c.Assert(initialOffset, DeepEquals, Offset{2000, "bar"})
}

// A partition offset manager can be closed even while it keeps trying to
// resolve the coordinator for the broker.
func (s *OffsetMgrSuite) TestInitialNoCoordinator(c *C) {
	// Given
	broker1 := sarama.NewMockBroker(c, 101)
	defer broker1.Close()
	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(c).
			SetError(sarama.CoordinatorGroup, "g1", sarama.ErrOffsetsLoadInProgress),
	})

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.RetryBackoff = 50 * time.Millisecond
	cfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, nil)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()

	// When
	om, err := f.Spawn(s.ns.NewChild("g1", "t1", 8), "g1", "t1", 8)
	c.Assert(err, IsNil)
	om.Stop()

	// Then
	// We are just checking that this test does not hangs.
}

// A partition offset manager can be closed even while it keeps trying to
// resolve the coordinator for the broker.
func (s *OffsetMgrSuite) TestInitialFetchError(c *C) {
	// Given
	broker1 := sarama.NewMockBroker(c, 101)
	defer broker1.Close()
	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(c).
			SetCoordinator(sarama.CoordinatorGroup, "g1", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 0, "", sarama.ErrNotLeaderForPartition),
	})

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.RetryBackoff = 50 * time.Millisecond
	cfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, nil)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()

	// When
	om, err := f.Spawn(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)
	defer om.Stop()

	// Then
	err = <-om.(*offsetMgr).testErrorsCh
	c.Assert(errors.Cause(err), Equals, sarama.ErrNotLeaderForPartition)
}

// If offset commit fails then the corresponding error is sent down to the
// errors channel, but the offset manager keeps retrying until it succeeds.
func (s *OffsetMgrSuite) TestCommitError(c *C) {
	// Given
	broker1 := sarama.NewMockBroker(c, 101)
	defer broker1.Close()

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(c).
			SetCoordinator(sarama.CoordinatorGroup, "g1", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 1234, "foo", sarama.ErrNoError),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 7, sarama.ErrNotLeaderForPartition),
	})

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.RetryBackoff = 100 * time.Millisecond
	cfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, nil)
	c.Assert(err, IsNil)

	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()

	om, err := f.Spawn(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)

	// When
	om.SubmitOffset(Offset{1000, "foo"})
	var wg sync.WaitGroup
	actor.Spawn(actor.Root().NewChild("stopper"), &wg, om.Stop)

	// Then
	err = <-om.(*offsetMgr).testErrorsCh
	c.Assert(errors.Cause(err), Equals, sarama.ErrNotLeaderForPartition)

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(c).
			SetCoordinator(sarama.CoordinatorGroup, "g1", broker1),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 7, sarama.ErrNoError),
	})

	wg.Wait()
	committedOffset := lastCommittedOffset(broker1, "g1", "t1", 7)
	c.Assert(committedOffset, DeepEquals, Offset{1000, "foo"})
}

// If offset a response received from Kafka for an offset commit request does
// not contain information for a submitted offset, then offset manager keeps,
// retrying until it succeeds.
func (s *OffsetMgrSuite) TestCommitIncompleteResponse(c *C) {
	// Given
	broker1 := sarama.NewMockBroker(c, 101)
	defer broker1.Close()

	offsetCommitResponse := sarama.OffsetCommitResponse{
		Errors: map[string]map[int32]sarama.KError{"t1": {2: sarama.ErrNoError}}}

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(c).
			SetCoordinator(sarama.CoordinatorGroup, "g1", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 1, 1000, "foo", sarama.ErrNoError).
			SetOffset("g1", "t1", 2, 2000, "bar", sarama.ErrNoError),
		"OffsetCommitRequest": sarama.NewMockWrapper(&offsetCommitResponse),
	})

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.RetryBackoff = 100 * time.Millisecond
	cfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, nil)
	c.Assert(err, IsNil)

	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()

	om1, err := f.Spawn(s.ns.NewChild("g1", "t1", 1), "g1", "t1", 1)
	c.Assert(err, IsNil)
	<-om1.CommittedOffsets() // Ignore initial offset.
	om2, err := f.Spawn(s.ns.NewChild("g1", "t1", 2), "g1", "t1", 2)
	c.Assert(err, IsNil)
	<-om2.CommittedOffsets() // Ignore initial offset.

	// When
	om1.SubmitOffset(Offset{1001, "foo1"})
	om2.SubmitOffset(Offset{2001, "bar2"})
	var wg sync.WaitGroup
	actor.Spawn(actor.Root().NewChild("stopper"), &wg, om1.Stop)
	actor.Spawn(actor.Root().NewChild("stopper"), &wg, om2.Stop)

	// Then
	err = <-om1.(*offsetMgr).testErrorsCh
	c.Assert(errors.Cause(err), Equals, sarama.ErrIncompleteResponse)
	c.Assert(<-om2.CommittedOffsets(), Equals, Offset{2001, "bar2"})

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(c).
			SetCoordinator(sarama.CoordinatorGroup, "g1", broker1),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 1, sarama.ErrNoError),
	})

	wg.Wait()
	c.Assert(<-om1.CommittedOffsets(), Equals, Offset{1001, "foo1"})
}

// It is guaranteed that a partition offset manager commits all pending offsets
// before it terminates. A transient broker error keeps being retried. A
// metadata-too-large rejection however is a property of the payload: it is
// reported on the dedicated CommitErrors channel, the same payload is never
// resent automatically, and the offset manager proceeds as soon as a smaller
// snapshot is submitted.
func (s *OffsetMgrSuite) TestCommitBeforeClose(c *C) {
	// Given
	broker1 := sarama.NewMockBroker(c, 101)
	defer broker1.Close()

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(c).
			SetCoordinator(sarama.CoordinatorGroup, "g1", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 0, "", sarama.ErrNotLeaderForPartition),
	})

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.RetryBackoff = 25 * time.Millisecond
	cfg.Consumer.OffsetsCommitInterval = 100 * time.Millisecond
	saramaCfg := sarama.NewConfig()
	saramaCfg.Net.ReadTimeout = 10 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, saramaCfg)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()

	om, err := f.Spawn(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)

	// When: a partition offset manager gets a snapshot whose metadata the
	// coordinator rejects as too large.
	om.SubmitOffset(Offset{1001, "foo"})

	// STAGE 1: Requests for the initial offset time out.
	log.Infof("    STAGE 1")
	err = <-om.(*offsetMgr).testErrorsCh
	c.Assert(errors.Cause(err), Equals, sarama.ErrNotLeaderForPartition)

	// STAGE 2: The initial offset is fetched and the commit is rejected with
	// ErrOffsetMetadataTooLarge.
	log.Infof("    STAGE 2")
	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(c).
			SetCoordinator(sarama.CoordinatorGroup, "g1", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 1234, "foo", sarama.ErrNoError),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 7, sarama.ErrOffsetMetadataTooLarge),
	})
	initialOffset := <-om.CommittedOffsets()
	c.Assert(initialOffset, DeepEquals, Offset{1234, "foo"})
	commitErr := <-om.CommitErrors()
	c.Assert(errors.Cause(commitErr.Err), Equals, sarama.ErrOffsetMetadataTooLarge)
	c.Assert(commitErr.Offset, DeepEquals, Offset{1001, "foo"})
	c.Assert(commitErr.Local, Equals, false)
	c.Assert(commitErr.Partition, Equals, int32(7))

	// The same payload must not be remapped/resent automatically: even after
	// several commit intervals the broker has seen it exactly once.
	time.Sleep(300 * time.Millisecond)
	var identicalRequests int
	for _, m := range broker1.History() {
		if req, ok := m.Request.(*sarama.OffsetCommitRequest); ok {
			if offset, metadata, err := req.Offset("t1", 7); err == nil &&
				offset == 1001 && metadata == "foo" {
				identicalRequests++
			}
		}
	}
	c.Assert(identicalRequests, Equals, 1)

	// STAGE 3: The caller (partition consumer) submits a smaller snapshot and
	// the broker accepts it.
	log.Infof("    STAGE 3")
	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(c).
			SetCoordinator(sarama.CoordinatorGroup, "g1", broker1),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 7, sarama.ErrNoError),
	})
	om.SubmitOffset(Offset{1001, ""})
	c.Assert(<-om.CommittedOffsets(), DeepEquals, Offset{1001, ""})

	// Then: termination is immediate once the latest snapshot is persisted.
	om.Stop()
	if _, ok := <-om.CommittedOffsets(); ok {
		c.Errorf("committed offsets channel should be closed")
	}
	committedOffset := lastCommittedOffset(broker1, "g1", "t1", 7)
	c.Assert(committedOffset, DeepEquals, Offset{1001, ""})
}

// Different consumer groups can keep different offsets for the same
// topic/partition, even where they have the same broker as a coordinator.
func (s *OffsetMgrSuite) TestCommitDifferentGroups(c *C) {
	// Given
	broker1 := sarama.NewMockBroker(c, 101)
	defer broker1.Close()

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(c).
			SetCoordinator(sarama.CoordinatorGroup, "g1", broker1).
			SetCoordinator(sarama.CoordinatorGroup, "g2", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 1000, "foo", sarama.ErrNoError).
			SetOffset("g2", "t1", 7, 2000, "bar", sarama.ErrNoError),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 7, sarama.ErrNoError).
			SetError("g2", "t1", 7, sarama.ErrNoError),
	})

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, nil)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()
	om1, err := f.Spawn(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)
	om2, err := f.Spawn(s.ns.NewChild("g2", "t1", 7), "g2", "t1", 7)
	c.Assert(err, IsNil)

	// When
	om1.SubmitOffset(Offset{1009, "foo1"})
	om1.SubmitOffset(Offset{1010, "foo2"})
	om2.SubmitOffset(Offset{2010, "bar1"})
	om2.SubmitOffset(Offset{2011, "bar2"})
	om1.SubmitOffset(Offset{1017, "foo3"})
	om2.SubmitOffset(Offset{2019, "bar3"})
	om1.Stop()
	om2.Stop()

	// Then
	committedOffset1 := lastCommittedOffset(broker1, "g1", "t1", 7)
	c.Assert(committedOffset1, DeepEquals, Offset{1017, "foo3"})
	committedOffset2 := lastCommittedOffset(broker1, "g2", "t1", 7)
	c.Assert(committedOffset2, DeepEquals, Offset{2019, "bar3"})
}

func (s *OffsetMgrSuite) TestCommitNetworkError(c *C) {
	// Given
	broker1 := sarama.NewMockBroker(c, 101)
	broker2 := sarama.NewMockBroker(c, 102)
	defer broker2.Close()

	mockResponses := map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()).
			SetBroker(broker2.Addr(), broker2.BrokerID()),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(c).
			SetCoordinator(sarama.CoordinatorGroup, "g1", broker1).
			SetCoordinator(sarama.CoordinatorGroup, "g2", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 1000, "foo1", sarama.ErrNoError).
			SetOffset("g1", "t1", 8, 2000, "foo2", sarama.ErrNoError).
			SetOffset("g2", "t1", 7, 3000, "foo3", sarama.ErrNoError),
	}
	broker1.SetHandlerByMap(mockResponses)
	broker2.SetHandlerByMap(mockResponses)

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.RetryBackoff = 100 * time.Millisecond
	cfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	saramaCfg := sarama.NewConfig()
	saramaCfg.Net.ReadTimeout = 100 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr(), broker2.Addr()}, saramaCfg)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()

	om1, err := f.Spawn(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)
	om2, err := f.Spawn(s.ns.NewChild("g1", "t1", 8), "g1", "t1", 8)
	c.Assert(err, IsNil)
	om3, err := f.Spawn(s.ns.NewChild("g2", "t1", 7), "g2", "t1", 7)
	c.Assert(err, IsNil)

	// Terminate mock broker to simulate network error.
	broker1.Close()

	om1.SubmitOffset(Offset{1001, "bar1"})
	om2.SubmitOffset(Offset{2001, "bar2"})
	om3.SubmitOffset(Offset{3001, "bar3"})

	log.Infof("*** Waiting for errors...")
	<-om1.(*offsetMgr).testErrorsCh
	<-om2.(*offsetMgr).testErrorsCh
	<-om3.(*offsetMgr).testErrorsCh

	// When
	log.Infof("*** Network recovering...")

	broker1 = sarama.NewMockBrokerAddr(c, 101, broker1.Addr())
	defer broker1.Close()
	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()).
			SetBroker(broker2.Addr(), broker2.BrokerID()),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(c).
			SetCoordinator(sarama.CoordinatorGroup, "g1", broker1).
			SetCoordinator(sarama.CoordinatorGroup, "g2", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 1000, "foo1", sarama.ErrNoError).
			SetOffset("g1", "t1", 8, 2000, "foo2", sarama.ErrNoError).
			SetOffset("g2", "t1", 7, 3000, "foo3", sarama.ErrNoError),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 7, sarama.ErrNoError).
			SetError("g1", "t1", 8, sarama.ErrNoError).
			SetError("g2", "t1", 7, sarama.ErrNoError),
	})
	om1.Stop()
	om2.Stop()
	om3.Stop()

	// Then: offset managers are able to commit offsets and terminate.
	committedOffset1 := lastCommittedOffset(broker1, "g1", "t1", 7)
	c.Assert(committedOffset1, DeepEquals, Offset{1001, "bar1"})
	committedOffset2 := lastCommittedOffset(broker1, "g1", "t1", 8)
	c.Assert(committedOffset2, DeepEquals, Offset{2001, "bar2"})
	committedOffset3 := lastCommittedOffset(broker1, "g2", "t1", 7)
	c.Assert(committedOffset3, DeepEquals, Offset{3001, "bar3"})
}

func (s *OffsetMgrSuite) TestCommittedChannel(c *C) {
	// Given
	broker1 := sarama.NewMockBroker(c, 101)
	defer broker1.Close()

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(c).
			SetCoordinator(sarama.CoordinatorGroup, "g1", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 1000, "foo1", sarama.ErrNoError),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 7, sarama.ErrNoError),
	})

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, nil)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()
	om, err := f.Spawn(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)
	<-om.CommittedOffsets() // Ignore initial offset.

	// When
	om.SubmitOffset(Offset{1001, "bar1"})
	om.SubmitOffset(Offset{1002, "bar2"})
	om.SubmitOffset(Offset{1003, "bar3"})
	om.SubmitOffset(Offset{1004, "bar4"})
	om.SubmitOffset(Offset{1005, "bar5"})
	om.Stop()

	// Then
	var lastCommittedOffset Offset
	for committedOffset := range om.CommittedOffsets() {
		lastCommittedOffset = committedOffset
	}
	c.Assert(lastCommittedOffset, Equals, Offset{1005, "bar5"})
}

// Test for issue https://github.com/mailgun/kafka-pixy/issues/29. The problem
// was that if a connection to the broker was broken on the Kafka side while a
// partition manager tried to retrieve an initial commit, the later would never
// try to reestablish connection and get stuck in an infinite loop of
// unassign->assign to the same broker over and over again.
func (s *OffsetMgrSuite) TestBugConnectionRestored(c *C) {
	broker1 := sarama.NewMockBroker(c, 101)
	defer broker1.Close()
	broker2 := sarama.NewMockBroker(c, 102)

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()).
			SetBroker(broker2.Addr(), broker2.BrokerID()),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(c).
			SetCoordinator(sarama.CoordinatorGroup, "g1", broker2),
	})

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.RetryBackoff = 100 * time.Millisecond
	cfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	saramaCfg := sarama.NewConfig()
	saramaCfg.Net.ReadTimeout = 100 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, saramaCfg)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()

	om, err := f.Spawn(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)

	log.Infof("    GIVEN 1")
	// Make sure the partition offset manager established connection with broker2.
	select {
	case err = <-om.(*offsetMgr).testErrorsCh:
		_, ok := errors.Cause(err).(*net.OpError)
		c.Assert(ok, Equals, true, Commentf("Unexpected or no error: err=%v", err))
	case <-time.After(200 * time.Millisecond):
	}

	log.Infof("    GIVEN 2")
	// Close both broker2 and the partition offset manager. That will break
	// client connection with broker2 from the broker end.
	om.Stop()
	broker2.Close()
	time.Sleep(cfg.Consumer.RetryBackoff * 2)

	log.Infof("    GIVEN 3")
	// Simulate broker restart. Make sure that the new instances listens on the
	// same port as the old one.
	broker2_2 := sarama.NewMockBrokerAddr(c, broker2.BrokerID(), broker2.Addr())
	broker2_2.SetHandlerByMap(map[string]sarama.MockResponse{
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 1000, "foo", sarama.ErrNoError),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 7, sarama.ErrNoError),
	})

	log.Infof("    WHEN")
	// Create a partition offset manager for the same topic partition as before.
	// It will be assigned the broken connection to broker2.
	om, err = f.Spawn(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)
	defer om.Stop()

	log.Infof("    THEN")
	// Then: the new partition offset manager re-establishes connection with
	// broker2 and successfully retrieves the initial offset. Note that there
	// maybe several faulty attempts before an initial offset is successfully
	// retrieved.
	select {
	case initialOffset := <-om.CommittedOffsets():
		c.Assert(initialOffset.Val, Equals, int64(1000),
			Commentf("Failed to retrieve initial offset: %s", err))
	case <-time.After(1 * time.Second):
		c.Errorf("Timeout waiting for initial offset")
	}
}

// Test for issue https://github.com/mailgun/kafka-pixy/issues/62. The problem
// was that if a stop signal is received while there are two submitted offsets
// pending, then offset manager would stop as soon as the first one was
// committed, hereby dropping the second offset.
func (s *OffsetMgrSuite) TestBugOffsetDroppedOnStop(c *C) {
	// Given
	broker1 := sarama.NewMockBroker(c, 101)
	// Set broker latency to ensure proper test timing.
	broker1.SetLatency(200 * time.Millisecond)
	defer broker1.Close()

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(c).
			SetCoordinator(sarama.CoordinatorGroup, "g1", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 1, 1000, "", sarama.ErrNoError),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 1, sarama.ErrNoError),
	})

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.OffsetsCommitInterval = 300 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, nil)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()
	om, err := f.Spawn(s.ns.NewChild("g1", "t1", 1), "g1", "t1", 1)
	c.Assert(err, IsNil)
	time.Sleep(100 * time.Millisecond)
	<-om.CommittedOffsets() // Ignore initial offset.

	// When
	// 0ms: the first offset is submitted;
	om.SubmitOffset(Offset{1001, "bar1"})
	time.Sleep(400 * time.Millisecond)
	// 300ms: broker executor sends OffsetCommitRequest to Kafka
	// 400ms: the second offset is submitted.
	om.SubmitOffset(Offset{1002, "bar2"})
	om.Stop()
	// 500ms: a first offset commit response received from Kafka. Due to a bug
	// the offset manager was quiting here, dropping the second commit.
	// 700ms: a second offset commit response received from Kafka.

	// Then
	var committedOffsets []Offset
	for committedOffset := range om.CommittedOffsets() {
		committedOffsets = append(committedOffsets, committedOffset)
	}
	c.Assert(committedOffsets, DeepEquals, []Offset{{1001, "bar1"}, {1002, "bar2"}})
}

// A snapshot whose metadata exceeds the locally configured byte budget is
// rejected before any broker request is made: the rejection is reported on
// CommitErrors with Local=true, the over-budget payload never reaches the
// coordinator, and a subsequent smaller snapshot commits normally.
func (s *OffsetMgrSuite) TestCommitLocalMetadataOverBudget(c *C) {
	// Given
	broker1 := sarama.NewMockBroker(c, 101)
	defer broker1.Close()

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(c).
			SetCoordinator(sarama.CoordinatorGroup, "g1", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 1000, "", sarama.ErrNoError),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 7, sarama.ErrNoError),
	})

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	// Tight budget: metadata of at most 3 bytes may be committed.
	cfg.Consumer.MaxOffsetMetadataSize = 3
	client, err := sarama.NewClient([]string{broker1.Addr()}, nil)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()

	om, err := f.Spawn(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)
	c.Assert(<-om.CommittedOffsets(), DeepEquals, Offset{1000, ""})

	// When: a snapshot with 9 bytes of metadata is submitted.
	om.SubmitOffset(Offset{1001, "too-large"})

	// Then: it is rejected locally, before reaching the coordinator.
	commitErr := <-om.CommitErrors()
	c.Assert(commitErr.Local, Equals, true)
	c.Assert(errors.Cause(commitErr.Err), Equals, sarama.ErrOffsetMetadataTooLarge)
	c.Assert(commitErr.Offset, DeepEquals, Offset{1001, "too-large"})
	c.Assert(commitErr.Group, Equals, "g1")
	c.Assert(commitErr.Topic, Equals, "t1")
	c.Assert(commitErr.Partition, Equals, int32(7))

	// And: no OffsetCommitRequest carrying the rejected payload is ever sent,
	// not even after several commit intervals.
	time.Sleep(200 * time.Millisecond)
	c.Assert(countCommitRequests(broker1, "t1", 7), Equals, 0)
	// The local rejection is not treated as a connection failure.
	select {
	case err := <-om.(*offsetMgr).testErrorsCh:
		c.Errorf("unexpected reassign error: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	// When: the caller submits a smaller snapshot that fits the budget.
	om.SubmitOffset(Offset{1001, "ok"})
	c.Assert(<-om.CommittedOffsets(), DeepEquals, Offset{1001, "ok"})
	om.Stop()
	for range om.CommittedOffsets() {
	}

	// Then: only the small snapshot was actually persisted.
	c.Assert(lastCommittedOffset(broker1, "g1", "t1", 7),
		DeepEquals, Offset{1001, "ok"})
}

// When a batched commit response rejects one partition with
// ErrOffsetMetadataTooLarge and accepts another, the rejected partition gets
// a CommitErrors notification (Local=false) and its payload is not remapped or
// resent, while the successful partition is committed normally and is not
// affected by the other partition's failure.
func (s *OffsetMgrSuite) TestCommitMetadataTooLargePartialBatch(c *C) {
	// Given
	broker1 := sarama.NewMockBroker(c, 101)
	defer broker1.Close()

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(c).
			SetCoordinator(sarama.CoordinatorGroup, "g1", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 1000, "", sarama.ErrNoError).
			SetOffset("g1", "t1", 8, 2000, "", sarama.ErrNoError),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 7, sarama.ErrOffsetMetadataTooLarge).
			SetError("g1", "t1", 8, sarama.ErrNoError),
	})

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.RetryBackoff = 100 * time.Millisecond
	cfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, nil)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()

	om1, err := f.Spawn(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)
	c.Assert(<-om1.CommittedOffsets(), DeepEquals, Offset{1000, ""})
	om2, err := f.Spawn(s.ns.NewChild("g1", "t1", 8), "g1", "t1", 8)
	c.Assert(err, IsNil)
	c.Assert(<-om2.CommittedOffsets(), DeepEquals, Offset{2000, ""})

	// When: both partitions commit together in the same batch; partition 7
	// is rejected by the coordinator while partition 8 is accepted.
	om1.SubmitOffset(Offset{1001, "foo"})
	om2.SubmitOffset(Offset{2001, "bar"})

	// Then: partition 7 is reported on the dedicated error channel and the
	// cause is distinct from a connection failure.
	commitErr := <-om1.CommitErrors()
	c.Assert(commitErr.Local, Equals, false)
	c.Assert(errors.Cause(commitErr.Err), Equals, sarama.ErrOffsetMetadataTooLarge)
	c.Assert(commitErr.Offset, DeepEquals, Offset{1001, "foo"})
	c.Assert(commitErr.Partition, Equals, int32(7))
	// Then: partition 8's success is not dropped because of partition 7.
	c.Assert(<-om2.CommittedOffsets(), DeepEquals, Offset{2001, "bar"})

	// Then: neither a reassign is triggered for partition 7 nor is the doomed
	// payload resent: after several commit intervals it was sent exactly once.
	select {
	case err := <-om1.(*offsetMgr).testErrorsCh:
		c.Errorf("metadata rejection must not trigger a reassign: %v", err)
	case <-time.After(150 * time.Millisecond):
	}
	c.Assert(countCommitRequestsWith(broker1, "g1", "t1", 7, 1001, "foo"),
		Equals, 1)

	// When: the coordinator starts accepting commits and partition 7 submits
	// a smaller snapshot.
	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"FindCoordinatorRequest": sarama.NewMockFindCoordinatorResponse(c).
			SetCoordinator(sarama.CoordinatorGroup, "g1", broker1),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 7, sarama.ErrNoError).
			SetError("g1", "t1", 8, sarama.ErrNoError),
	})
	om1.SubmitOffset(Offset{1001, ""})
	c.Assert(<-om1.CommittedOffsets(), DeepEquals, Offset{1001, ""})

	// Then: both managers terminate and the broker persists the accepted
	// snapshots, not the rejected metadata.
	om1.Stop()
	om2.Stop()
	c.Assert(lastCommittedOffset(broker1, "g1", "t1", 7),
		DeepEquals, Offset{1001, ""})
	c.Assert(lastCommittedOffset(broker1, "g1", "t1", 8),
		DeepEquals, Offset{2001, "bar"})
}

// lastCommittedOffset traverses the mock broker history backwards searching
// for the OffsetCommitRequest coming from the specified consumer group that
// commits an offset of the specified topic/partition.
func lastCommittedOffset(mb *sarama.MockBroker, group, topic string, partition int32) Offset {
	for i := len(mb.History()) - 1; i >= 0; i-- {
		req, ok := mb.History()[i].Request.(*sarama.OffsetCommitRequest)
		if !ok || req.ConsumerGroup != group {
			continue
		}
		offset, metadata, err := req.Offset(topic, partition)
		if err != nil {
			continue
		}
		return Offset{offset, metadata}
	}
	return Offset{}
}

// countCommitRequests returns how many OffsetCommitRequests in the mock broker
// history carry a block for the specified topic/partition.
func countCommitRequests(mb *sarama.MockBroker, topic string, partition int32) int {
	return countCommitRequestsWith(mb, "", topic, partition, -1, "")
}

// countCommitRequestsWith counts commit requests for a topic/partition that
// carry exactly the given offset and metadata. Pass group "" to match any
// consumer group; pass offset < 0 to ignore the offset/metadata payload.
func countCommitRequestsWith(mb *sarama.MockBroker, group, topic string,
	partition int32, offset int64, metadata string,
) int {
	count := 0
	for _, m := range mb.History() {
		req, ok := m.Request.(*sarama.OffsetCommitRequest)
		if !ok || (group != "" && req.ConsumerGroup != group) {
			continue
		}
		reqOffset, reqMeta, err := req.Offset(topic, partition)
		if err != nil {
			continue
		}
		if offset >= 0 && (reqOffset != offset || reqMeta != metadata) {
			continue
		}
		count++
	}
	return count
}
