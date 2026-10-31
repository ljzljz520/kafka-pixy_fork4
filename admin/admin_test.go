package admin

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/testhelpers"
	"github.com/mailgun/kafka-pixy/testhelpers/kafkahelper"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type AdminSuite struct {
	ns  *actor.Descriptor
	cfg *config.Proxy
	kh  *kafkahelper.T
}

var _ = Suite(&AdminSuite{})

func (s *AdminSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging()
	s.cfg = config.DefaultProxy()
	s.cfg.ClientID = "producer"
	s.cfg.Kafka.SeedPeers = testhelpers.KafkaPeers
	s.cfg.ZooKeeper.SeedPeers = testhelpers.ZookeeperPeers
	s.kh = kafkahelper.New(c)
}

func (s *AdminSuite) TearDownSuite(c *C) {
	s.kh.Close()
}

func (s *AdminSuite) SetUpTest(c *C) {
	s.ns = actor.Root().NewChild("T")
}

// AdminValidateSuite groups pure client-side validation checks. Unlike
// AdminSuite it never connects to Kafka, so its tests are deterministic and
// run without a Vagrant cluster.
type AdminValidateSuite struct{}

var _ = Suite(&AdminValidateSuite{})

// validateCommitOffsets accepts every payload within the byte budget,
// including exact-boundary metadata and empty payloads, and rejects an
// over-budget block with a locate-able error (partition and sizes) before any
// broker request could be made.
func (s *AdminValidateSuite) TestValidateCommitOffsets(c *C) {
	offsets := func(meta ...string) []PartitionOffset {
		pos := make([]PartitionOffset, len(meta))
		for i, m := range meta {
			pos[i] = PartitionOffset{Partition: int32(i), Offset: int64(100 + i), Metadata: m}
		}
		return pos
	}
	for i, tc := range []struct {
		desc          string
		offsets       []PartitionOffset
		maxSize       int
		expectErr     bool
		wantPartition int32
	}{
		0: {"empty payload accepted", nil, 4096, false, -1},
		1: {"all blocks within budget accepted", offsets("a", "bb", "ccc"), 3, false, -1},
		2: {"metadata of exactly max size accepted", offsets("aaa"), 3, false, -1},
		3: {"first offending partition is reported", offsets("ok", "way-too-big"), 3, true, 1},
		4: {"zero budget accepts empty metadata", offsets("", ""), 0, false, -1},
		5: {"zero budget rejects non-empty metadata", offsets("x"), 0, true, 0},
		6: {"negative budget disables the limit", offsets(strings.Repeat("x", 10000)), -1, false, -1},
	} {
		err := validateCommitOffsets(tc.offsets, tc.maxSize)
		if !tc.expectErr {
			c.Assert(err, IsNil, Commentf("case #%d: %s", i, tc.desc))
			continue
		}
		c.Assert(err, NotNil, Commentf("case #%d: %s", i, tc.desc))
		// The pre-request rejection is an invalid-parameter error, the same
		// kind the HTTP/gRPC layers map to a client error.
		_, ok := err.(ErrInvalidParam)
		c.Assert(ok, Equals, true, Commentf("case #%d: %s", i, tc.desc))
		msg := err.Error()
		c.Assert(strings.Contains(msg, "offset metadata too large"), Equals, true,
			Commentf("case #%d: %s", i, tc.desc))
		c.Assert(strings.Contains(msg, fmt.Sprintf("partition=%d", tc.wantPartition)),
			Equals, true, Commentf("case #%d: %s", i, tc.desc))
		c.Assert(strings.Contains(msg, "max_size=3") || strings.Contains(msg, "max_size=0"),
			Equals, true, Commentf("case #%d: %s", i, tc.desc))
	}
}

// SetGroupOffsets rejects an over-budget payload before touching any broker
// connection: with no Kafka seed peer configured the call still fails fast
// with the locate-able invalid-parameter error rather than a connection error
// or a client reset/retry.
func (s *AdminValidateSuite) TestSetGroupOffsetsRejectsOverBudgetBeforeRequest(c *C) {
	cfg := config.DefaultProxy()
	cfg.Kafka.SeedPeers = nil
	cfg.Consumer.MaxOffsetMetadataSize = 2
	a, err := Spawn(actor.Root().NewChild("T"), cfg)
	c.Assert(err, IsNil)
	defer a.Stop()

	err = a.SetGroupOffsets("g1", "t1", []PartitionOffset{
		{Partition: 9, Offset: 1001, Metadata: "legal"},
	})
	c.Assert(err, NotNil)
	_, ok := err.(ErrInvalidParam)
	c.Assert(ok, Equals, true)
	c.Assert(strings.Contains(err.Error(), "partition=9"), Equals, true)
	c.Assert(strings.Contains(err.Error(), "max_size=2"), Equals, true)
}

// The end offset of partition ranges is properly reflects the number of
// messages produced since the previous check.
func (s *AdminSuite) TestGetOffsetsAfterProduce(c *C) {
	// Given
	keyToCount := make(map[string]int, 64)
	for i := 0; i < 64; i++ {
		keyToCount[strconv.Itoa(i)] = i
	}
	a, err := Spawn(s.ns, s.cfg)
	c.Assert(err, IsNil)
	offsetsBefore, err := a.GetGroupOffsets("foo", "test.64")
	c.Assert(err, IsNil)
	s.kh.PutMessages("get_offsets", "test.64", keyToCount)

	// When
	_, _ = a.GetGroupOffsets("foo", "test.64")
	// It takes sometime the production result to be reflected in get metadata
	// output. So the first query sometimes returns unexpected results. So we
	// make the second request in lame attempt to make this test deterministic.
	offsetsAfter, err := a.GetGroupOffsets("foo", "test.64")
	c.Assert(err, IsNil)

	// Then
	rangeEndDiffs := []int{
		0, 75, 3, 0, 57, 0, 0, 8, 58, 6, 30, 59, 0, 75, 63, 0, 48, 37, 49, 0,
		75, 2, 0, 51, 0, 0, 0, 79, 1, 33, 61, 39, 75, 62, 0, 4, 36, 0, 0, 79,
		61, 28, 53, 35, 18, 0, 79, 0, 32, 63, 38, 0, 9, 59, 7, 31, 14, 0, 79, 60,
		29, 55, 34, 67,
	}
	for i := 0; i < 64; i++ {
		actualDiff := int(offsetsAfter[i].End - offsetsBefore[i].End)
		if actualDiff != rangeEndDiffs[i] {
			c.Errorf("partition %d, want=%d, got=%d", i, rangeEndDiffs[i], actualDiff)
		}
	}
	a.Stop()
}

// It is possible to set offsets for only a subset of group/topic partitions.
func (s *AdminSuite) TestSetOffsetsPartialUpdate(c *C) {
	// Given
	a, err := Spawn(s.ns, s.cfg)
	c.Assert(err, IsNil)
	a.SetGroupOffsets("foo", "test.4", []PartitionOffset{
		{Partition: 0, Offset: 1001, Metadata: "A1"},
		{Partition: 1, Offset: 1002, Metadata: "A2"},
		{Partition: 2, Offset: 1003, Metadata: "A3"},
		{Partition: 3, Offset: 1004, Metadata: "A4"},
	})

	// When
	a.SetGroupOffsets("foo", "test.4", []PartitionOffset{
		{Partition: 0, Offset: 2001, Metadata: "B1"},
		{Partition: 3, Offset: 2004, Metadata: "B4"},
	})

	// Then
	offsets, err := a.GetGroupOffsets("foo", "test.4")
	c.Assert(err, IsNil)
	c.Assert(offsets[0].Offset, Equals, int64(2001))
	c.Assert(offsets[1].Offset, Equals, int64(1002))
	c.Assert(offsets[2].Offset, Equals, int64(1003))
	c.Assert(offsets[3].Offset, Equals, int64(2004))
	c.Assert(offsets[0].Metadata, Equals, "B1")
	c.Assert(offsets[1].Metadata, Equals, "A2")
	c.Assert(offsets[2].Metadata, Equals, "A3")
	c.Assert(offsets[3].Metadata, Equals, "B4")

	a.Stop()
}
