package offsettrk

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/offsetmgr"
	"github.com/mailgun/kafka-pixy/testhelpers"
	. "gopkg.in/check.v1"
)

type action int

const (
	doOfr action = iota
	doAck
)

var _ = Suite(&OffsetTrkSuite{})

type OffsetTrkSuite struct {
	ns *actor.Descriptor
}

func Test(t *testing.T) {
	TestingT(t)
}

func (s *OffsetTrkSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging()
}

func (s *OffsetTrkSuite) SetUpTest(c *C) {
	s.ns = actor.Root().NewChild("T")
}

// Acknowledged offsets are properly reflected in ackedRanges.
func (s *OffsetTrkSuite) TestOnAckedRanges(c *C) {
	ot := New(s.ns, offsetmgr.Offset{Val: 300}, -1, -1)
	for i, tc := range []struct {
		acked     int64
		committed int64
		ranges    string
	}{
		0: {acked: 299, committed: 300, ranges: ""},
		1: {acked: 300, committed: 301, ranges: ""},
		2: {acked: 302, committed: 301, ranges: "1-2"},
		3: {acked: 310, committed: 301, ranges: "1-2,9-10"},
		4: {acked: 311, committed: 301, ranges: "1-2,9-11"},
		5: {acked: 307, committed: 301, ranges: "1-2,6-7,9-11"},
		6: {acked: 305, committed: 301, ranges: "1-2,4-5,6-7,9-11"},
		7: {acked: 304, committed: 301, ranges: "1-2,3-5,6-7,9-11"},
		// Acking an acked offset makes no difference.
		8:  {acked: 305, committed: 301, ranges: "1-2,3-5,6-7,9-11"},
		9:  {acked: 308, committed: 301, ranges: "1-2,3-5,6-8,9-11"},
		10: {acked: 303, committed: 301, ranges: "1-5,6-8,9-11"},
		11: {acked: 309, committed: 301, ranges: "1-5,6-11"},
		12: {acked: 306, committed: 301, ranges: "1-11"},
		13: {acked: 301, committed: 312, ranges: ""},
		14: {acked: 398, committed: 312, ranges: "86-87"},
		15: {acked: 399, committed: 312, ranges: "86-88"},
		16: {acked: 4496, committed: 312, ranges: "86-88,4184-4185"},
		17: {acked: 0x7FFFFFFFFFFFFFFE, committed: 312, ranges: "86-88,4184-4185,9223372036854775494-9223372036854775495"},
	} {
		// When
		offset, _ := ot.OnAcked(tc.acked)
		ot2 := New(s.ns, offset, -1, -1)

		// Then
		c.Assert(offset.Val, Equals, tc.committed, Commentf("case #%d", i))
		c.Assert(SparseAcks2Str(offset), Equals, tc.ranges, Commentf("case #%d", i))
		// Both nil and empty slice ack ranges become nil when going through
		// the encode/decode circle.
		if ot.ackedRanges == nil || len(ot.ackedRanges) == 0 {
			c.Assert(ot2.ackedRanges, IsNil, Commentf("case #%d", i))
		} else {
			c.Assert(ot2.ackedRanges, DeepEquals, ot.ackedRanges, Commentf("case #%d", i))
		}
	}
}

// When an offset is adjusted, then acked ranges are respectively adjusted too.
func (s *OffsetTrkSuite) TestAdjust(c *C) {
	initialOffset := int64(300)
	for i, tc := range []struct {
		offset    int64
		ranges    string
		offered   int
		nextOffer int64
	}{
		0:  {offset: 300, ranges: "2-4,5-9,12-13,14-16", offered: 8, nextOffer: 300},
		1:  {offset: 301, ranges: "1-3,4-8,11-12,13-15", offered: 7, nextOffer: 301},
		2:  {offset: 304, ranges: "1-5,8-9,10-12", offered: 6, nextOffer: 304},
		3:  {offset: 304, ranges: "1-5,8-9,10-12", offered: 6, nextOffer: 304},
		4:  {offset: 304, ranges: "1-5,8-9,10-12", offered: 6, nextOffer: 304},
		5:  {offset: 309, ranges: "3-4,5-7", offered: 5, nextOffer: 309},
		6:  {offset: 309, ranges: "3-4,5-7", offered: 5, nextOffer: 309},
		7:  {offset: 309, ranges: "3-4,5-7", offered: 5, nextOffer: 309},
		8:  {offset: 309, ranges: "3-4,5-7", offered: 5, nextOffer: 309},
		9:  {offset: 309, ranges: "3-4,5-7", offered: 5, nextOffer: 309},
		10: {offset: 310, ranges: "2-3,4-6", offered: 4, nextOffer: 310},
		11: {offset: 311, ranges: "1-2,3-5", offered: 3, nextOffer: 311},
		12: {offset: 313, ranges: "1-3", offered: 2, nextOffer: 313},
		13: {offset: 313, ranges: "1-3", offered: 2, nextOffer: 313},
		14: {offset: 316, ranges: "", offered: 1, nextOffer: 316},
		15: {offset: 316, ranges: "", offered: 1, nextOffer: 316},
		16: {offset: 316, ranges: "", offered: 1, nextOffer: 316},
		17: {offset: 317, ranges: "", offered: 0},
	} {
		correction := initialOffset + int64(i)
		ot := New(s.ns, offsetmgr.Offset{Val: initialOffset}, -1, -1)
		for j, acked := range []int{0, 0, 1, 1, 0, 1, 1, 1, 1, 0, 0, 0, 1, 0, 1, 1, 0} {
			offset := initialOffset + int64(j)
			ot.OnOffered(msg(offset))
			if acked == 1 {
				ot.OnAcked(offset)
			}
		}
		c.Assert(SparseAcks2Str(ot.offset), Equals, "2-4,5-9,12-13,14-16")

		// When
		adjustedOffset, offeredCount := ot.Adjust(correction)

		// Then
		c.Assert(adjustedOffset.Val, Equals, tc.offset, Commentf("case #%d", i))
		c.Assert(SparseAcks2Str(ot.offset), Equals, tc.ranges, Commentf("case #%d", i))
		c.Assert(offeredCount, Equals, tc.offered, Commentf("case #%d", i))

		msg, _, ok := ot.nextRetry(time.Now())
		if tc.offered > 0 {
			c.Assert(ok, Equals, true)
			c.Assert(msg.Offset, Equals, tc.nextOffer, Commentf("case #%d", i))
		} else {
			c.Assert(ok, Equals, false)
		}
	}
}

func (s *OffsetTrkSuite) TestRangeEncodeDecode(c *C) {
	for i, tc := range []struct {
		base int64
		ar   offsetRange
	}{
		0: {0, offsetRange{1, 2}},
		1: {100, offsetRange{101, 102}},
		2: {0, offsetRange{0x7FFFFFFFFFFFFFFE, 0x7FFFFFFFFFFFFFFF}},
	} {
		var encoded []byte
		var decoded offsetRange

		// When
		encoded = tc.ar.encode(tc.base, encoded)
		decoded.decode(tc.base, encoded)

		// Then
		c.Assert(decoded, Equals, tc.ar, Commentf("case #%d", i))
	}
}

func (s *OffsetTrkSuite) TestRangeDecodeError(c *C) {
	var ar offsetRange
	for i, tc := range []struct {
		encoded string
		error   string
	}{
		0: {"AA", "bad range: {10000 10000}"},
		1: {"0123456789012a", "bad `from` boundary: bad sequence: 0123456789012"},
		2: {"a0123456789012b", "bad `to` boundary: bad sequence: 0123456789012"},
		3: {"1@b", "bad `from` boundary: bad char: @"},
		4: {"a@b", "bad `to` boundary: bad char: @"},
	} {
		// When
		buf, err := ar.decode(10000, []byte(tc.encoded))

		// Then
		c.Assert(err.Error(), Equals, tc.error, Commentf("case #%d", i))
		c.Assert(buf, IsNil, Commentf("case #%d", i))
	}
}

func (s *OffsetTrkSuite) TestNew(c *C) {
	for i, tc := range []struct {
		given  offsetmgr.Offset
		actual offsetmgr.Offset
	}{
		0: {
			offsetmgr.Offset{Val: 1000, Meta: ""},
			offsetmgr.Offset{Val: 1000, Meta: ""},
		},
		1: {
			offsetmgr.Offset{Val: 1000, Meta: "abra1234+/P"},
			offsetmgr.Offset{Val: 1000, Meta: "abra1234+/P"},
		},
		2: {
			offsetmgr.Offset{Val: 1000, Meta: "abra1234+/PS"},
			offsetmgr.Offset{Val: 1000, Meta: ""},
		},
		3: {
			offsetmgr.Offset{Val: 1000, Meta: "a@b"},
			offsetmgr.Offset{Val: 1000, Meta: ""},
		},
	} {
		// When
		ot := New(s.ns, tc.given, -1, -1)

		// Then
		c.Assert(ot.offset, Equals, tc.actual, Commentf("case #%d", i))
	}
}

func (s *OffsetTrkSuite) TestIsAcked(c *C) {
	meta := encodeAckedRanges(301, []offsetRange{
		{302, 305}, {307, 309}, {310, 313}}, -1)
	offset := offsetmgr.Offset{Val: 301, Meta: meta}
	ot := New(s.ns, offset, -1, -1)
	for i, tc := range []struct {
		offset       int64
		isAcked      bool
		nextNotAcked int64
	}{
		0:  {offset: 299, isAcked: true, nextNotAcked: 301},
		1:  {offset: 300, isAcked: true, nextNotAcked: 301},
		2:  {offset: 301, isAcked: false, nextNotAcked: 305},
		3:  {offset: 302, isAcked: true, nextNotAcked: 305},
		4:  {offset: 303, isAcked: true, nextNotAcked: 305},
		5:  {offset: 304, isAcked: true, nextNotAcked: 305},
		6:  {offset: 305, isAcked: false, nextNotAcked: 306},
		7:  {offset: 306, isAcked: false, nextNotAcked: 309},
		8:  {offset: 307, isAcked: true, nextNotAcked: 309},
		9:  {offset: 308, isAcked: true, nextNotAcked: 309},
		10: {offset: 309, isAcked: false, nextNotAcked: 313},
		11: {offset: 310, isAcked: true, nextNotAcked: 313},
		12: {offset: 311, isAcked: true, nextNotAcked: 313},
		13: {offset: 312, isAcked: true, nextNotAcked: 313},
		14: {offset: 313, isAcked: false, nextNotAcked: 314},
		15: {offset: 314, isAcked: false, nextNotAcked: 315},
	} {
		// When
		isAcked, nextNotAcked := ot.IsAcked(tc.offset)
		// /Then
		c.Assert(isAcked, Equals, tc.isAcked, Commentf("case #%d", i))
		c.Assert(nextNotAcked, Equals, tc.nextNotAcked, Commentf("case #%d", i))
	}
}

func (s *OffsetTrkSuite) TestOfferAckLoop(c *C) {
	ot := New(s.ns, offsetmgr.Offset{Val: 300}, -1, -1)
	for i, tc := range []struct {
		act       action
		offset    int64
		count     int
		committed int64
		ranges    string
	}{
		0: {act: doOfr, offset: 298, count: 0},
		1: {act: doOfr, offset: 299, count: 0},
		// Test offering in arbitrary order, even though it never happens in
		// real life.
		2: {act: doOfr, offset: 301, count: 1},
		3: {act: doOfr, offset: 300, count: 2},
		4: {act: doOfr, offset: 304, count: 3},
		5: {act: doOfr, offset: 302, count: 4},
		6: {act: doOfr, offset: 303, count: 5},
		// And a bit of regular sequential offering
		7: {act: doOfr, offset: 305, count: 6},
		8: {act: doOfr, offset: 306, count: 7},
		// Ack some messages
		9:  {act: doAck, offset: 307, count: 7, committed: 300, ranges: "7-8"},
		10: {act: doAck, offset: 299, count: 7, committed: 300, ranges: "7-8"},
		11: {act: doAck, offset: 303, count: 6, committed: 300, ranges: "3-4,7-8"},
		12: {act: doAck, offset: 300, count: 5, committed: 301, ranges: "2-3,6-7"},
		// Offer acked again
		13: {act: doOfr, offset: 298, count: 5},
		14: {act: doOfr, offset: 305, count: 5},
		// Offer offered (ignored)
		15: {act: doOfr, offset: 302, count: 5},
		16: {act: doOfr, offset: 305, count: 5},
		17: {act: doOfr, offset: 306, count: 5},
		//        Ack acked (ignored)
		18: {act: doAck, offset: 299, count: 5, committed: 301, ranges: "2-3,6-7"},
		19: {act: doAck, offset: 300, count: 5, committed: 301, ranges: "2-3,6-7"},
		//        Offer/Ack some more
		20: {act: doOfr, offset: 309, count: 6},
		21: {act: doAck, offset: 302, count: 5, committed: 301, ranges: "1-3,6-7"},
		22: {act: doAck, offset: 301, count: 4, committed: 304, ranges: "3-4"},
		23: {act: doOfr, offset: 308, count: 5},
		24: {act: doAck, offset: 306, count: 4, committed: 304, ranges: "2-4"},
	} {
		var count int
		// When
		switch tc.act {
		case doOfr:
			count = ot.OnOffered(msg(tc.offset))
		case doAck:
			var offset offsetmgr.Offset
			offset, count = ot.OnAcked(tc.offset)
			c.Assert(offset.Val, Equals, tc.committed, Commentf("case #%d", i))
			c.Assert(SparseAcks2Str(offset), Equals, tc.ranges, Commentf("case #%d", i))
		}
		// Then
		c.Assert(count, Equals, tc.count, Commentf("case #%d", i))
	}
}

func (s *OffsetTrkSuite) TestNextRetry(c *C) {
	ot := New(s.ns, offsetmgr.Offset{Val: 300}, 5*time.Second, -1)
	msgs := []consumer.Message{
		msg(300),
		msg(301),
		msg(302),
	}
	begin := time.Now()
	for _, msg := range msgs {
		ot.OnOffered(msg)
	}
	ot.offers[0].deadline = begin.Add(5 * time.Second)
	ot.offers[1].deadline = begin.Add(7 * time.Second)
	ot.offers[2].deadline = begin.Add(9 * time.Second)

	for i, tc := range []struct {
		millis     int
		offset     int64
		retryCount int
	}{
		0: {millis: 0},
		1: {millis: 1000},
		2: {millis: 5000},
		3: {millis: 5001, offset: 300, retryCount: 1},
		4: {millis: 7000},
		// The next attempt deadline is counted from the time when NextRetry
		// returns the message.
		5:  {millis: 9001, offset: 301, retryCount: 1},
		6:  {millis: 9003, offset: 302, retryCount: 1},
		7:  {millis: 10000},
		8:  {millis: 20000, offset: 300, retryCount: 2},
		9:  {millis: 20002, offset: 301, retryCount: 2},
		10: {millis: 20004, offset: 302, retryCount: 2},
		11: {millis: 25000},
		12: {millis: 25001, offset: 300, retryCount: 3},
		13: {millis: 25002},
		14: {millis: 25003, offset: 301, retryCount: 3},
		15: {millis: 25004},
		16: {millis: 25005, offset: 302, retryCount: 3},
		17: {millis: 25006},
	} {
		// When
		now := begin.Add(time.Duration(tc.millis) * time.Millisecond)
		msg, retryCount, ok := ot.nextRetry(now)

		// Then
		if ok {
			c.Assert(msg.Offset, Equals, tc.offset, Commentf("case #%d", i))
			c.Assert(retryCount, Equals, tc.retryCount, Commentf("case #%d", i))
		} else {
			c.Assert(tc.offset, Equals, int64(0), Commentf("case #%d", i))
			c.Assert(retryCount, Equals, -1, Commentf("case #%d", i))
		}
	}
}

func (s *OffsetTrkSuite) TestMaxOfferTimeout(c *C) {
	ot := New(s.ns, offsetmgr.Offset{Val: 300}, -1, -1)
	msgs := []consumer.Message{
		msg(300),
		msg(301),
		msg(302),
		msg(303),
	}
	begin := time.Now()
	for _, msg := range msgs {
		ot.OnOffered(msg)
	}
	ot.offers[0].deadline = begin.Add(3 * time.Second)
	ot.offers[1].deadline = begin.Add(9 * time.Second)
	ot.offers[2].deadline = begin.Add(4 * time.Second)

	for i, tc := range []struct {
		millis  int
		wait    bool
		timeout int
	}{
		0:  {millis: 0, timeout: 9000},
		1:  {millis: 2998, timeout: 6002},
		2:  {millis: 2999, timeout: 6001},
		3:  {millis: 3000, timeout: 6000},
		4:  {millis: 3001, timeout: 5999},
		5:  {millis: 3999, timeout: 5001},
		6:  {millis: 4000, timeout: 5000},
		7:  {millis: 8950, timeout: 50},
		8:  {millis: 8999, timeout: 1},
		9:  {millis: 9000, timeout: 0},
		10: {millis: 9001, timeout: 0},
		11: {millis: 9999, timeout: 0},
	} {
		// When/Then
		now := begin.Add(time.Duration(tc.millis) * time.Millisecond)
		timeout := ot.shouldWait4Ack(now)
		c.Assert(timeout, Equals, time.Duration(tc.timeout)*time.Millisecond, Commentf("case #%d", i))
	}
}

func msg(offset int64) consumer.Message {
	return consumer.Message{ConsumerMessage: sarama.ConsumerMessage{Offset: offset}}
}

// Metadata produced for a tight byte budget contains only complete acked
// ranges nearest the frontier, is never truncated mid-range and leaves the
// frontier untouched.
func (s *OffsetTrkSuite) TestCheckpointBudget(c *C) {
	ot := New(s.ns, offsetmgr.Offset{Val: 300}, -1, -1)
	// Build five distinct ranges: [301,303) [305,307) [310,313)
	// [340,342) [400,403).
	for _, offset := range []int64{301, 302, 305, 306, 310, 311, 312, 340, 341, 400, 401, 402} {
		ot.OnOffered(msg(offset))
	}
	for _, offset := range []int64{400, 340, 310, 305, 301, 311, 302, 401, 306, 341, 312, 402} {
		ot.OnAcked(offset)
	}
	full := []offsetRange{{301, 303}, {305, 307}, {310, 313}, {340, 342}, {400, 403}}
	c.Assert(ot.ackedRanges, DeepEquals, full)

	unbounded := encodeAckedRanges(300, full, -1)
	// Each range is encoded against the end of the previous range, so the
	// per-range costs here are 2,2,2,2,3 bytes (cumulative 2,4,6,8,11):
	// the 27-wide gap before [340,342) still fits one 5-bit token, while
	// the 58-wide gap before [400,403) requires a continuation token.
	c.Assert(unbounded, Equals, "BCCCDDbC6BD")
	for i, tc := range []struct {
		budget        int
		expectRanges  []offsetRange
		expectMetaLen int
	}{
		// Budget 0: only the continuous frontier is persisted.
		0: {0, nil, 0},
		// One byte does not fit a whole range (each costs 2 bytes here).
		1: {1, nil, 0},
		// Two bytes fit exactly one complete range, nearest first.
		2: {2, []offsetRange{{301, 303}}, 2},
		// Four bytes fit two complete ranges.
		3: {4, []offsetRange{{301, 303}, {305, 307}}, 4},
		// Six bytes fit three complete ranges.
		4: {6, []offsetRange{{301, 303}, {305, 307}, {310, 313}}, 6},
		// A budget between range costs never produces a truncated range.
		5: {7, []offsetRange{{301, 303}, {305, 307}, {310, 313}}, 6},
		// Eight bytes fit exactly four complete ranges.
		6: {8, []offsetRange{{301, 303}, {305, 307}, {310, 313}, {340, 342}}, 8},
		// Ten bytes still fit only four: the last range costs 3 bytes and
		// 8+3 > 10.
		7: {10, []offsetRange{{301, 303}, {305, 307}, {310, 313}, {340, 342}}, 8},
		// Eleven bytes fit all five complete ranges.
		8: {11, full, 11},
	} {
		// When
		ot.metaBudget = tc.budget
		snapshot := ot.Checkpoint()

		// Then: the frontier is unchanged.
		c.Assert(snapshot.Val, Equals, int64(300), Commentf("case #%d", i))
		// Then: the metadata fits the budget.
		c.Assert(len(snapshot.Meta) <= tc.budget || tc.budget == 0, Equals, true,
			Commentf("case #%d: meta=%q len=%d budget=%d", i, snapshot.Meta, len(snapshot.Meta), tc.budget))
		if tc.expectMetaLen == 0 {
			c.Assert(snapshot.Meta, Equals, "", Commentf("case #%d", i))
		} else {
			c.Assert(len(snapshot.Meta), Equals, tc.expectMetaLen,
				Commentf("case #%d: meta=%q", i, snapshot.Meta))
		}
		// Then: the metadata is fully decodable (no truncated tokens) and
		// contains exactly the expected near-frontier ranges.
		decoded, err := decodeAckedRanges(300, snapshot.Meta)
		c.Assert(err, IsNil, Commentf("case #%d", i))
		if tc.expectRanges == nil {
			c.Assert(decoded, IsNil, Commentf("case #%d", i))
		} else {
			c.Assert(decoded, DeepEquals, tc.expectRanges, Commentf("case #%d", i))
		}
	}

	// The unbounded encoding is strictly a superset of every budgeted one.
	ot.metaBudget = -1
	c.Assert(ot.Checkpoint().Meta, Equals, unbounded)
}

// A checkpoint persisted with the historical encoding (no version prefix,
// relative base64 varints) keeps being decoded byte for byte; no format
// migration is introduced.
func (s *OffsetTrkSuite) TestCheckpointOldFormatRead(c *C) {
	// "BBHB" is the historical wire encoding of ranges [302,303) and
	// [310,311) relative to base 301, rendered by SparseAcks2Str as
	// "1-2,9-10".
	ot := New(s.ns, offsetmgr.Offset{Val: 301, Meta: "BBHB"}, -1, -1)
	c.Assert(ot.ackedRanges, DeepEquals, []offsetRange{{302, 303}, {310, 311}})
	c.Assert(ot.Checkpoint().Meta, Equals, "BBHB")
	c.Assert(SparseAcks2Str(ot.Checkpoint()), Equals, "1-2,9-10")
}

// Thousands of interleaved acknowledgements under a tiny byte budget always
// produce decodable checkpoints that never cross an unacknowledged gap and
// encode only a near-frontier prefix of the complete in-memory ranges.
func (s *OffsetTrkSuite) TestCheckpointBudgetThousandsAcks(c *C) {
	const (
		base   int64 = 1000
		n            = 5000
		budget       = 16
	)
	// Deterministic permutation of ack order, so that acks are interleaved
	// with gaps and the run is reproducible.
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	seed := int64(987654321)
	nextRand := func() int {
		seed = (seed*1103515245 + 12345) & 0x7fffffff
		return int(seed)
	}
	for i := n - 1; i > 0; i-- {
		j := nextRand() % (i + 1)
		order[i], order[j] = order[j], order[i]
	}

	acked := make([]bool, n)
	frontier := func() int64 {
		f := 0
		for f < n && acked[f] {
			f++
		}
		return base + int64(f)
	}
	ot := New(s.ns, offsetmgr.Offset{Val: base}, -1, budget)
	for i := 0; i < n; i++ {
		ot.OnOffered(msg(base + int64(i)))
	}

	// Pass 1: ack in the shuffled order, leaving every 7th offset unacked.
	for k, i := range order {
		if i%7 == 3 {
			continue
		}
		acked[i] = true
		snapshot, _ := ot.OnAcked(base + int64(i))
		assertCheckpointInvariants(c, ot, snapshot, acked, base, budget,
			"pass1", k, i)
	}

	// Full in-memory state must be strictly richer than the 16-byte snapshot.
	c.Assert(len(ot.ackedRanges) > 0, Equals, true)
	snapshot := ot.Checkpoint()
	persisted, err := decodeAckedRanges(snapshot.Val, snapshot.Meta)
	c.Assert(err, IsNil)
	c.Assert(len(persisted) < len(ot.ackedRanges), Equals, true)
	c.Assert(snapshot.Val, Equals, frontier())

	// Pass 2: fill half of the remaining gaps, in descending order. Every
	// merge advances the frontier and the freed encoding space is reused for
	// ranges nearer the new frontier.
	for i := n - 1; i >= 0; i-- {
		if i%7 != 3 || (i/7)%2 != 0 {
			continue
		}
		acked[i] = true
		snapshot, _ := ot.OnAcked(base + int64(i))
		assertCheckpointInvariants(c, ot, snapshot, acked, base, budget,
			"pass2", 0, i)
	}
	c.Assert(ot.Checkpoint().Val, Equals, frontier())
}

func assertCheckpointInvariants(c *C, ot *T, snapshot offsetmgr.Offset,
	acked []bool, base int64, budget int, stage string, k, i int,
) {
	comment := Commentf("%s k=%d i=%d snapshot=%s", stage, k, i, SparseAcks2Str(snapshot))
	// 1. The persisted metadata never exceeds the byte budget.
	c.Assert(len(snapshot.Meta) <= budget, Equals, true, comment)
	// 2. It is fully decodable: bytes are never truncated mid-range.
	decoded, err := decodeAckedRanges(snapshot.Val, snapshot.Meta)
	c.Assert(err, IsNil, comment)
	// 3. Decoded ranges are exactly the near-frontier prefix of the complete
	//    in-memory acked ranges that fits the budget.
	for idx, ar := range decoded {
		c.Assert(ar, Equals, ot.ackedRanges[idx], comment)
		for off := ar.from; off < ar.to; off++ {
			idx := int(off - base)
			c.Assert(idx >= 0 && idx < len(acked) && acked[idx], Equals, true, comment)
		}
	}
	if len(decoded) < len(ot.ackedRanges) {
		// The first omitted range must not fit anymore (greedy prefix).
		head := encodeAckedRanges(snapshot.Val, decoded, budget)
		next := ot.ackedRanges[len(decoded)].encode(
			decodedBase(snapshot.Val, decoded), nil)
		c.Assert(len(head)+len(next) > budget, Equals, true, comment)
	}
	// 4. The continuous frontier never crosses an unacknowledged gap.
	f := 0
	for f < len(acked) && acked[f] {
		f++
	}
	c.Assert(snapshot.Val, Equals, base+int64(f), comment)
}

func decodedBase(base int64, ranges []offsetRange) int64 {
	for _, ar := range ranges {
		base = ar.to
	}
	return base
}

// After a crash only the persisted checkpoint survives: acked ranges omitted
// from it may be redelivered (conservative duplicates), but never-acked gaps
// remain unacknowledged and readable. Acknowledging the earliest gap merges
// ranges, advances the frontier and reclaims the metadata budget.
func (s *OffsetTrkSuite) TestCheckpointCrashRecovery(c *C) {
	const budget = 8
	ot := New(s.ns, offsetmgr.Offset{Val: 100}, -1, budget)
	for _, off := range []int64{101, 102, 105, 106, 110, 111, 112, 150, 151, 300, 301, 302} {
		ot.OnOffered(msg(off))
	}
	for _, off := range []int64{300, 150, 110, 105, 101, 111, 102, 301, 106, 151, 112, 302} {
		ot.OnAcked(off)
	}
	fullRanges := []offsetRange{{101, 103}, {105, 107}, {110, 113}, {150, 152}, {300, 303}}
	c.Assert(ot.ackedRanges, DeepEquals, fullRanges)

	crashSnapshot := ot.Checkpoint()
	c.Assert(len(crashSnapshot.Meta) <= budget, Equals, true)
	persisted, err := decodeAckedRanges(crashSnapshot.Val, crashSnapshot.Meta)
	c.Assert(err, IsNil)
	c.Assert(len(persisted) < len(fullRanges), Equals, true,
		Commentf("some far acked ranges must have been omitted"))

	// Simulate a restart: nothing but the persisted checkpoint is available.
	restarted := New(s.ns, crashSnapshot, -1, budget)
	c.Assert(restarted.offset.Val, Equals, int64(100),
		Commentf("frontier must not jump over the unacked gap 100"))

	// Every persisted range is still known as acked.
	for _, ar := range persisted {
		for off := ar.from; off < ar.to; off++ {
			acked, _ := restarted.IsAcked(off)
			c.Assert(acked, Equals, true)
		}
	}

	// Find an acked-but-omitted offset: after the crash it is allowed to be
	// redelivered, i.e. reported as not acked (conservative duplicate).
	persistedSet := map[int64]bool{}
	for _, ar := range persisted {
		for off := ar.from; off < ar.to; off++ {
			persistedSet[off] = true
		}
	}
	var omittedAcked int64 = -1
	for _, ar := range fullRanges {
		for off := ar.from; off < ar.to; off++ {
			if !persistedSet[off] {
				omittedAcked = off
			}
		}
	}
	c.Assert(omittedAcked >= 0, Equals, true)
	isAcked, _ := restarted.IsAcked(omittedAcked)
	c.Assert(isAcked, Equals, false,
		Commentf("omitted acked offset %d is a legal duplicate after restart", omittedAcked))

	// Unacknowledged messages, including the frontier gap and the holes
	// between ranges, are still readable: they must not be considered acked.
	for _, gap := range []int64{100, 103, 104, 107, 113, 149} {
		isAcked, _ := restarted.IsAcked(gap)
		c.Assert(isAcked, Equals, false, Commentf("gap %d must remain unacked", gap))
	}

	// Acknowledge the earliest gap: ranges merge, the frontier advances over
	// complete intervals and the resulting checkpoint stays within budget.
	restarted.OnOffered(msg(100))
	snapshot, _ := restarted.OnAcked(100)
	c.Assert(snapshot.Val, Equals, int64(103), Commentf("frontier advances: %d", snapshot.Val))
	c.Assert(len(snapshot.Meta) <= budget, Equals, true)

	// Ranges omitted from the crash checkpoint are forgotten after restart,
	// so their records are redelivered as conservative duplicates. Once the
	// consumer re-acks them, the budget reclaimed by the advancing frontier
	// lets [150,152) be persisted again: before the advance base was 100 and
	// the first three ranges already cost 6 bytes, leaving no room for the
	// 3-byte [150,152) range (9 > 8); afterwards base is 103 and the first
	// range was absorbed by the frontier.
	for _, off := range []int64{150, 151} {
		restarted.OnOffered(msg(off))
	}
	for _, off := range []int64{150, 151} {
		snapshot, _ = restarted.OnAcked(off)
	}
	c.Assert(len(snapshot.Meta) <= budget, Equals, true)
	decoded, err := decodeAckedRanges(snapshot.Val, snapshot.Meta)
	c.Assert(err, IsNil)
	recovered := false
	for _, ar := range decoded {
		if ar.from == 150 && ar.to == 152 {
			recovered = true
		}
	}
	c.Assert(recovered, Equals, true,
		Commentf("frontier advance reuses the budget: %s", SparseAcks2Str(snapshot)))
	// And the frontier still stops at the next unacknowledged gap (103).
	isAcked, _ = restarted.IsAcked(103)
	c.Assert(isAcked, Equals, false)
}

// ShrinkCheckpoint converges to a frontier-only snapshot in a bounded number
// of strictly-smaller steps; once the frontier gap is acknowledged the
// tightened budget is reused by the advancing frontier.
func (s *OffsetTrkSuite) TestShrinkCheckpointBounded(c *C) {
	ot := New(s.ns, offsetmgr.Offset{Val: 0}, -1, 4096)
	for i := 0; i < 2000; i++ {
		ot.OnOffered(msg(int64(i)))
	}
	// Sparse acks with many small gaps produce a sizable metadata string.
	for i := 0; i < 2000; i += 2 {
		ot.OnAcked(int64(i))
	}
	initial := ot.Checkpoint()
	c.Assert(len(initial.Meta) > 100, Equals, true)

	prevLen := len(initial.Meta)
	steps := 0
	for {
		retry, retriable := ot.ShrinkCheckpoint()
		if !retriable {
			c.Assert(retry.Meta, Equals, "")
			c.Assert(retry.Val, Equals, initial.Val)
			break
		}
		steps++
		c.Assert(len(retry.Meta) < prevLen, Equals, true,
			Commentf("step %d must carry a strictly smaller payload", steps))
		c.Assert(len(retry.Meta) <= 4096, Equals, true)
		_, err := decodeAckedRanges(retry.Val, retry.Meta)
		c.Assert(err, IsNil, Commentf("shrunk checkpoint must stay decodable"))
		prevLen = len(retry.Meta)
	}
	c.Assert(steps <= 13, Equals, true, Commentf("retries must be bounded, got %d", steps))

	// Even with a frontier-only budget the frontier advances contiguously and
	// the checkpoint remains legal.
	frontier := ot.offset.Val
	ot.OnOffered(msg(frontier))
	advanced, _ := ot.OnAcked(frontier)
	c.Assert(advanced.Val > frontier, Equals, true)
	c.Assert(advanced.Meta, Equals, "")
	_, err := decodeAckedRanges(advanced.Val, advanced.Meta)
	c.Assert(err, IsNil)
}
