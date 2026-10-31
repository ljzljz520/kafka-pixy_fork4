package offsettrk

import (
	"bytes"
	"sort"
	"strconv"
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/offsetmgr"
	"github.com/pkg/errors"
)

const (
	base64EncodeMap = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
)

var (
	base64DecodeMap [256]byte
	decodeShifts    [64/5 + 1]uint
)

func init() {
	for i := range base64DecodeMap {
		base64DecodeMap[i] = 0xFF
	}
	for i := range base64EncodeMap {
		base64DecodeMap[base64EncodeMap[i]] = byte(i)
	}
	for i := range decodeShifts {
		decodeShifts[i] = uint(5 * i)
	}
}

// T represents an entity that tracks offered and acknowledged messages and
// maintains offset data for the current state.
//
// The tracker distinguishes two kinds of state:
//
//   - The complete acknowledgement state (`offset.Val` continuous ack frontier
//     plus the full `ackedRanges` list) lives in memory only. It is used to
//     suppress duplicate offers and to merge ranges as gaps get acknowledged.
//   - A committable checkpoint (see `Checkpoint`) is what is safe to persist
//     in Kafka: the continuous frontier plus only the complete acked ranges
//     that fit into the metadata byte budget, preferentially keeping the
//     ranges nearest the frontier. Ranges omitted from a checkpoint are not
//     lost while the process lives (they remain in memory), but after a
//     restart the corresponding already-processed messages may be redelivered.
//     The continuous frontier never jumps over an unacknowledged gap, so
//     unacknowledged messages are never skipped.
type T struct {
	actDesc      *actor.Descriptor
	offerTimeout time.Duration
	metaBudget   int
	offset       offsetmgr.Offset
	ackedRanges  []offsetRange
	offers       []offer
}

// SparseAcks2Str returns human readable representation of sparsely committed
// ranges encoded in the specified offset metadata.
func SparseAcks2Str(offset offsetmgr.Offset) string {
	var buf bytes.Buffer
	ackedRanges, _ := decodeAckedRanges(offset.Val, offset.Meta)
	for i, ar := range ackedRanges {
		if i != 0 {
			buf.WriteString(",")
		}
		buf.WriteString(strconv.FormatInt(ar.from-offset.Val, 10))
		buf.WriteString("-")
		buf.WriteString(strconv.FormatInt(ar.to-offset.Val, 10))
	}
	return buf.String()
}

// New creates a new offset tracker instance. The metadata of the given
// persisted offset is decoded using the historical sparse-acks encoding (no
// format migration is performed), and maxMetaSize bounds the metadata of
// every subsequently produced checkpoint:
//
//   - maxMetaSize < 0: no budget limit (unbounded encoding);
//   - maxMetaSize == 0: only the continuous frontier is persisted;
//   - maxMetaSize > 0: at most maxMetaSize bytes of complete acked ranges
//     nearest the frontier are encoded as whole ranges, encoded bytes are
//     never truncated mid-range.
func New(actDesc *actor.Descriptor, offset offsetmgr.Offset, offerTimeout time.Duration, maxMetaSize int) *T {
	ot := T{
		actDesc:      actDesc,
		offerTimeout: offerTimeout,
		metaBudget:   maxMetaSize,
		offset:       offset,
	}
	var err error
	ot.ackedRanges, err = decodeAckedRanges(offset.Val, offset.Meta)
	if err != nil {
		ot.ackedRanges = nil
		ot.offset.Meta = ""
		ot.actDesc.Log().WithError(err).Errorf("Bad sparse acks: %v", offset)
	} else {
		// Normalize the persisted snapshot so that the in-memory checkpoint is
		// guaranteed to fit the budget. Encoding is deterministic, therefore
		// metadata that already fits is rewritten byte for byte.
		ot.offset.Meta = encodeAckedRanges(offset.Val, ot.ackedRanges, maxMetaSize)
	}
	return &ot
}

// Checkpoint returns the snapshot that is safe to persist for the current
// state: the continuous ack frontier together with as many complete acked
// ranges nearest the frontier as fit into the metadata byte budget. The
// returned metadata is always decodable and never covers an unacknowledged
// gap; ranges omitted from the snapshot are retained in memory.
func (ot *T) Checkpoint() offsetmgr.Offset {
	return offsetmgr.Offset{
		Val:  ot.offset.Val,
		Meta: encodeAckedRanges(ot.offset.Val, ot.ackedRanges, ot.metaBudget),
	}
}

// ShrinkCheckpoint is called after a coordinator rejected the last submitted
// checkpoint because its metadata was too large. It permanently tightens the
// metadata budget (so the same payload is never resent) and returns the
// resulting smaller checkpoint. It returns false when no complete range can
// be dropped anymore, i.e. even the frontier-only checkpoint keeps getting
// rejected, which means an unrecoverable broker/client configuration
// mismatch. Every successful shrink returns a strictly smaller metadata
// string, therefore the number of retries is bounded by the payload size.
// Advancing the frontier later still reuses the tightened budget.
func (ot *T) ShrinkCheckpoint() (offsetmgr.Offset, bool) {
	current := ot.Checkpoint()
	if len(current.Meta) == 0 {
		return current, false
	}
	// Size the new budget against the rejected encoding so that the first
	// retry already carries fewer ranges; converge to frontier-only rather
	// than dividing the budget in lockstep.
	newBudget := len(current.Meta) / 2
	if ot.metaBudget > 0 && newBudget >= ot.metaBudget {
		newBudget = ot.metaBudget / 2
	}
	ot.metaBudget = newBudget
	shrunk := ot.Checkpoint()
	return shrunk, len(shrunk.Meta) > 0
}

// Adjust adjusts the tracked offset. Offers with offsets lower then the new
// offset value are dropped.
func (ot *T) Adjust(offset int64) (offsetmgr.Offset, int) {
	if offset < ot.offset.Val {
		return ot.offset, len(ot.offers)
	}
	ot.dropOffersBefore(offset)
	ot.correctOffset(offset)
	return ot.offset, len(ot.offers)
}

// OnOffered should be called when a message has been offered to a consumer. It
// returns the total number of offered messages. It is callers responsibility
// to ensure that the number of offered message does not grow too large.
func (ot *T) OnOffered(msg consumer.Message) int {
	offersCount := len(ot.offers)
	// Ignore messages that has already been acknowledged.
	if ok, _ := ot.IsAcked(msg.Offset); ok {
		return offersCount
	}
	// Even though the logic of this function allows offering in any order,
	// this special case exists to expedite the most common real life usage
	// pattern where messages are offered in their offset order.
	if offersCount == 0 || msg.Offset > ot.offers[offersCount-1].offset {
		ot.offers = append(ot.offers, ot.newOffer(msg))
		return offersCount + 1
	}
	// Find the spot where the message should be inserted to keep the offer
	// list sorted by offsets.
	i := sort.Search(offersCount, func(i int) bool {
		return ot.offers[i].msg.Offset >= msg.Offset
	})
	// Ignore if the message is already in the list. It is either a retry, then
	// the offer has already been updated in a NextRetry call, or a mistake of
	// the caller, then we just do not care.
	if ot.offers[i].msg.Offset == msg.Offset {
		return offersCount
	}
	// Insert the message to the offer list keeping it sorted by offsets.
	ot.offers = append(ot.offers, offer{})
	copy(ot.offers[i+1:], ot.offers[i:offersCount])
	ot.offers[i] = ot.newOffer(msg)
	return offersCount + 1
}

// OnAcked should be called when a message has been acknowledged by a consumer.
// It returns an offset to be submitted and a total number of offered messages.
func (ot *T) OnAcked(offset int64) (offsetmgr.Offset, int) {
	offerRemoved := ot.removeOffer(offset)
	ackedRangesUpdated := ot.updateAckedRanges(offset)
	if !offerRemoved || !ackedRangesUpdated {
		ot.actDesc.Log().Errorf("Bad ack: offset=%d, offerMissing=%t, duplicateAck=%t",
			offset, !offerRemoved, !ackedRangesUpdated)
	}
	if ackedRangesUpdated {
		ot.offset = ot.Checkpoint()
	}
	return ot.offset, len(ot.offers)
}

// IsAcked checks if an offset has already been acknowledged. The second
// returned value is the smallest not acked offset that is greater than the
// specified offset.
func (ot *T) IsAcked(offset int64) (bool, int64) {
	if offset < ot.offset.Val {
		return true, ot.offset.Val
	}
	for _, ar := range ot.ackedRanges {
		if offset < ar.from {
			if offset+1 == ar.from {
				return false, ar.to
			}
			return false, offset + 1
		}
		if offset < ar.to {
			return true, ar.to
		}
	}
	return false, offset + 1
}

// NextRetry returns a next message to be retried along with the retry attempt
// number. If there are no messages to be retried then nil is returned.
func (ot *T) NextRetry() (consumer.Message, int, bool) {
	return ot.nextRetry(time.Now())
}
func (ot *T) nextRetry(now time.Time) (consumer.Message, int, bool) {
	for i := range ot.offers {
		o := &ot.offers[i]
		if o.deadline.Before(now) {
			o.deadline = now.Add(ot.offerTimeout)
			o.retryNo += 1
			return o.msg, o.retryNo, true
		}
		// When we reach the first never retried offer with a deadline set in
		// the future it is guaranteed that all further offers in the list have
		// not expired yet. It is only true if messages are offered in the
		// order of their offsets, which is indeed how partition consumer does
		// it. But the offset tracker API allows any order. So the following
		// logic is not valid in general case.
		if o.retryNo == 0 {
			return consumer.Message{}, -1, false
		}
	}
	return consumer.Message{}, -1, false
}

// ShouldWait4Ack tells how much time until all offers expire.
func (ot *T) ShouldWait4Ack() time.Duration {
	return ot.shouldWait4Ack(time.Now())
}
func (ot *T) shouldWait4Ack(now time.Time) time.Duration {
	var maxTimeout time.Duration
	var validOffersCount = 0
	for _, o := range ot.offers {
		if o.deadline.After(now) {
			validOffersCount++
			timeout := o.deadline.Sub(now)
			if maxTimeout < timeout {
				maxTimeout = timeout
			}
		}
	}
	if validOffersCount > 0 {
		ot.actDesc.Log().Infof("Waiting for acks: count=%d, timeout=%v",
			len(ot.offers), maxTimeout)
		return maxTimeout
	}
	// Complain about all not acknowledged messages before giving up.
	for _, o := range ot.offers {
		ot.actDesc.Log().Errorf("Not acked: offset=%d", o.msg.Offset)
	}
	return 0
}

func (ot *T) newOffer(msg consumer.Message) offer {
	return offer{msg, msg.Offset, 0, time.Now().Add(ot.offerTimeout)}
}

// removeOffer if there is an offer with the specified offset in the list, then
// it is removed and true is returned, otherwise it returns false.
func (ot *T) removeOffer(offset int64) bool {
	offersCount := len(ot.offers)
	i := sort.Search(offersCount, func(i int) bool {
		return ot.offers[i].msg.Offset >= offset
	})
	if i >= offersCount || ot.offers[i].msg.Offset != offset {
		return false
	}
	offersCount -= 1
	copy(ot.offers[i:offersCount], ot.offers[i+1:])
	ot.offers[offersCount] = offer{} // Makes it subject for garbage collection.
	ot.offers = ot.offers[:offersCount]
	return true
}

// dropOffersBefore removes all offers that have offset before the given one.
func (ot *T) dropOffersBefore(offset int64) {
	drop := 0
	for i, offer := range ot.offers {
		if offset <= offer.offset {
			break
		}
		drop = i + 1
		ot.actDesc.Log().Errorf("Offer dropped: offset=%d", offer.offset)
	}
	if drop > 0 {
		left := len(ot.offers) - drop
		copy(ot.offers[:left], ot.offers[drop:])
		// Zero dropped offers to make them eligible for garbage collection.
		for i := left; i < len(ot.offers); i++ {
			ot.offers[i] = offer{}
		}
		ot.offers = ot.offers[:left]
	}
}

// correctOffset if the specified offset falls into the middle of an acked
// range then the  offset is set to the end of the range otherwise the
// specified offset value is set as tracked. All acked ranges that are lower
// then the specified offset are dropped.
func (ot *T) correctOffset(offset int64) {
	drop := 0
	for i, ar := range ot.ackedRanges {
		if offset < ar.from {
			break
		}
		drop = i + 1
		if offset < ar.to {
			offset = ar.to
			break
		}
	}
	if drop > 0 {
		ot.ackedRanges = ot.ackedRanges[drop:]
	}
	ot.offset.Val = offset
	ot.offset = ot.Checkpoint()
}

// updateAckedRanges updates acked ranges with a new acked offset. It returns
// true if the acked ranges has been modified by this call. It returns false if
// the specified offset has already been acked and there is nothing to update.
func (ot *T) updateAckedRanges(offset int64) bool {
	ackedRangesCount := len(ot.ackedRanges)
	if offset < ot.offset.Val {
		return false
	}
	if offset == ot.offset.Val {
		if ackedRangesCount > 0 && offset == ot.ackedRanges[0].from-1 {
			ot.offset.Val = ot.ackedRanges[0].to
			ot.ackedRanges = ot.ackedRanges[1:]
			return true
		}
		ot.offset.Val += 1
		return true
	}
	for i := range ot.ackedRanges {
		if offset < ot.ackedRanges[i].from {
			if offset == ot.ackedRanges[i].from-1 {
				ot.ackedRanges[i].from -= 1
				return true
			}
			ot.ackedRanges = append(ot.ackedRanges, offsetRange{})
			copy(ot.ackedRanges[i+1:], ot.ackedRanges[i:ackedRangesCount])
			ot.ackedRanges[i] = newOffsetRange(offset)
			return true
		}
		if offset < ot.ackedRanges[i].to {
			return false
		}
		if offset == ot.ackedRanges[i].to {
			if ackedRangesCount > i+1 && offset == ot.ackedRanges[i+1].from-1 {
				ot.ackedRanges[i+1].from = ot.ackedRanges[i].from
				ackedRangesCount -= 1
				copy(ot.ackedRanges[i:ackedRangesCount], ot.ackedRanges[i+1:])
				ot.ackedRanges = ot.ackedRanges[:ackedRangesCount]
				return true
			}
			ot.ackedRanges[i].to += 1
			return true
		}
	}
	ot.ackedRanges = append(ot.ackedRanges, newOffsetRange(offset))
	return true
}

// encodeAckedRanges encodes the given acked ranges relative to base. Ranges
// must be sorted and each of them represents a complete contiguous interval
// of acknowledged offsets. If maxSize is non-negative then only the prefix
// of ranges nearest the base (the ack frontier) that fits the budget is
// encoded. Ranges are appended atomically: either both boundary varints of a
// range make it into the output, or the range is omitted entirely; encoded
// bytes are therefore never truncated in the middle of a token. A negative
// maxSize means no budget limit.
func encodeAckedRanges(base int64, ackedRanges []offsetRange, maxSize int) string {
	ackedRangesCount := len(ackedRanges)
	if ackedRangesCount == 0 {
		return ""
	}
	buf := make([]byte, 0, ackedRangesCount*4)
	for _, ar := range ackedRanges {
		encoded := ar.encode(base, nil)
		if maxSize >= 0 && len(buf)+len(encoded) > maxSize {
			break
		}
		buf = append(buf, encoded...)
		base = ar.to
	}
	return string(buf)
}

func decodeAckedRanges(base int64, encoded string) ([]offsetRange, error) {
	if encoded == "" {
		return nil, nil
	}
	ackedRanges := make([]offsetRange, 0, len(encoded)/2)
	var err error
	buf := []byte(encoded)
	for i := 0; len(buf) > 0; i++ {
		var ar offsetRange
		if buf, err = ar.decode(base, buf); err != nil {
			return nil, errors.Wrapf(err, "bad encoding: %s", encoded)
		}
		base = ar.to
		ackedRanges = append(ackedRanges, ar)
	}
	return ackedRanges, nil
}

type offsetRange struct {
	from, to int64
}

func newOffsetRange(offset int64) offsetRange {
	return offsetRange{offset, offset + 1}
}

func (or *offsetRange) encode(base int64, buf []byte) []byte {
	buf = encodeInt64(or.from-base, buf)
	buf = encodeInt64(or.to-or.from, buf)
	return buf
}

func (or *offsetRange) decode(base int64, buf []byte) ([]byte, error) {
	delta, buf, err := decodeInt64(buf)
	if err != nil {
		return nil, errors.Wrap(err, "bad `from` boundary")
	}
	or.from = base + delta

	delta, buf, err = decodeInt64(buf)
	if err != nil {
		return nil, errors.Wrap(err, "bad `to` boundary")
	}
	or.to = or.from + delta

	if or.from >= or.to || or.from <= 0 {
		return nil, errors.Errorf("bad range: %v", *or)
	}
	return buf, nil
}

func encodeInt64(n int64, buf []byte) []byte {
	var ci int64
	for ci, n = n&0x1F, n>>5; n > 0; ci, n = n&0x1F, n>>5 {
		buf = append(buf, base64EncodeMap[ci|0x20])
	}
	return append(buf, base64EncodeMap[ci])
}

func decodeInt64(buf []byte) (int64, []byte, error) {
	var n int64
	i := 0
	for ; i < len(buf) && i < len(decodeShifts); i++ {
		ci := base64DecodeMap[buf[i]]
		if ci == 0xFF {
			return -1, nil, errors.Errorf("bad char: %c", buf[i])
		}
		n |= int64(ci) & 0x1F << decodeShifts[i]
		if ci&0x20 == 0 {
			return n, buf[i+1:], nil
		}
	}
	return -1, nil, errors.Errorf("bad sequence: %s", string(buf[:i]))
}

type offer struct {
	msg      consumer.Message
	offset   int64
	retryNo  int
	deadline time.Time
}
