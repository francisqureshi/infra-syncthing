// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package protocol

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	lz4 "github.com/pierrec/lz4/v4"
	"google.golang.org/protobuf/proto"

	"github.com/syncthing/syncthing/internal/gen/bep"
	"github.com/syncthing/syncthing/lib/rand"
	"github.com/syncthing/syncthing/lib/testutil"
)

var (
	c0ID = NewDeviceID([]byte{1})
	c1ID = NewDeviceID([]byte{2})
)

func TestPing(t *testing.T) {
	ar, aw := io.Pipe()
	br, bw := io.Pipe()

	c0 := getRawConnection(NewConnection(c0ID, ar, bw, testutil.NoopCloser{}, newTestModel(), new(mockedConnectionInfo), CompressionAlways, testKeyGen))
	c0.Start()
	defer closeAndWait(c0, ar, bw)
	c1 := getRawConnection(NewConnection(c1ID, br, aw, testutil.NoopCloser{}, newTestModel(), new(mockedConnectionInfo), CompressionAlways, testKeyGen))
	c1.Start()
	defer closeAndWait(c1, ar, bw)
	c0.ClusterConfig(&ClusterConfig{}, nil)
	c1.ClusterConfig(&ClusterConfig{}, nil)

	if ok := c0.ping(); !ok {
		t.Error("c0 ping failed")
	}
	if ok := c1.ping(); !ok {
		t.Error("c1 ping failed")
	}
}

var errManual = errors.New("manual close")

func TestClose(t *testing.T) {
	m0 := newTestModel()
	m1 := newTestModel()

	ar, aw := io.Pipe()
	br, bw := io.Pipe()

	c0 := getRawConnection(NewConnection(c0ID, ar, bw, testutil.NoopCloser{}, m0, new(mockedConnectionInfo), CompressionAlways, testKeyGen))
	c0.Start()
	defer closeAndWait(c0, ar, bw)
	c1 := NewConnection(c1ID, br, aw, testutil.NoopCloser{}, m1, new(mockedConnectionInfo), CompressionAlways, testKeyGen)
	c1.Start()
	defer closeAndWait(c1, ar, bw)
	c0.ClusterConfig(&ClusterConfig{}, nil)
	c1.ClusterConfig(&ClusterConfig{}, nil)

	c0.internalClose(errManual)

	<-c0.closed
	if err := m0.closedError(); err != errManual {
		t.Fatal("Connection should be closed")
	}

	// None of these should panic, some should return an error

	if c0.ping() {
		t.Error("Ping should not return true")
	}

	ctx := context.Background()

	c0.Index(ctx, &Index{Folder: "default"})
	c0.Index(ctx, &Index{Folder: "default"})

	if _, err := c0.Request(ctx, &Request{Folder: "default", Name: "foo"}); err == nil {
		t.Error("Request should return an error")
	}
}

// TestCloseOnBlockingSend checks that the connection does not deadlock when
// Close is called while the underlying connection is broken (send blocks).
// https://github.com/syncthing/syncthing/pull/5442
func TestCloseOnBlockingSend(t *testing.T) {
	oldCloseTimeout := CloseTimeout
	CloseTimeout = 100 * time.Millisecond
	defer func() {
		CloseTimeout = oldCloseTimeout
	}()

	m := newTestModel()

	rw := testutil.NewBlockingRW()
	c := getRawConnection(NewConnection(c0ID, rw, rw, testutil.NoopCloser{}, m, new(mockedConnectionInfo), CompressionAlways, testKeyGen))
	c.Start()
	defer closeAndWait(c, rw)

	wg := sync.WaitGroup{}

	wg.Go(func() {
		c.ClusterConfig(&ClusterConfig{}, nil)
	})

	wg.Go(func() {
		c.Close(errManual)
	})

	// This simulates an error from ping timeout
	wg.Go(func() {
		c.internalClose(ErrTimeout)
	})

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out before all functions returned")
	}
}

func TestCloseRace(t *testing.T) {
	indexReceived := make(chan struct{})
	unblockIndex := make(chan struct{})
	m0 := newTestModel()
	m0.indexFn = func(string, []FileInfo) {
		close(indexReceived)
		<-unblockIndex
	}
	m1 := newTestModel()

	ar, aw := io.Pipe()
	br, bw := io.Pipe()

	c0 := getRawConnection(NewConnection(c0ID, ar, bw, testutil.NoopCloser{}, m0, new(mockedConnectionInfo), CompressionNever, testKeyGen))
	c0.Start()
	defer closeAndWait(c0, ar, bw)
	c1 := NewConnection(c1ID, br, aw, testutil.NoopCloser{}, m1, new(mockedConnectionInfo), CompressionNever, testKeyGen)
	c1.Start()
	defer closeAndWait(c1, ar, bw)
	c0.ClusterConfig(&ClusterConfig{}, nil)
	c1.ClusterConfig(&ClusterConfig{}, nil)

	c1.Index(context.Background(), &Index{Folder: "default"})
	select {
	case <-indexReceived:
	case <-time.After(time.Second):
		t.Fatal("timed out before receiving index")
	}

	go c0.internalClose(errManual)
	select {
	case <-c0.closed:
	case <-time.After(time.Second):
		t.Fatal("timed out before c0.closed was closed")
	}

	select {
	case <-m0.closedCh:
		t.Errorf("receiver.Closed called before receiver.Index")
	default:
	}

	close(unblockIndex)

	if err := m0.closedError(); err != errManual {
		t.Fatal("Connection should be closed")
	}
}

func TestClusterConfigFirst(t *testing.T) {
	m := newTestModel()

	rw := testutil.NewBlockingRW()
	c := getRawConnection(NewConnection(c0ID, rw, &testutil.NoopRW{}, testutil.NoopCloser{}, m, new(mockedConnectionInfo), CompressionAlways, testKeyGen))
	c.Start()
	defer closeAndWait(c, rw)

	pingReturned := make(chan bool, 1)
	go func() {
		pingReturned <- c.send(context.Background(), &bep.Ping{})
	}()
	select {
	case <-pingReturned:
		t.Fatal("able to send ping before cluster config")
	case <-time.After(100 * time.Millisecond):
		// Allow some time for c.writerLoop to set up after c.Start
	}

	c.ClusterConfig(&ClusterConfig{}, nil)
	if ok := <-pingReturned; !ok {
		t.Fatal("send ping after cluster config returned false")
	}

	done := make(chan struct{})
	go func() {
		c.internalClose(errManual)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close didn't return before timeout")
	}

	if err := m.closedError(); err != errManual {
		t.Fatal("Connection should be closed")
	}
}

// TestCloseTimeout checks that calling Close times out and proceeds, if sending
// the close message does not succeed.
func TestCloseTimeout(t *testing.T) {
	oldCloseTimeout := CloseTimeout
	CloseTimeout = 100 * time.Millisecond
	defer func() {
		CloseTimeout = oldCloseTimeout
	}()

	m := newTestModel()

	rw := testutil.NewBlockingRW()
	c := getRawConnection(NewConnection(c0ID, rw, rw, testutil.NoopCloser{}, m, new(mockedConnectionInfo), CompressionAlways, testKeyGen))
	c.Start()
	defer closeAndWait(c, rw)

	done := make(chan struct{})
	go func() {
		c.Close(errManual)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * CloseTimeout):
		t.Fatal("timed out before Close returned")
	}
}

func TestUnmarshalFDPUv16v17(t *testing.T) {
	var fdpu bep.FileDownloadProgressUpdate

	m0, _ := hex.DecodeString("08cda1e2e3011278f3918787f3b89b8af2958887f0aa9389f3a08588f3aa8f96f39aa8a5f48b9188f19286a0f3848da4f3aba799f3beb489f0a285b9f487b684f2a3bda2f48598b4f2938a89f2a28badf187a0a2f2aebdbdf4849494f4808fbbf2b3a2adf2bb95bff0a6ada4f198ab9af29a9c8bf1abb793f3baabb2f188a6ba1a0020bb9390f60220f6d9e42220b0c7e2b2fdffffffff0120fdb2dfcdfbffffffff0120cedab1d50120bd8784c0feffffffff0120ace99591fdffffffff0120eed7d09af9ffffffff01")
	if err := proto.Unmarshal(m0, &fdpu); err != nil {
		t.Fatal("Unmarshalling message from v0.14.16:", err)
	}

	m1, _ := hex.DecodeString("0880f1969905128401f099b192f0abb1b9f3b280aff19e9aa2f3b89e84f484b39df1a7a6b0f1aea4b1f0adac94f3b39caaf1939281f1928a8af0abb1b0f0a8b3b3f3a88e94f2bd85acf29c97a9f2969da6f0b7a188f1908ea2f09a9c9bf19d86a6f29aada8f389bb95f0bf9d88f1a09d89f1b1a4b5f29b9eabf298a59df1b2a589f2979ebdf0b69880f18986b21a440a1508c7d8fb8897ca93d90910e8c4d8e8f2f8f0ccee010a1508afa8ffd8c085b393c50110e5bdedc3bddefe9b0b0a1408a1bedddba4cac5da3c10b8e5d9958ca7e3ec19225ae2f88cb2f8ffffffff018ceda99cfbffffffff01b9c298a407e295e8e9fcffffffff01f3b9ade5fcffffffff01c08bfea9fdffffffff01a2c2e5e1ffffffffff0186dcc5dafdffffffff01e9ffc7e507c9d89db8fdffffffff01")
	if err := proto.Unmarshal(m1, &fdpu); err != nil {
		t.Fatal("Unmarshalling message from v0.14.16:", err)
	}
}

func TestWriteCompressed(t *testing.T) {
	for _, random := range []bool{false, true} {
		buf := new(bytes.Buffer)
		c := &rawConnection{
			cr:          &countingReader{Reader: buf},
			cw:          &countingWriter{Writer: buf},
			compression: CompressionAlways,
		}

		msg := (&Response{Data: make([]byte, 10240)}).toWire()
		if random {
			// This should make the message incompressible.
			rand.Read(msg.Data)
		}

		if err := c.writeMessage(msg); err != nil {
			t.Fatal(err)
		}
		got, err := c.readMessage(make([]byte, 4))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.(*bep.Response).Data, msg.Data) {
			t.Error("received the wrong message")
		}

		hdr := &bep.Header{Type: typeOf(msg)}
		size := int64(2 + proto.Size(hdr) + 4 + proto.Size(msg))
		if c.cr.Tot() > size {
			t.Errorf("compression enlarged message from %d to %d",
				size, c.cr.Tot())
		}
	}
}

func TestLZ4Compression(t *testing.T) {
	for i := range 10 {
		dataLen := 150 + rand.Intn(150)
		data := make([]byte, dataLen)
		_, err := io.ReadFull(rand.Reader, data[100:])
		if err != nil {
			t.Fatal(err)
		}

		comp := make([]byte, lz4.CompressBlockBound(dataLen))
		compLen, err := lz4Compress(data, comp)
		if err != nil {
			t.Errorf("compressing %d bytes: %v", dataLen, err)
			continue
		}

		res, err := lz4Decompress(comp[:compLen])
		if err != nil {
			t.Errorf("decompressing %d bytes to %d: %v", len(comp), dataLen, err)
			continue
		}
		if len(res) != len(data) {
			t.Errorf("Incorrect len %d != expected %d", len(res), len(data))
		}
		if !bytes.Equal(data, res) {
			t.Error("Incorrect decompressed data")
		}
		t.Logf("OK #%d, %d -> %d -> %d", i, dataLen, len(comp), dataLen)
	}
}

func TestLZ4CompressionUpdate(t *testing.T) {
	uncompressed := []byte("this is some arbitrary yet fairly compressible data")

	// Compressed, as created by the LZ4 implementation in Syncthing 1.18.6 and earlier.
	oldCompressed, _ := hex.DecodeString("00000033f0247468697320697320736f6d65206172626974726172792079657420666169726c7920636f6d707265737369626c652064617461")

	// Verify that we can decompress

	res, err := lz4Decompress(oldCompressed)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(uncompressed, res) {
		t.Fatal("result does not match")
	}

	// Verify that our current compression is equivalent

	buf := make([]byte, 128)
	n, err := lz4Compress(uncompressed, buf)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(oldCompressed, buf[:n]) {
		t.Logf("%x", oldCompressed)
		t.Logf("%x", buf[:n])
		t.Fatal("compression does not match")
	}
}

func TestCheckFilename(t *testing.T) {
	cases := []struct {
		name string
		ok   bool
	}{
		// Valid filenames
		{"foo", true},
		{"foo/bar/baz", true},
		{"foo/bar:baz", true}, // colon is ok in general, will be filtered on windows
		{`\`, true},           // path separator on the wire is forward slash, so as above
		{`\.`, true},
		{`\..`, true},
		{".foo", true},
		{"foo..", true},

		// Invalid filenames
		{"foo/..", false},
		{"foo/../bar", false},
		{"../foo/../bar", false},
		{"", false},
		{".", false},
		{"..", false},
		{"/", false},
		{"/.", false},
		{"/..", false},
		{"/foo", false},
		{"./foo", false},
		{"foo./", false},
		{"foo/.", false},
		{"foo/", false},
	}

	for _, tc := range cases {
		err := checkFilename(tc.name)
		if (err == nil) != tc.ok {
			t.Errorf("Unexpected result for checkFilename(%q): %v", tc.name, err)
		}
	}
}

func TestCheckConsistency(t *testing.T) {
	cases := []struct {
		fi FileInfo
		ok bool
	}{
		{
			// valid
			fi: FileInfo{
				Name:   "foo",
				Type:   FileInfoTypeFile,
				Blocks: []BlockInfo{{Size: 1234, Offset: 0, Hash: []byte{1, 2, 3, 4}}},
			},
			ok: true,
		},
		{
			// deleted with blocks
			fi: FileInfo{
				Name:    "foo",
				Deleted: true,
				Type:    FileInfoTypeFile,
				Blocks:  []BlockInfo{{Size: 1234, Offset: 0, Hash: []byte{1, 2, 3, 4}}},
			},
			ok: false,
		},
		{
			// no blocks
			fi: FileInfo{
				Name: "foo",
				Type: FileInfoTypeFile,
			},
			ok: false,
		},
		{
			// directory with blocks
			fi: FileInfo{
				Name:   "foo",
				Type:   FileInfoTypeDirectory,
				Blocks: []BlockInfo{{Size: 1234, Offset: 0, Hash: []byte{1, 2, 3, 4}}},
			},
			ok: false,
		},
		{
			// directory with zero size
			fi: FileInfo{
				Name: "foo",
				Type: FileInfoTypeDirectory,
			},
			ok: true,
		},
		{
			// directory with synthetic size
			fi: FileInfo{
				Name: "foo",
				Type: FileInfoTypeDirectory,
				Size: deprecatedSyntheticDirectorySize,
			},
			ok: true,
		},
		{
			// directory with arbitrary size
			fi: FileInfo{
				Name: "foo",
				Type: FileInfoTypeDirectory,
				Size: 42,
			},
			ok: false,
		},
		{
			// symlink with zero size
			fi: FileInfo{
				Name:          "foo",
				Type:          FileInfoTypeSymlink,
				SymlinkTarget: []byte("bar"),
			},
			ok: true,
		},
		{
			// symlink with synthetic directory size (not permitted)
			fi: FileInfo{
				Name:          "foo",
				Type:          FileInfoTypeSymlink,
				SymlinkTarget: []byte("bar"),
				Size:          deprecatedSyntheticDirectorySize,
			},
			ok: false,
		},
		{
			// symlink with arbitrary size
			fi: FileInfo{
				Name:          "foo",
				Type:          FileInfoTypeSymlink,
				SymlinkTarget: []byte("bar"),
				Size:          42,
			},
			ok: false,
		},
	}

	for _, tc := range cases {
		err := checkFileInfoConsistency(tc.fi)
		if tc.ok && err != nil {
			t.Errorf("Unexpected error %v (want nil) for %v", err, tc.fi)
		}
		if !tc.ok && err == nil {
			t.Errorf("Unexpected nil error for %v", tc.fi)
		}
	}
}

func TestBlockSize(t *testing.T) {
	cases := []struct {
		fileSize  int64
		blockSize int
	}{
		{1 << KiB, 128 << KiB},
		{1 << MiB, 128 << KiB},
		{499 << MiB, 256 << KiB},
		{500 << MiB, 512 << KiB},
		{501 << MiB, 512 << KiB},
		{1 << GiB, 1 << MiB},
		{2 << GiB, 2 << MiB},
		{3 << GiB, 2 << MiB},
		{500 << GiB, 16 << MiB},
		{50000 << GiB, 16 << MiB},
	}

	for _, tc := range cases {
		size := BlockSize(tc.fileSize)
		if size != tc.blockSize {
			t.Errorf("BlockSize(%d), size=%d, expected %d", tc.fileSize, size, tc.blockSize)
		}
	}
}

var blockSize int

func BenchmarkBlockSize(b *testing.B) {
	for i := 0; i < b.N; i++ {
		blockSize = BlockSize(16 << 30)
	}
}

// TestClusterConfigAfterClose checks that ClusterConfig does not deadlock when
// ClusterConfig is called on a closed connection.
func TestClusterConfigAfterClose(t *testing.T) {
	m := newTestModel()

	rw := testutil.NewBlockingRW()
	c := getRawConnection(NewConnection(c0ID, rw, rw, testutil.NoopCloser{}, m, new(mockedConnectionInfo), CompressionAlways, testKeyGen))
	c.Start()
	defer closeAndWait(c, rw)

	c.internalClose(errManual)

	done := make(chan struct{})
	go func() {
		c.ClusterConfig(&ClusterConfig{}, nil)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out before Cluster Config returned")
	}
}

func TestDispatcherToCloseDeadlock(t *testing.T) {
	// Verify that we don't deadlock when calling Close() from within one of
	// the model callbacks (ClusterConfig).
	m := newTestModel()
	rw := testutil.NewBlockingRW()
	c := getRawConnection(NewConnection(c0ID, rw, &testutil.NoopRW{}, testutil.NoopCloser{}, m, new(mockedConnectionInfo), CompressionAlways, testKeyGen))
	m.ccFn = func(*ClusterConfig) {
		c.Close(errManual)
	}
	c.Start()
	defer closeAndWait(c, rw)

	c.inbox <- &bep.ClusterConfig{}

	select {
	case <-c.dispatcherLoopStopped:
	case <-time.After(time.Second):
		t.Fatal("timed out before dispatcher loop terminated")
	}
}

func TestRequestMaxSize(t *testing.T) {
	invalidSize := []int{-65536, -1, MaxRequestSize + 1}
	for _, s := range invalidSize {
		t.Run(fmt.Sprintf("invalid/%d", s), func(t *testing.T) {
			m := newTestModel()
			rw := testutil.NewBlockingRW()
			writer := &controlledWireWriter{writes: make(chan controlledWireWrite)}
			c := getRawConnection(NewConnection(c0ID, rw, writer, testutil.NoopCloser{}, m, new(mockedConnectionInfo), CompressionAlways, testKeyGen))
			c.Start()
			defer closeAndWait(c, rw)
			go c.ClusterConfig(&ClusterConfig{}, nil)
			initialWrite := awaitControlledWireWrite(t, writer)
			close(initialWrite.complete)

			c.inbox <- &bep.ClusterConfig{}

			// A request at exactly MaxRequestSize should be accepted.
			c.inbox <- &bep.Request{
				Id:   1,
				Name: "valid",
				Size: MaxRequestSize,
				Hash: []byte{42},
			}

			responseWrite := awaitControlledWireWrite(t, writer)
			if id := responseIDFromWire(t, responseWrite.data); id != 1 {
				t.Errorf("bad response ID %d", id)
			}
			close(responseWrite.complete)

			// A request with an invalid size should cause the dispatcher to
			// return with a protocol error.
			c.inbox <- &bep.Request{
				Id:   2,
				Name: "invalid",
				Size: int32(s),
				Hash: []byte{42},
			}

			select {
			case <-c.dispatcherLoopStopped:
			case <-time.After(time.Second):
				t.Fatal("timed out before dispatcher loop terminated")
			}
			closeWrite := awaitControlledWireWrite(t, writer)
			close(closeWrite.complete)

			err := m.closedError()
			if err == nil {
				t.Fatal("expected connection to be closed with an error")
			}
			if !strings.Contains(err.Error(), "protocol error") {
				t.Errorf("expected a protocol error, got %v", err)
			}
		})
	}
}

func TestRequestZeroSize(t *testing.T) {
	// A zero-sized request should be accepted, since current versions of
	// Syncthing send these. See https://github.com/syncthing/syncthing/issues/10709.
	m := newTestModel()
	rw := testutil.NewBlockingRW()
	writer := &controlledWireWriter{writes: make(chan controlledWireWrite)}
	c := getRawConnection(NewConnection(c0ID, rw, writer, testutil.NoopCloser{}, m, new(mockedConnectionInfo), CompressionAlways, testKeyGen))
	c.Start()
	defer closeAndWait(c, rw)
	go c.ClusterConfig(&ClusterConfig{}, nil)
	initialWrite := awaitControlledWireWrite(t, writer)
	close(initialWrite.complete)

	c.inbox <- &bep.ClusterConfig{}
	c.inbox <- &bep.Request{
		Id:   1,
		Name: "valid",
		Size: 0,
		Hash: []byte{42},
	}

	responseWrite := awaitControlledWireWrite(t, writer)
	if id := responseIDFromWire(t, responseWrite.data); id != 1 {
		t.Errorf("bad response ID %d", id)
	}
	close(responseWrite.complete)
}

func TestRequestResponsesPreserveAdmissionOrder(t *testing.T) {
	ready := make(chan struct{})
	close(ready)
	firstMayReturn := make(chan struct{})
	firstStarted := make(chan struct{})
	first := newOrderedFakeRequestResponse(ready)
	second := newOrderedFakeRequestResponse(first.closed)

	m := newTestModel()
	m.requestFn = func(req *Request) (RequestResponse, error) {
		switch req.ID {
		case 1:
			close(firstStarted)
			<-firstMayReturn
			return first, nil
		case 2:
			return second, nil
		default:
			return nil, errors.New("unexpected request")
		}
	}
	writer := &controlledWireWriter{writes: make(chan controlledWireWrite)}
	c := getRawConnection(NewConnection(c0ID, &testutil.NoopRW{}, writer, testutil.NoopCloser{}, m, new(mockedConnectionInfo), CompressionNever, testKeyGen))
	writerStopped := make(chan struct{})
	go func() { c.writerLoop(); close(writerStopped) }()
	go c.ClusterConfig(&ClusterConfig{}, nil)
	initialWrite := awaitControlledWireWrite(t, writer)
	close(initialWrite.complete)
	t.Cleanup(func() {
		close(c.closed)
		<-writerStopped
	})

	go c.handleRequest(&Request{ID: 1})
	<-firstStarted
	go c.handleRequest(&Request{ID: 2})

	select {
	case write := <-writer.writes:
		close(write.complete)
		t.Fatalf("response %d overtook the admitted response", responseIDFromWire(t, write.data))
	case <-second.waitStarted:
	}

	close(firstMayReturn)
	firstWrite := awaitControlledWireWrite(t, writer)
	if id := responseIDFromWire(t, firstWrite.data); id != 1 {
		t.Fatalf("first response ID is %d, expected 1", id)
	}
	close(firstWrite.complete)

	secondWrite := awaitControlledWireWrite(t, writer)
	if id := responseIDFromWire(t, secondWrite.data); id != 2 {
		t.Fatalf("second response ID is %d, expected 2", id)
	}
	close(secondWrite.complete)
}

func TestRequestResponseFrameIsNonPreemptiveOnWire(t *testing.T) {
	ready := make(chan struct{})
	close(ready)
	first := newOrderedFakeRequestResponse(ready)
	second := newOrderedFakeRequestResponse(first.closed)

	m := newTestModel()
	m.requestFn = func(req *Request) (RequestResponse, error) {
		switch req.ID {
		case 1:
			return first, nil
		case 2:
			return second, nil
		default:
			return nil, errors.New("unexpected request")
		}
	}
	writer := &controlledWireWriter{writes: make(chan controlledWireWrite)}
	c := getRawConnection(NewConnection(c0ID, &testutil.NoopRW{}, writer, testutil.NoopCloser{}, m, new(mockedConnectionInfo), CompressionNever, testKeyGen))
	writerStopped := make(chan struct{})
	go func() {
		c.writerLoop()
		close(writerStopped)
	}()
	go c.ClusterConfig(&ClusterConfig{}, nil)
	initialWrite := awaitControlledWireWrite(t, writer)
	close(initialWrite.complete)
	t.Cleanup(func() {
		close(c.closed)
		<-writerStopped
	})

	go c.handleRequest(&Request{ID: 1})
	firstWrite := awaitControlledWireWrite(t, writer)
	if id := responseIDFromWire(t, firstWrite.data); id != 1 {
		t.Fatalf("first wire response ID is %d, expected 1", id)
	}

	go c.handleRequest(&Request{ID: 2})
	<-second.waitStarted
	select {
	case write := <-writer.writes:
		close(write.complete)
		t.Fatal("second response began writing before the active frame completed")
	default:
	}

	close(firstWrite.complete)
	secondWrite := awaitControlledWireWrite(t, writer)
	if id := responseIDFromWire(t, secondWrite.data); id != 2 {
		t.Fatalf("second wire response ID is %d, expected 2", id)
	}
	close(secondWrite.complete)
	select {
	case <-second.closed:
	case <-time.After(time.Second):
		t.Fatal("second response lifecycle did not complete after its wire frame")
	}
}

func TestConnectionCriticalTrafficLeadsQueuedFolderWork(t *testing.T) {
	t.Run("configured priority", func(t *testing.T) {
		testConnectionCriticalTrafficLeadsQueuedFolderWork(t, &networkPriorityTestModel{
			TestModel:  newTestModel(),
			priorities: map[string]int{"folder": 100},
		})
	})
	t.Run("default priority", func(t *testing.T) {
		testConnectionCriticalTrafficLeadsQueuedFolderWork(t, newTestModel())
	})
}

func testConnectionCriticalTrafficLeadsQueuedFolderWork(t *testing.T, model Model) {
	t.Helper()
	c, writer, initialWrite := newControlledProtocolConnection(t, c0ID, model)

	indexReturned := make(chan error, 1)
	go func() {
		indexReturned <- c.Index(context.Background(), &Index{Folder: "folder"})
	}()
	awaitQueuedOutputs(t, c, 1)

	clusterConfigReturned := make(chan struct{})
	go func() {
		c.ClusterConfig(&ClusterConfig{}, nil)
		close(clusterConfigReturned)
	}()
	awaitQueuedOutputs(t, c, 2)
	select {
	case write := <-writer.writes:
		close(write.complete)
		t.Fatal("Connection-Critical Traffic interrupted an active protocol frame")
	default:
	}

	close(initialWrite.complete)
	criticalWrite := awaitControlledWireWrite(t, writer)
	if got := messageTypeFromWire(t, criticalWrite.data); got != bep.MessageType_MESSAGE_TYPE_CLUSTER_CONFIG {
		close(criticalWrite.complete)
		t.Fatalf("first queued frame is %v, expected Connection-Critical Traffic", got)
	}
	close(criticalWrite.complete)
	<-clusterConfigReturned

	metadataWrite := awaitControlledWireWrite(t, writer)
	if got := messageTypeFromWire(t, metadataWrite.data); got != bep.MessageType_MESSAGE_TYPE_INDEX {
		close(metadataWrite.complete)
		t.Fatalf("second queued frame is %v, expected Folder-Scoped Metadata", got)
	}
	close(metadataWrite.complete)
	if err := <-indexReturned; err != nil {
		t.Fatal(err)
	}
}

func TestFolderScopedMetadataInheritsCurrentNetworkPriority(t *testing.T) {
	model := &networkPriorityTestModel{
		TestModel: newTestModel(),
		priorities: map[string]int{
			"low":  -100,
			"high": 100,
		},
	}
	c, writer, initialWrite := newControlledProtocolConnection(t, c0ID, model)

	results := make(chan error, 2)
	go func() { results <- c.Index(context.Background(), &Index{Folder: "low"}) }()
	go func() { results <- c.Index(context.Background(), &Index{Folder: "high"}) }()
	awaitQueuedOutputs(t, c, 2)
	model.priorities["low"] = 200

	close(initialWrite.complete)
	lowWrite := awaitControlledWireWrite(t, writer)
	if got := indexFolderFromWire(t, lowWrite.data); got != "low" {
		t.Fatalf("first metadata folder is %q, expected reprioritized current Network Priority", got)
	}
	close(lowWrite.complete)
	highWrite := awaitControlledWireWrite(t, writer)
	if got := indexFolderFromWire(t, highWrite.data); got != "high" {
		t.Fatalf("second metadata folder is %q, expected the remaining Network Priority", got)
	}
	close(highWrite.complete)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}

func TestFolderScopedMetadataYieldsToSamePriorityBlock(t *testing.T) {
	model := &networkPriorityTestModel{
		TestModel:  newTestModel(),
		priorities: map[string]int{"folder": 10},
	}
	c, writer, initialWrite := newControlledProtocolConnection(t, c0ID, model)
	close(initialWrite.complete)
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	t.Cleanup(cancelRequest)

	firstResult := make(chan error, 1)
	go func() { firstResult <- c.Index(context.Background(), &Index{Folder: "folder", LastSequence: 1}) }()
	firstMetadata := awaitControlledWireWrite(t, writer)
	if err := <-firstResult; err != nil {
		t.Fatal(err)
	}

	secondResult := make(chan error, 1)
	go func() {
		secondResult <- c.IndexUpdate(context.Background(), &IndexUpdate{Folder: "folder", LastSequence: 2})
	}()
	requestResult := make(chan error, 1)
	go func() {
		_, err := c.Request(requestCtx, &Request{Folder: "folder", Name: "file", Size: 1})
		requestResult <- err
	}()
	awaitQueuedOutputs(t, c, 2)

	close(firstMetadata.complete)
	blockWrite := awaitControlledWireWrite(t, writer)
	if got := messageTypeFromWire(t, blockWrite.data); got != bep.MessageType_MESSAGE_TYPE_REQUEST {
		t.Fatalf("frame after one metadata batch is %v, expected same-priority Block Transfer", got)
	}
	close(blockWrite.complete)
	cancelRequest()
	if err := <-requestResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("Request returned %v, expected cancellation after observing its frame", err)
	}

	secondMetadata := awaitControlledWireWrite(t, writer)
	if got := messageTypeFromWire(t, secondMetadata.data); got != bep.MessageType_MESSAGE_TYPE_INDEX_UPDATE {
		t.Fatalf("frame after block turn is %v, expected next Folder-Scoped Metadata batch", got)
	}
	close(secondMetadata.complete)
	if err := <-secondResult; err != nil {
		t.Fatal(err)
	}
}

func TestConnectionCriticalTrafficDoesNotResetMetadataBatch(t *testing.T) {
	model := &networkPriorityTestModel{
		TestModel:  newTestModel(),
		priorities: map[string]int{"folder": 10},
	}
	c, writer, initialWrite := newControlledProtocolConnection(t, c0ID, model)
	close(initialWrite.complete)
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	t.Cleanup(cancelRequest)

	go c.Index(context.Background(), &Index{Folder: "folder", LastSequence: 1})
	firstMetadata := awaitControlledWireWrite(t, writer)
	go c.IndexUpdate(context.Background(), &IndexUpdate{Folder: "folder", LastSequence: 2})
	requestResult := make(chan error, 1)
	go func() {
		_, err := c.Request(requestCtx, &Request{Folder: "folder", Name: "file", Size: 1})
		requestResult <- err
	}()
	criticalReturned := make(chan struct{})
	go func() {
		c.ClusterConfig(&ClusterConfig{}, nil)
		close(criticalReturned)
	}()
	awaitQueuedOutputs(t, c, 3)

	close(firstMetadata.complete)
	criticalWrite := awaitControlledWireWrite(t, writer)
	if got := messageTypeFromWire(t, criticalWrite.data); got != bep.MessageType_MESSAGE_TYPE_CLUSTER_CONFIG {
		t.Fatalf("frame after metadata is %v, expected queued Connection-Critical Traffic", got)
	}
	close(criticalWrite.complete)
	<-criticalReturned

	blockWrite := awaitControlledWireWrite(t, writer)
	if got := messageTypeFromWire(t, blockWrite.data); got != bep.MessageType_MESSAGE_TYPE_REQUEST {
		t.Fatalf("frame after Connection-Critical Traffic is %v, expected pending block turn", got)
	}
	close(blockWrite.complete)
	cancelRequest()
	if err := <-requestResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("Request returned %v, expected cancellation after observing its frame", err)
	}

	metadataWrite := awaitControlledWireWrite(t, writer)
	if got := messageTypeFromWire(t, metadataWrite.data); got != bep.MessageType_MESSAGE_TYPE_INDEX_UPDATE {
		t.Fatalf("frame after block turn is %v, expected next metadata batch", got)
	}
	close(metadataWrite.complete)
}

func TestMetadataBatchStateResetsWhenPriorityBucketEmpties(t *testing.T) {
	model := &networkPriorityTestModel{
		TestModel:  newTestModel(),
		priorities: map[string]int{"folder": 10},
	}
	c, writer, initialWrite := newControlledProtocolConnection(t, c0ID, model)
	close(initialWrite.complete)
	requestCtx, cancelRequest := context.WithCancel(context.Background())
	t.Cleanup(cancelRequest)

	firstResult := make(chan error, 1)
	go func() { firstResult <- c.Index(context.Background(), &Index{Folder: "folder", LastSequence: 1}) }()
	firstMetadata := awaitControlledWireWrite(t, writer)
	close(firstMetadata.complete)
	if err := <-firstResult; err != nil {
		t.Fatal(err)
	}

	criticalReturned := make(chan struct{})
	go func() {
		c.ClusterConfig(&ClusterConfig{}, nil)
		close(criticalReturned)
	}()
	criticalWrite := awaitControlledWireWrite(t, writer)
	go c.IndexUpdate(context.Background(), &IndexUpdate{Folder: "folder", LastSequence: 2})
	requestResult := make(chan error, 1)
	go func() {
		_, err := c.Request(requestCtx, &Request{Folder: "folder", Name: "file", Size: 1})
		requestResult <- err
	}()
	awaitQueuedOutputs(t, c, 2)

	close(criticalWrite.complete)
	<-criticalReturned
	metadataWrite := awaitControlledWireWrite(t, writer)
	if got := messageTypeFromWire(t, metadataWrite.data); got != bep.MessageType_MESSAGE_TYPE_INDEX_UPDATE {
		t.Fatalf("first frame in refilled bucket is %v, expected a fresh metadata batch", got)
	}
	close(metadataWrite.complete)
	blockWrite := awaitControlledWireWrite(t, writer)
	if got := messageTypeFromWire(t, blockWrite.data); got != bep.MessageType_MESSAGE_TYPE_REQUEST {
		t.Fatalf("frame after fresh metadata batch is %v, expected block turn", got)
	}
	close(blockWrite.complete)
	cancelRequest()
	if err := <-requestResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("Request returned %v, expected cancellation after observing its frame", err)
	}
}

func TestMetadataBatchStateIsScopedByConnection(t *testing.T) {
	model := &networkPriorityTestModel{
		TestModel:  newTestModel(),
		priorities: map[string]int{"folder": 10},
	}
	connectionA, writerA, initialA := newControlledProtocolConnection(t, c0ID, model)
	connectionB, writerB, initialB := newControlledProtocolConnection(t, c1ID, model)
	close(initialA.complete)
	requestCtxA, cancelRequestA := context.WithCancel(context.Background())
	requestCtxB, cancelRequestB := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancelRequestA()
		cancelRequestB()
	})

	go connectionA.Index(context.Background(), &Index{Folder: "folder", LastSequence: 1})
	firstA := awaitControlledWireWrite(t, writerA)
	go connectionA.IndexUpdate(context.Background(), &IndexUpdate{Folder: "folder", LastSequence: 2})
	requestResultA := make(chan error, 1)
	go func() {
		_, err := connectionA.Request(requestCtxA, &Request{Folder: "folder", Name: "a", Size: 1})
		requestResultA <- err
	}()
	awaitQueuedOutputs(t, connectionA, 2)

	go connectionB.Index(context.Background(), &Index{Folder: "folder", LastSequence: 1})
	requestResultB := make(chan error, 1)
	go func() {
		_, err := connectionB.Request(requestCtxB, &Request{Folder: "folder", Name: "b", Size: 1})
		requestResultB <- err
	}()
	awaitQueuedOutputs(t, connectionB, 2)

	close(firstA.complete)
	nextA := awaitControlledWireWrite(t, writerA)
	if got := messageTypeFromWire(t, nextA.data); got != bep.MessageType_MESSAGE_TYPE_REQUEST {
		t.Fatalf("connection A frame is %v, expected its pending block turn", got)
	}
	close(initialB.complete)
	nextB := awaitControlledWireWrite(t, writerB)
	if got := messageTypeFromWire(t, nextB.data); got != bep.MessageType_MESSAGE_TYPE_INDEX {
		t.Fatalf("connection B frame is %v, expected its independent initial metadata batch", got)
	}

	close(nextA.complete)
	cancelRequestA()
	if err := <-requestResultA; !errors.Is(err, context.Canceled) {
		t.Fatalf("connection A Request returned %v", err)
	}
	remainingA := awaitControlledWireWrite(t, writerA)
	close(remainingA.complete)
	close(nextB.complete)
	nextBlockB := awaitControlledWireWrite(t, writerB)
	close(nextBlockB.complete)
	cancelRequestB()
	if err := <-requestResultB; !errors.Is(err, context.Canceled) {
		t.Fatalf("connection B Request returned %v", err)
	}
}

func TestMetadataBatchStateIsScopedByNetworkPriority(t *testing.T) {
	model := &networkPriorityTestModel{
		TestModel: newTestModel(),
		priorities: map[string]int{
			"low":  10,
			"high": 20,
		},
	}
	c, writer, initialWrite := newControlledProtocolConnection(t, c0ID, model)
	close(initialWrite.complete)
	lowCtx, cancelLow := context.WithCancel(context.Background())
	highCtx, cancelHigh := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancelLow()
		cancelHigh()
	})

	go c.Index(context.Background(), &Index{Folder: "low", LastSequence: 1})
	firstLow := awaitControlledWireWrite(t, writer)
	go c.IndexUpdate(context.Background(), &IndexUpdate{Folder: "low", LastSequence: 2})
	lowResult := make(chan error, 1)
	go func() {
		_, err := c.Request(lowCtx, &Request{Folder: "low", Name: "low", Size: 1})
		lowResult <- err
	}()
	go c.Index(context.Background(), &Index{Folder: "high", LastSequence: 1})
	highResult := make(chan error, 1)
	go func() {
		_, err := c.Request(highCtx, &Request{Folder: "high", Name: "high", Size: 1})
		highResult <- err
	}()
	awaitQueuedOutputs(t, c, 4)

	close(firstLow.complete)
	highMetadata := awaitControlledWireWrite(t, writer)
	if got := outputFolderFromWire(t, highMetadata.data); got != "high" {
		t.Fatalf("first competing folder is %q, expected fresh high-priority metadata batch", got)
	}
	close(highMetadata.complete)
	highBlock := awaitControlledWireWrite(t, writer)
	if got := outputFolderFromWire(t, highBlock.data); got != "high" {
		t.Fatalf("second competing folder is %q, expected high-priority block turn", got)
	}
	close(highBlock.complete)
	cancelHigh()
	if err := <-highResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("high-priority Request returned %v", err)
	}

	lowBlock := awaitControlledWireWrite(t, writer)
	if got := outputFolderFromWire(t, lowBlock.data); got != "low" || messageTypeFromWire(t, lowBlock.data) != bep.MessageType_MESSAGE_TYPE_REQUEST {
		t.Fatalf("next frame is %q, expected preserved low-priority block turn", got)
	}
	close(lowBlock.complete)
	cancelLow()
	if err := <-lowResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("low-priority Request returned %v", err)
	}
	lowMetadata := awaitControlledWireWrite(t, writer)
	if got := outputFolderFromWire(t, lowMetadata.data); got != "low" || messageTypeFromWire(t, lowMetadata.data) != bep.MessageType_MESSAGE_TYPE_INDEX_UPDATE {
		t.Fatalf("last frame is %q, expected remaining low-priority metadata", got)
	}
	close(lowMetadata.complete)
}

func TestRequestErrorsPreserveAdmissionOrder(t *testing.T) {
	ready := make(chan struct{})
	close(ready)
	firstMayReturn := make(chan struct{})
	firstStarted := make(chan struct{})
	first := newOrderedFakeRequestError(ready)
	second := newOrderedFakeRequestResponse(first.closed)

	m := newTestModel()
	m.requestFn = func(req *Request) (RequestResponse, error) {
		switch req.ID {
		case 1:
			close(firstStarted)
			<-firstMayReturn
			return nil, first
		case 2:
			return second, nil
		default:
			return nil, errors.New("unexpected request")
		}
	}
	writer := &controlledWireWriter{writes: make(chan controlledWireWrite)}
	c := getRawConnection(NewConnection(c0ID, &testutil.NoopRW{}, writer, testutil.NoopCloser{}, m, new(mockedConnectionInfo), CompressionNever, testKeyGen))
	writerStopped := make(chan struct{})
	go func() { c.writerLoop(); close(writerStopped) }()
	go c.ClusterConfig(&ClusterConfig{}, nil)
	initialWrite := awaitControlledWireWrite(t, writer)
	close(initialWrite.complete)
	t.Cleanup(func() {
		close(c.closed)
		<-writerStopped
	})

	go c.handleRequest(&Request{ID: 1})
	<-firstStarted
	go c.handleRequest(&Request{ID: 2})

	select {
	case write := <-writer.writes:
		close(write.complete)
		t.Fatalf("response %d overtook the admitted error response", responseIDFromWire(t, write.data))
	case <-second.waitStarted:
	}

	close(firstMayReturn)
	<-first.waitStarted
	firstWrite := awaitControlledWireWrite(t, writer)
	if id := responseIDFromWire(t, firstWrite.data); id != 1 {
		t.Fatalf("first response ID is %d, expected 1", id)
	}
	close(firstWrite.complete)

	secondWrite := awaitControlledWireWrite(t, writer)
	if id := responseIDFromWire(t, secondWrite.data); id != 2 {
		t.Fatalf("second response ID is %d, expected 2", id)
	}
	close(secondWrite.complete)
}

func TestEncryptedResponsePreservesAdmissionLifecycle(t *testing.T) {
	ready := make(chan struct{})
	underlying := newOrderedFakeRequestResponse(ready)
	response := newRawResponse([]byte("encrypted"), underlying)

	waiting := make(chan struct{})
	go func() {
		response.WaitForResponse()
		close(waiting)
	}()
	<-underlying.waitStarted
	select {
	case <-waiting:
		t.Fatal("encrypted response bypassed the underlying admission order")
	default:
	}

	close(ready)
	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("encrypted response did not follow the underlying admission order")
	}
	response.Close()
	select {
	case <-underlying.closed:
	default:
		t.Fatal("encrypted response did not complete the underlying admission")
	}
}

func TestEncryptedResponseUsesLegacyLifecycleWithoutAdmission(t *testing.T) {
	ready := make(chan struct{})
	close(ready)
	underlying := newOrderedFakeRequestResponse(ready)
	underlying.retainForTransmission = false

	response := newRawResponse([]byte("encrypted"), underlying)
	select {
	case <-underlying.closed:
	default:
		t.Fatal("legacy encrypted response retained the unencrypted response")
	}
	if response.response != nil {
		t.Fatal("legacy encrypted response kept the unencrypted response after encryption")
	}
}

type orderedFakeRequestResponse struct {
	data                  []byte
	previous              <-chan struct{}
	closed                chan struct{}
	waitStarted           chan struct{}
	retainForTransmission bool
	waitOnce              sync.Once
	closeOnce             sync.Once
}

type networkPriorityTestModel struct {
	*TestModel
	priorities map[string]int
}

func (m *networkPriorityTestModel) NetworkPriority(folder string) NetworkPriorityPolicy {
	return NetworkPriorityPolicy{Priority: m.priorities[folder]}
}

func newOrderedFakeRequestResponse(previous <-chan struct{}) *orderedFakeRequestResponse {
	return &orderedFakeRequestResponse{
		previous:              previous,
		closed:                make(chan struct{}),
		waitStarted:           make(chan struct{}),
		retainForTransmission: true,
	}
}

func (r *orderedFakeRequestResponse) Data() []byte {
	return r.data
}

func (r *orderedFakeRequestResponse) Close() {
	r.closeOnce.Do(func() { close(r.closed) })
}

func (r *orderedFakeRequestResponse) Wait() {
	<-r.closed
}

func (r *orderedFakeRequestResponse) WaitForResponse() {
	r.waitOnce.Do(func() { close(r.waitStarted) })
	<-r.previous
}

func (r *orderedFakeRequestResponse) RetainForTransmission() bool {
	return r.retainForTransmission
}

type orderedFakeRequestError struct {
	previous    <-chan struct{}
	closed      chan struct{}
	waitStarted chan struct{}
	waitOnce    sync.Once
	closeOnce   sync.Once
}

func newOrderedFakeRequestError(previous <-chan struct{}) *orderedFakeRequestError {
	return &orderedFakeRequestError{
		previous:    previous,
		closed:      make(chan struct{}),
		waitStarted: make(chan struct{}),
	}
}

func (*orderedFakeRequestError) Error() string {
	return "request failed after admission"
}

func (e *orderedFakeRequestError) Close() {
	e.closeOnce.Do(func() { close(e.closed) })
}

func (e *orderedFakeRequestError) WaitForResponse() {
	e.waitOnce.Do(func() { close(e.waitStarted) })
	<-e.previous
}

type controlledWireWriter struct {
	writes chan controlledWireWrite
	stop   chan struct{}
}

type controlledWireWrite struct {
	data     []byte
	complete chan struct{}
}

func (w *controlledWireWriter) Write(data []byte) (int, error) {
	write := controlledWireWrite{
		data:     append([]byte(nil), data...),
		complete: make(chan struct{}),
	}
	w.writes <- write
	if w.stop == nil {
		<-write.complete
	} else {
		select {
		case <-write.complete:
		case <-w.stop:
		}
	}
	return len(data), nil
}

func newControlledProtocolConnection(t *testing.T, deviceID DeviceID, model Model) (*rawConnection, *controlledWireWriter, controlledWireWrite) {
	t.Helper()
	writer := &controlledWireWriter{
		writes: make(chan controlledWireWrite),
		stop:   make(chan struct{}),
	}
	connection := getRawConnection(NewConnection(deviceID, &testutil.NoopRW{}, writer, testutil.NoopCloser{}, model, new(mockedConnectionInfo), CompressionNever, testKeyGen))
	writerStopped := make(chan struct{})
	go func() {
		connection.writerLoop()
		close(writerStopped)
	}()
	go connection.ClusterConfig(&ClusterConfig{}, nil)
	initialWrite := awaitControlledWireWrite(t, writer)
	t.Cleanup(func() {
		close(connection.closed)
		close(writer.stop)
		<-writerStopped
	})
	return connection, writer, initialWrite
}

func awaitControlledWireWrite(t *testing.T, writer *controlledWireWriter) controlledWireWrite {
	t.Helper()
	select {
	case write := <-writer.writes:
		return write
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for wire write")
		return controlledWireWrite{}
	}
}

func awaitQueuedOutputs(t *testing.T, c *rawConnection, count int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		c.outputMut.Lock()
		queued := len(c.outputQueue)
		c.outputMut.Unlock()
		if queued >= count {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("timed out waiting for %d queued protocol outputs", count)
}

func messageTypeFromWire(t *testing.T, data []byte) bep.MessageType {
	t.Helper()
	headerSize := int(binary.BigEndian.Uint16(data))
	var header bep.Header
	if err := proto.Unmarshal(data[2:2+headerSize], &header); err != nil {
		t.Fatal(err)
	}
	return header.Type
}

func indexFolderFromWire(t *testing.T, data []byte) string {
	t.Helper()
	var index bep.Index
	if err := proto.Unmarshal(wireMessagePayload(data), &index); err != nil {
		t.Fatal(err)
	}
	return index.Folder
}

func outputFolderFromWire(t *testing.T, data []byte) string {
	t.Helper()
	switch messageTypeFromWire(t, data) {
	case bep.MessageType_MESSAGE_TYPE_INDEX:
		var message bep.Index
		if err := proto.Unmarshal(wireMessagePayload(data), &message); err != nil {
			t.Fatal(err)
		}
		return message.Folder
	case bep.MessageType_MESSAGE_TYPE_INDEX_UPDATE:
		var message bep.IndexUpdate
		if err := proto.Unmarshal(wireMessagePayload(data), &message); err != nil {
			t.Fatal(err)
		}
		return message.Folder
	case bep.MessageType_MESSAGE_TYPE_REQUEST:
		var message bep.Request
		if err := proto.Unmarshal(wireMessagePayload(data), &message); err != nil {
			t.Fatal(err)
		}
		return message.Folder
	default:
		t.Fatalf("message %v has no folder", messageTypeFromWire(t, data))
		return ""
	}
}

func responseIDFromWire(t *testing.T, data []byte) int {
	t.Helper()
	var response bep.Response
	if err := proto.Unmarshal(wireMessagePayload(data), &response); err != nil {
		t.Fatal(err)
	}
	return int(response.Id)
}

func wireMessagePayload(data []byte) []byte {
	headerSize := int(binary.BigEndian.Uint16(data))
	return data[2+headerSize+4:]
}

func TestRequestInvalidFilename(t *testing.T) {
	m := newTestModel()
	rw := testutil.NewBlockingRW()
	c := getRawConnection(NewConnection(c0ID, rw, &testutil.NoopRW{}, testutil.NoopCloser{}, m, new(mockedConnectionInfo), CompressionAlways, testKeyGen))
	c.Start()
	defer closeAndWait(c, rw)

	c.inbox <- &bep.ClusterConfig{}
	c.inbox <- &bep.Request{
		Id:   1,
		Name: "../escape",
		Size: 1024,
		Hash: []byte{42},
	}

	select {
	case <-c.dispatcherLoopStopped:
	case <-time.After(time.Second):
		t.Fatal("timed out before dispatcher loop terminated")
	}

	err := m.closedError()
	if err == nil {
		t.Fatal("expected connection to be closed with an error")
	}
	if !strings.Contains(err.Error(), "protocol error") {
		t.Errorf("expected a protocol error, got %v", err)
	}
}

func TestIndexIDString(t *testing.T) {
	// Index ID is a 64 bit, zero padded hex integer.
	var i IndexID = 42
	if i.String() != "0x000000000000002A" {
		t.Error(i.String())
	}
}

func closeAndWait(c any, closers ...io.Closer) {
	for _, closer := range closers {
		closer.Close()
	}
	var raw *rawConnection
	switch i := c.(type) {
	case *rawConnection:
		raw = i
	default:
		raw = getRawConnection(c.(Connection))
	}
	raw.internalClose(ErrClosed)
	raw.loopWG.Wait()
}

func getRawConnection(c Connection) *rawConnection {
	var raw *rawConnection
	switch i := c.(type) {
	case wireFormatConnection:
		raw = i.Connection.(encryptedConnection).conn
	case encryptedConnection:
		raw = i.conn
	}
	return raw
}
