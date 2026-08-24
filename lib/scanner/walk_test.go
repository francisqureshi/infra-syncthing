// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package scanner

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	rdebug "runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/d4l3k/messagediff"
	"golang.org/x/text/unicode/norm"

	"github.com/syncthing/syncthing/lib/build"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/ignore"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/rand"
)

type testfile struct {
	name   string
	length int64
	hash   string
}

type testfileList []testfile

var testdata = testfileList{
	{"afile", 4, "b5bb9d8014a0f9b1d61e21e796d78dccdf1352f23cd32812f4850b878ae4944c"},
	{"dir1", 0, ""},
	{filepath.Join("dir1", "dfile"), 5, "49ae93732fcf8d63fe1cce759664982dbd5b23161f007dba8561862adc96d063"},
	{"dir2", 0, ""},
	{filepath.Join("dir2", "cfile"), 4, "bf07a7fbb825fc0aae7bf4a1177b2b31fcf8a3feeaf7092761e18c859ee52a9c"},
	{"excludes", 37, "df90b52f0c55dba7a7a940affe482571563b1ac57bd5be4d8a0291e7de928e06"},
	{"further-excludes", 5, "7eb0a548094fa6295f7fd9200d69973e5f5ec5c04f2a86d998080ac43ecf89f1"},
}

func init() {
	// This test runs the risk of entering infinite recursion if it fails.
	// Limit the stack size to 10 megs to crash early in that case instead of
	// potentially taking down the box...
	rdebug.SetMaxStack(10 * 1 << 20)
}

func newTestFs(opts ...fs.Option) fs.Filesystem {
	// This mirrors some test data we used to have in a physical `testdata`
	// directory here.
	tfs := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true&nostfolder=true", opts...)
	tfs.Mkdir("dir1", 0o755)
	tfs.Mkdir("dir2", 0o755)
	tfs.Mkdir("dir3", 0o755)
	tfs.MkdirAll("dir2/dir21/dir22/dir23", 0o755)
	tfs.MkdirAll("dir2/dir21/dir22/efile", 0o755)
	tfs.MkdirAll("dir2/dir21/dira", 0o755)
	tfs.MkdirAll("dir2/dir21/efile/ign", 0o755)
	fs.WriteFile(tfs, "dir1/cfile", []byte("baz\n"), 0o644)
	fs.WriteFile(tfs, "dir1/dfile", []byte("quux\n"), 0o644)
	fs.WriteFile(tfs, "dir2/cfile", []byte("baz\n"), 0o644)
	fs.WriteFile(tfs, "dir2/dfile", []byte("quux\n"), 0o644)
	fs.WriteFile(tfs, "dir2/dir21/dir22/dir23/efile", []byte("\n"), 0o644)
	fs.WriteFile(tfs, "dir2/dir21/dir22/efile/efile", []byte("\n"), 0o644)
	fs.WriteFile(tfs, "dir2/dir21/dir22/efile/ign/efile", []byte("\n"), 0o644)
	fs.WriteFile(tfs, "dir2/dir21/dira/efile", []byte("\n"), 0o644)
	fs.WriteFile(tfs, "dir2/dir21/dira/ffile", []byte("\n"), 0o644)
	fs.WriteFile(tfs, "dir2/dir21/efile/ign/efile", []byte("\n"), 0o644)
	fs.WriteFile(tfs, "dir2/dir21/cfile", []byte("foo\n"), 0o644)
	fs.WriteFile(tfs, "dir2/dir21/dfile", []byte("quux\n"), 0o644)
	fs.WriteFile(tfs, "dir3/cfile", []byte("foo\n"), 0o644)
	fs.WriteFile(tfs, "dir3/dfile", []byte("quux\n"), 0o644)
	fs.WriteFile(tfs, "afile", []byte("foo\n"), 0o644)
	fs.WriteFile(tfs, "bfile", []byte("bar\n"), 0o644)
	fs.WriteFile(tfs, ".stignore", []byte("#include excludes\n\nbfile\ndir1/cfile\n/dir2/dir21\n"), 0o644)
	fs.WriteFile(tfs, "excludes", []byte("dir2/dfile\n#include further-excludes\n"), 0o644)
	fs.WriteFile(tfs, "further-excludes", []byte("dir3\n"), 0o644)
	return tfs
}

func TestWalkSub(t *testing.T) {
	testFs := newTestFs()
	ignores := ignore.New(testFs)
	err := ignores.Load(".stignore")
	if err != nil {
		t.Fatal(err)
	}

	cfg, cancel := testConfig()
	defer cancel()
	cfg.Subs = []string{"dir2"}
	cfg.Matcher = ignores
	fchan := Walk(context.TODO(), cfg).Results
	var files []protocol.FileInfo
	for f := range fchan {
		if f.Err != nil {
			t.Errorf("Error while scanning %v: %v", f.Err, f.Path)
		}
		files = append(files, f.File)
	}

	// The directory contains two files, where one is ignored from a higher
	// level. We should see only the directory and one of the files.

	if len(files) != 2 {
		t.Fatalf("Incorrect length %d != 2", len(files))
	}
	if files[0].Name != "dir2" {
		t.Errorf("Incorrect file %v != dir2", files[0])
	}
	if files[1].Name != filepath.Join("dir2", "cfile") {
		t.Errorf("Incorrect file %v != dir2/cfile", files[1])
	}
}

func TestWalk(t *testing.T) {
	testFs := newTestFs()
	ignores := ignore.New(testFs)
	err := ignores.Load(".stignore")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(ignores)

	cfg, cancel := testConfig()
	defer cancel()
	cfg.Matcher = ignores
	fchan := Walk(context.TODO(), cfg).Results

	var tmp []protocol.FileInfo
	for f := range fchan {
		if f.Err != nil {
			t.Errorf("Error while scanning %v: %v", f.Err, f.Path)
		}
		tmp = append(tmp, f.File)
	}
	slices.SortFunc(fileList(tmp), compareByName)
	files := fileList(tmp).testfiles()

	if diff, equal := messagediff.PrettyDiff(testdata, files); !equal {
		t.Errorf("Walk returned unexpected data. Diff:\n%s", diff)
		t.Error(testdata[4], files[4])
	}
}

func TestVerify(t *testing.T) {
	blocksize := 16
	// data should be an even multiple of blocksize long
	data := []byte("Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut e")
	buf := bytes.NewBuffer(data)
	progress := newByteCounter()
	defer progress.Close()

	blocks, err := Blocks(context.TODO(), buf, blocksize, -1, progress)
	if err != nil {
		t.Fatal(err)
	}
	if exp := len(data) / blocksize; len(blocks) != exp {
		t.Fatalf("Incorrect number of blocks %d != %d", len(blocks), exp)
	}

	if int64(len(data)) != progress.Total() {
		t.Fatalf("Incorrect counter value %d  != %d", len(data), progress.Total())
	}

	buf = bytes.NewBuffer(data)
	err = verify(buf, blocksize, blocks)
	t.Log(err)
	if err != nil {
		t.Fatal("Unexpected verify failure", err)
	}

	buf = bytes.NewBuffer(append(data, '\n'))
	err = verify(buf, blocksize, blocks)
	t.Log(err)
	if err == nil {
		t.Fatal("Unexpected verify success")
	}

	buf = bytes.NewBuffer(data[:len(data)-1])
	err = verify(buf, blocksize, blocks)
	t.Log(err)
	if err == nil {
		t.Fatal("Unexpected verify success")
	}

	data[42] = 42
	buf = bytes.NewBuffer(data)
	err = verify(buf, blocksize, blocks)
	t.Log(err)
	if err == nil {
		t.Fatal("Unexpected verify success")
	}
}

func TestNormalization(t *testing.T) {
	if build.IsDarwin {
		t.Skip("Normalization test not possible on darwin")
		return
	}

	testFs := newTestFs()

	tests := []string{
		"0-A",            // ASCII A -- accepted
		"1-\xC3\x84",     // NFC 'Ä' -- conflicts with the entry below, accepted
		"1-\x41\xCC\x88", // NFD 'Ä' -- conflicts with the entry above, ignored
		"2-\xC3\x85",     // NFC 'Å' -- accepted
		"3-\x41\xCC\x83", // NFD 'Ã' -- converted to NFC
		"4-\xE2\x98\x95", // U+2615 HOT BEVERAGE (☕) -- accepted
		"5-\xCD\xE2",     // EUC-CN "wài" (外) -- ignored (not UTF8)
	}
	numInvalid := 2

	numValid := len(tests) - numInvalid

	for _, s1 := range tests {
		// Create a directory for each of the interesting strings above
		if err := testFs.MkdirAll(filepath.Join("normalization", s1), 0o755); err != nil {
			t.Fatal(err)
		}

		for _, s2 := range tests {
			// Within each dir, create a file with each of the interesting
			// file names. Ensure that the file doesn't exist when it's
			// created. This detects and fails if there's file name
			// normalization stuff at the filesystem level.
			if fd, err := testFs.OpenFile(filepath.Join("normalization", s1, s2), os.O_CREATE|os.O_EXCL, 0o644); err != nil {
				t.Fatal(err)
			} else {
				if _, err := fd.Write([]byte("test")); err != nil {
					t.Fatal(err)
				}
				if err := fd.Close(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	// We can normalize a directory name, but we can't descend into it in the
	// same pass due to how filepath.Walk works. So we run the scan twice to
	// make sure it all gets done. In production, things will be correct
	// eventually...

	walkDir(testFs, "normalization", nil, nil, 0)
	tmp := walkDir(testFs, "normalization", nil, nil, 0)

	files := fileList(tmp).testfiles()

	// We should have one file per combination, plus the directories
	// themselves, plus the "testdata/normalization" directory

	expectedNum := numValid*numValid + numValid + 1
	if len(files) != expectedNum {
		t.Errorf("Expected %d files, got %d, numvalid %d", expectedNum, len(files), numValid)
	}

	// The file names should all be in NFC form.

	for _, f := range files {
		t.Logf("%q (% x) %v", f.name, f.name, norm.NFC.IsNormalString(f.name))
		if !norm.NFC.IsNormalString(f.name) {
			t.Errorf("File name %q is not NFC normalized", f.name)
		}
	}
}

func TestNormalizationDarwinCaseFS(t *testing.T) {
	// This tests that normalization works on Darwin, through a CaseFS.

	if !build.IsDarwin {
		t.Skip("Normalization test not possible on non-Darwin")
		return
	}

	testFs := newTestFs(new(fs.OptionDetectCaseConflicts))

	testFs.RemoveAll("normalization")
	defer testFs.RemoveAll("normalization")
	testFs.MkdirAll("normalization", 0o755)

	const (
		inNFC = "\xC3\x84"
		inNFD = "\x41\xCC\x88"
	)

	// Create dir in NFC
	if err := testFs.Mkdir(filepath.Join("normalization", "dir-"+inNFC), 0o755); err != nil {
		t.Fatal(err)
	}

	// Create file in NFC
	fd, err := testFs.Create(filepath.Join("normalization", "dir-"+inNFC, "file-"+inNFC))
	if err != nil {
		t.Fatal(err)
	}
	fd.Close()

	// Walk, which should normalize and return
	walkDir(testFs, "normalization", nil, nil, 0)
	tmp := walkDir(testFs, "normalization", nil, nil, 0)
	if len(tmp) != 3 {
		t.Error("Expected one file and one dir scanned")
	}

	// Verify we see the normalized entries in the result
	foundFile := false
	foundDir := false
	for _, f := range tmp {
		if f.Name == filepath.Join("normalization", "dir-"+inNFD) {
			foundDir = true
			continue
		}
		if f.Name == filepath.Join("normalization", "dir-"+inNFD, "file-"+inNFD) {
			foundFile = true
			continue
		}
	}
	if !foundFile || !foundDir {
		t.Error("Didn't find expected normalization form")
	}
}

func TestIssue1507(_ *testing.T) {
	w := &walker{}
	w.Matcher = ignore.New(w.Filesystem)
	h := make(chan protocol.FileInfo, 100)
	f := make(chan ScanResult, 100)
	fn := w.walkAndHashFiles(context.TODO(), h, f)

	fn("", nil, protocol.ErrClosed)
}

func TestWalkSymlinkUnix(t *testing.T) {
	if build.IsWindows {
		t.Skip("skipping unsupported symlink test")
		return
	}

	// Create a folder with a symlink in it
	os.RemoveAll("_symlinks")
	os.Mkdir("_symlinks", 0o755)
	defer os.RemoveAll("_symlinks")
	os.Symlink("../testdata", "_symlinks/link")

	fs := fs.NewFilesystem(fs.FilesystemTypeBasic, "_symlinks")
	for _, path := range []string{".", "link"} {
		// Scan it
		files := walkDir(fs, path, nil, nil, 0)

		// Verify that we got one symlink and with the correct attributes
		if len(files) != 1 {
			t.Errorf("expected 1 symlink, not %d", len(files))
		}
		if len(files[0].Blocks) != 0 {
			t.Errorf("expected zero blocks for symlink, not %d", len(files[0].Blocks))
		}
		if string(files[0].SymlinkTarget) != "../testdata" {
			t.Errorf("expected symlink to have target destination, not %q", files[0].SymlinkTarget)
		}
	}
}

func TestBlocksizeHysteresis(t *testing.T) {
	// Verify that we select the right block size in the presence of old
	// file information.

	if testing.Short() {
		t.Skip("long and hard test")
	}

	sf := fs.NewWalkFilesystem(&singleFileFS{
		name:     "testfile.dat",
		filesize: 500 << 20, // 500 MiB
	})

	current := make(fakeCurrentFiler)

	runTest := func(expectedBlockSize int) {
		files := walkDir(sf, ".", current, nil, 0)
		if len(files) != 1 {
			t.Fatalf("expected one file, not %d", len(files))
		}
		if s := files[0].BlockSize(); s != expectedBlockSize {
			t.Fatalf("incorrect block size %d != expected %d", s, expectedBlockSize)
		}
	}

	// Scan with no previous knowledge. We should get a 512 KiB block size.

	runTest(512 << 10)

	// Scan on the assumption that previous size was 256 KiB. Retain 256 KiB
	// block size.

	current["testfile.dat"] = protocol.FileInfo{
		Name:         "testfile.dat",
		Size:         500 << 20,
		RawBlockSize: 256 << 10,
	}
	runTest(256 << 10)

	// Scan on the assumption that previous size was 1 MiB. Retain 1 MiB
	// block size.

	current["testfile.dat"] = protocol.FileInfo{
		Name:         "testfile.dat",
		Size:         500 << 20,
		RawBlockSize: 1 << 20,
	}
	runTest(1 << 20)

	// Scan on the assumption that previous size was 128 KiB. Move to 512
	// KiB because the difference is large.

	current["testfile.dat"] = protocol.FileInfo{
		Name:         "testfile.dat",
		Size:         500 << 20,
		RawBlockSize: 128 << 10,
	}
	runTest(512 << 10)

	// Scan on the assumption that previous size was 2 MiB. Move to 512
	// KiB because the difference is large.

	current["testfile.dat"] = protocol.FileInfo{
		Name:         "testfile.dat",
		Size:         500 << 20,
		RawBlockSize: 2 << 20,
	}
	runTest(512 << 10)
}

func TestWalkReceiveOnly(t *testing.T) {
	sf := fs.NewWalkFilesystem(&singleFileFS{
		name:     "testfile.dat",
		filesize: 1024,
	})

	current := make(fakeCurrentFiler)

	// Initial scan, no files in the CurrentFiler. Should pick up the file and
	// set the ReceiveOnly flag on it, because that's the flag we give the
	// walker to set.

	files := walkDir(sf, ".", current, nil, protocol.FlagLocalReceiveOnly)
	if len(files) != 1 {
		t.Fatal("Should have scanned one file")
	}

	if files[0].LocalFlags != protocol.FlagLocalReceiveOnly {
		t.Fatal("Should have set the ReceiveOnly flag")
	}

	// Update the CurrentFiler and scan again. It should not return
	// anything, because the file has not changed. This verifies that the
	// ReceiveOnly flag is properly ignored and doesn't trigger a rescan
	// every time.

	cur := files[0]
	current[cur.Name] = cur

	files = walkDir(sf, ".", current, nil, protocol.FlagLocalReceiveOnly)
	if len(files) != 0 {
		t.Fatal("Should not have scanned anything")
	}

	// Now pretend the file was previously ignored instead. We should pick up
	// the difference in flags and set just the LocalReceive flags.

	cur.LocalFlags = protocol.FlagLocalIgnored
	current[cur.Name] = cur

	files = walkDir(sf, ".", current, nil, protocol.FlagLocalReceiveOnly)
	if len(files) != 1 {
		t.Fatal("Should have scanned one file")
	}

	if files[0].LocalFlags != protocol.FlagLocalReceiveOnly {
		t.Fatal("Should have set the ReceiveOnly flag")
	}
}

func TestScanOwnershipPOSIX(t *testing.T) {
	// This test works on all operating systems because the FakeFS is always POSIXy.

	fakeFS := fs.NewFilesystem(fs.FilesystemTypeFake, "TestScanOwnership")
	current := make(fakeCurrentFiler)

	fakeFS.Create("root-owned")
	fakeFS.Create("user-owned")
	fakeFS.Lchown("user-owned", "1234", "5678")
	fakeFS.Mkdir("user-owned-dir", 0o755)
	fakeFS.Lchown("user-owned-dir", "2345", "6789")

	expected := []struct {
		name     string
		uid, gid int
	}{
		{"root-owned", 0, 0},
		{"user-owned", 1234, 5678},
		{"user-owned-dir", 2345, 6789},
	}

	files := walkDir(fakeFS, ".", current, nil, 0)
	if len(files) != len(expected) {
		t.Fatalf("expected %d items, not %d", len(expected), len(files))
	}
	for i := range expected {
		if files[i].Name != expected[i].name {
			t.Errorf("expected %s, got %s", expected[i].name, files[i].Name)
			continue
		}

		if files[i].Platform.Unix == nil {
			t.Error("failed to load POSIX data on", files[i].Name)
			continue
		}
		if files[i].Platform.Unix.UID != expected[i].uid {
			t.Errorf("expected %d, got %d", expected[i].uid, files[i].Platform.Unix.UID)
		}
		if files[i].Platform.Unix.GID != expected[i].gid {
			t.Errorf("expected %d, got %d", expected[i].gid, files[i].Platform.Unix.GID)
		}
	}
}

func TestScanOwnershipWindows(t *testing.T) {
	if !build.IsWindows {
		t.Skip("This test only works on Windows")
	}

	testFS := fs.NewFilesystem(fs.FilesystemTypeBasic, t.TempDir())
	current := make(fakeCurrentFiler)

	fd, err := testFS.Create("user-owned")
	if err != nil {
		t.Fatal(err)
	}
	fd.Close()

	files := walkDir(testFS, ".", current, nil, 0)
	if len(files) != 1 {
		t.Fatalf("expected %d items, not %d", 1, len(files))
	}
	t.Log(files[0])

	// The file should have an owner name set.
	if files[0].Platform.Windows == nil {
		t.Fatal("failed to load Windows data")
	}
	if files[0].Platform.Windows.OwnerName == "" {
		t.Errorf("expected owner name to be set")
	}
}

func walkDir(fs fs.Filesystem, dir string, cfiler CurrentFiler, matcher *ignore.Matcher, localFlags protocol.FlagLocal) []protocol.FileInfo {
	cfg, cancel := testConfig()
	defer cancel()
	cfg.Filesystem = fs
	cfg.Subs = []string{dir}
	cfg.AutoNormalize = true
	cfg.CurrentFiler = cfiler
	cfg.Matcher = matcher
	cfg.LocalFlags = localFlags
	cfg.ScanOwnership = true
	fchan := Walk(context.TODO(), cfg).Results

	var tmp []protocol.FileInfo
	for f := range fchan {
		if f.Err == nil {
			tmp = append(tmp, f.File)
		}
	}
	slices.SortFunc(fileList(tmp), compareByName)

	return tmp
}

type fileList []protocol.FileInfo

func compareByName(a, b protocol.FileInfo) int {
	return strings.Compare(a.Name, b.Name)
}

func (l fileList) testfiles() testfileList {
	testfiles := make(testfileList, len(l))
	for i, f := range l {
		if len(f.Blocks) > 1 {
			panic("simple test case stuff only supports a single block per file")
		}
		testfiles[i] = testfile{name: f.Name, length: f.Size}
		if len(f.Blocks) == 1 {
			testfiles[i].hash = fmt.Sprintf("%x", f.Blocks[0].Hash)
		}
	}
	return testfiles
}

func (l testfileList) String() string {
	var b bytes.Buffer
	b.WriteString("{\n")
	for _, f := range l {
		fmt.Fprintf(&b, "  %s (%d bytes): %s\n", f.name, f.length, f.hash)
	}
	b.WriteString("}")
	return b.String()
}

const (
	testdataSize = 17<<20 + 1
	testdataName = "_random.data"
)

func BenchmarkHashFile(b *testing.B) {
	testFs := newDataFs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := HashFile(context.TODO(), "", testFs, testdataName, protocol.MinBlockSize, nil); err != nil {
			b.Fatal(err)
		}
	}

	b.SetBytes(testdataSize)
	b.ReportAllocs()
}

func newDataFs() fs.Filesystem {
	tfs := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true")
	fd, err := tfs.Create(testdataName)
	if err != nil {
		panic(err)
	}

	lr := io.LimitReader(rand.Reader, testdataSize)
	if _, err := io.Copy(fd, lr); err != nil {
		panic(err)
	}

	if err := fd.Close(); err != nil {
		panic(err)
	}

	return tfs
}

func TestStopWalk(t *testing.T) {
	// Create tree that is 100 levels deep, with each level containing 100
	// files (each 1 MB) and 100 directories (in turn containing 100 files
	// and 100 directories, etc). That is, in total > 100^100 files and as
	// many directories. It'll take a while to scan, giving us time to
	// cancel it and make sure the scan stops.

	// Use an errorFs as the backing fs for the rest of the interface
	// The way we get it is a bit hacky tho.
	errorFs := fs.NewFilesystem(fs.FilesystemType("error"), ".")
	fs := fs.NewWalkFilesystem(&infiniteFS{errorFs, 100, 100, 1e6})

	const numHashers = 4
	ctx, cancel := context.WithCancel(context.Background())
	cfg, cfgCancel := testConfig()
	defer cfgCancel()
	cfg.Filesystem = fs
	cfg.Hashers = numHashers
	cfg.ProgressTickIntervalS = -1 // Don't attempt to build the full list of files before starting to scan...
	walkResult := Walk(ctx, cfg)
	fchan := walkResult.Results

	// Receive a few entries to make sure the walker is up and running,
	// scanning both files and dirs. Do some quick sanity tests on the
	// returned file entries to make sure we are not just reading crap from
	// a closed channel or something.
	dirs := 0
	files := 0
	for {
		res := <-fchan
		if res.Err != nil {
			t.Errorf("Error while scanning %v: %v", res.Err, res.Path)
		}
		f := res.File
		t.Log("Scanned", f)
		if f.IsDirectory() {
			if f.Name == "" || f.Permissions == 0 {
				t.Error("Bad directory entry", f)
			}
			dirs++
		} else {
			if f.Name == "" || len(f.Blocks) == 0 || f.Permissions == 0 {
				t.Error("Bad file entry", f)
			}
			files++
		}
		if dirs > 5 && files > 5 {
			break
		}
	}

	// Cancel the walker.
	cancel()

	// Empty out any waiting entries and wait for the channel to close.
	// Count them, they should be zero or very few - essentially, each
	// hasher has the choice of returning a fully handled entry or
	// cancelling, but they should not start on another item. The scan
	// goroutine can also have one directory entry in-flight on finishedChan
	// (directories bypass the hashers).
	extra := 0
	for range fchan {
		extra++
	}
	awaitScannerSignal(t, walkResult.TraversalDone, "cancelled traversal completion")
	t.Log("Extra entries:", extra)
	if extra > numHashers+1 {
		t.Error("unexpected extra entries received after cancel")
	}
}

func TestBufferedWalkSignalsTraversalBeforeHashingCompletes(t *testing.T) {
	testWalkSignalsTraversalBeforeHashingCompletes(t, 0)
}

func TestBufferedWalkOwnsDiscoverySpoolUntilBoundedFeedDrains(t *testing.T) {
	underlying := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true")
	for _, name := range []string{"payload", "zz-queued"} {
		if err := fs.WriteFile(underlying, name, make([]byte, 128<<10), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	readStarted := make(chan struct{})
	readRelease := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-readRelease:
		default:
			close(readRelease)
		}
	})

	before := discoverySpoolSnapshot(t)
	cfg, cancel := testConfig()
	defer cancel()
	cfg.Folder = "buffered-spool-success"
	cfg.Filesystem = &readBarrierFilesystem{
		Filesystem: underlying,
		started:    readStarted,
		release:    readRelease,
	}
	cfg.Hashers = 1
	cfg.ProgressTickIntervalS = 1
	walkResult := Walk(t.Context(), cfg)

	awaitScannerSignal(t, walkResult.TraversalDone, "buffered traversal completion")
	awaitScannerSignal(t, readStarted, "buffered hashing to start")
	spoolPath := awaitNewDiscoverySpool(t, before)

	close(readRelease)
	var results []ScanResult
	for result := range walkResult.Results {
		results = append(results, result)
	}
	if len(results) != 2 {
		t.Fatalf("scan results = %v, want two completed files", results)
	}
	if _, err := os.Stat(spoolPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("discovery spool still exists after success: %v", err)
	}
}

func TestBufferedWalkRemovesDiscoverySpoolAfterResultConsumerStops(t *testing.T) {
	underlying := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true")
	for _, name := range []string{"a", "b", "c"} {
		if err := fs.WriteFile(underlying, name, make([]byte, 128<<10), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	before := discoverySpoolSnapshot(t)
	cfg, cfgCancel := testConfig()
	defer cfgCancel()
	cfg.Folder = "buffered-spool-consumer-failure"
	cfg.Filesystem = underlying
	cfg.Hashers = 1
	ctx, cancel := context.WithCancel(t.Context())
	walkResult := Walk(ctx, cfg)

	awaitScannerSignal(t, walkResult.TraversalDone, "buffered traversal completion")
	spoolPath := awaitNewDiscoverySpool(t, before)
	select {
	case result := <-walkResult.Results:
		if result.Err != nil {
			t.Fatal(result.Err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first buffered result")
	}

	// A model result-consumer failure cancels the scan context before it
	// returns. Do the same while unread discovery records remain.
	cancel()
	for range walkResult.Results {
	}
	if _, err := os.Stat(spoolPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("discovery spool still exists after result-consumer failure: %v", err)
	}
}

func TestBufferedWalkRemovesDiscoverySpoolAfterTraversalCancellation(t *testing.T) {
	underlying := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true")
	for _, name := range []string{"a", "b"} {
		if err := fs.WriteFile(underlying, name, []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	blocked := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	before := discoverySpoolSnapshot(t)
	cfg, cfgCancel := testConfig()
	defer cfgCancel()
	cfg.Folder = "buffered-spool-traversal-cancellation"
	cfg.Filesystem = &walkBarrierFilesystem{
		Filesystem: underlying,
		path:       "a",
		blocked:    blocked,
		release:    release,
	}
	ctx, cancel := context.WithCancel(t.Context())
	walkResult := Walk(ctx, cfg)

	awaitScannerSignal(t, blocked, "buffered traversal barrier")
	spoolPath := awaitNewDiscoverySpool(t, before)
	cancel()
	close(release)
	awaitScannerSignal(t, walkResult.TraversalDone, "cancelled buffered traversal completion")
	for range walkResult.Results {
	}
	if _, err := os.Stat(spoolPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("discovery spool still exists after traversal cancellation: %v", err)
	}
}

func TestBufferedWalkRemovesDiscoverySpoolAfterTraversalFailure(t *testing.T) {
	underlying := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true")
	if err := fs.WriteFile(underlying, "payload", []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	blocked := make(chan struct{})
	release := make(chan struct{})
	before := discoverySpoolSnapshot(t)
	cfg, cancel := testConfig()
	defer cancel()
	cfg.Folder = "buffered-spool-traversal-failure"
	cfg.Filesystem = &walkBarrierFilesystem{
		Filesystem: underlying,
		path:       "payload",
		blocked:    blocked,
		release:    release,
		walkErr:    errors.New("controlled traversal failure"),
	}
	walkResult := Walk(t.Context(), cfg)

	awaitScannerSignal(t, blocked, "buffered traversal failure barrier")
	spoolPath := awaitNewDiscoverySpool(t, before)
	close(release)
	awaitScannerSignal(t, walkResult.TraversalDone, "failed buffered traversal completion")
	for range walkResult.Results {
	}
	if _, err := os.Stat(spoolPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("discovery spool still exists after traversal failure: %v", err)
	}
}

func TestBufferedWalkRemovesDiscoverySpoolAfterHashingFailure(t *testing.T) {
	underlying := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true")
	if err := fs.WriteFile(underlying, "payload", []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	blocked := make(chan struct{})
	release := make(chan struct{})
	before := discoverySpoolSnapshot(t)
	cfg, cancel := testConfig()
	defer cancel()
	cfg.Folder = "buffered-spool-hashing-failure"
	cfg.Filesystem = &openErrorFilesystem{
		Filesystem: &walkBarrierFilesystem{
			Filesystem: underlying,
			path:       "payload",
			blocked:    blocked,
			release:    release,
		},
		path: "payload",
		err:  errors.New("controlled hashing failure"),
	}
	walkResult := Walk(t.Context(), cfg)

	awaitScannerSignal(t, blocked, "buffered hashing failure barrier")
	spoolPath := awaitNewDiscoverySpool(t, before)
	close(release)
	var hashingErr error
	for result := range walkResult.Results {
		hashingErr = errors.Join(hashingErr, result.Err)
	}
	if hashingErr == nil || !strings.Contains(hashingErr.Error(), "controlled hashing failure") {
		t.Fatalf("scan error = %v, want controlled hashing failure", hashingErr)
	}
	if _, err := os.Stat(spoolPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("discovery spool still exists after hashing failure: %v", err)
	}
}

func TestBufferedWalkBoundsSyntheticMultiTerabyteDiscovery(t *testing.T) {
	const (
		files            = 32 << 10
		logicalFileSize  = int64(64 << 30)
		maxRetainedBytes = 8 << 20
		window           = 2
	)
	filesystem := &logicalInventoryFilesystem{
		Filesystem: fs.NewFilesystem(fs.FilesystemType("error"), "."),
		files:      files,
		fileSize:   logicalFileSize,
	}
	coordinator := &inventorySourceHashCoordinator{
		submitted: make(chan struct{}, window),
	}
	cfg, cfgCancel := testConfig()
	defer cfgCancel()
	cfg.Folder = "buffered-spool-logical-inventory"
	cfg.Filesystem = filesystem
	cfg.Hashers = window
	cfg.SourceHashCoordinator = coordinator

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	ctx, cancel := context.WithCancel(t.Context())
	walkResult := Walk(ctx, cfg)
	awaitScannerSignal(t, walkResult.TraversalDone, "synthetic logical inventory traversal")
	for range window {
		awaitScannerSignal(t, coordinator.submitted, "bounded Source Hash Work submission")
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	var retained uint64
	if after.HeapAlloc > before.HeapAlloc {
		retained = after.HeapAlloc - before.HeapAlloc
	}
	if retained > maxRetainedBytes {
		t.Fatalf("buffered discovery retained %d bytes for %d files, want at most %d", retained, files, maxRetainedBytes)
	}
	if got := coordinator.enrolled.Load(); got != window {
		t.Fatalf("Source Hash Work materialized %d files, want bounded window %d", got, window)
	}
	if got := filesystem.opens.Load(); got != 0 {
		t.Fatalf("buffered discovery opened %d source handles before admission, want zero", got)
	}
	if logicalBytes := int64(files) * logicalFileSize; logicalBytes < 1<<40 {
		t.Fatalf("synthetic inventory is only %d bytes", logicalBytes)
	}

	cancel()
	for range walkResult.Results {
	}
}

func TestBufferedWalkPreservesFinalScanProgressTotals(t *testing.T) {
	underlying := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true")
	for name, data := range map[string][]byte{
		"a": []byte("abc"),
		"b": []byte("12345"),
	} {
		if err := fs.WriteFile(underlying, name, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cfg, cancel := testConfig()
	defer cancel()
	cfg.Folder = "buffered-spool-progress"
	cfg.Filesystem = underlying
	cfg.ProgressTickIntervalS = 1
	sub := cfg.EventLogger.Subscribe(events.FolderScanProgress)
	defer sub.Unsubscribe()
	walkResult := Walk(t.Context(), cfg)
	for result := range walkResult.Results {
		if result.Err != nil {
			t.Fatal(result.Err)
		}
	}

	select {
	case event := <-sub.C():
		data, ok := event.Data.(map[string]any)
		if !ok {
			t.Fatalf("progress event data = %T, want map", event.Data)
		}
		if got, want := data["folder"], cfg.Folder; got != want {
			t.Fatalf("progress folder = %v, want %v", got, want)
		}
		if got, want := data["current"], int64(8); got != want {
			t.Fatalf("progress current = %v, want %v", got, want)
		}
		if got, want := data["total"], int64(9); got != want {
			t.Fatalf("progress total = %v, want %v", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for final buffered progress event")
	}
}

func TestBufferedWalkReportsSpooledSourceHashWorkAndCleansUpState(t *testing.T) {
	underlying := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true")
	for _, name := range []string{"payload", "waiting"} {
		if err := fs.WriteFile(underlying, name, make([]byte, protocol.MinBlockSize), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	readStarted := make(chan struct{})
	readRelease := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-readRelease:
		default:
			close(readRelease)
		}
	})
	now := time.Unix(1_000, 0)
	coordinator := NewSourceHashCoordinator(1)
	coordinator.(*sourceHashCoordinator).now = func() time.Time { return now }
	provider := coordinator.(SourceHashWorkStateProvider)
	cfg, cfgCancel := testConfig()
	defer cfgCancel()
	cfg.Folder = "alpha"
	cfg.Filesystem = &sourceHashStateBarrierFilesystem{
		Filesystem: underlying,
		started:    readStarted,
		release:    readRelease,
	}
	cfg.Hashers = 1
	cfg.SourceHashFolder = SourceHashFolder{ID: "alpha"}
	cfg.SourceHashCoordinator = coordinator
	cfg.ProgressTickIntervalS = 1
	ctx, cancel := context.WithCancel(t.Context())
	walkResult := Walk(ctx, cfg)

	awaitScannerSignal(t, walkResult.TraversalDone, "buffered traversal completion")
	awaitScannerSignal(t, readStarted, "buffered Source Hash Work admission")
	now = now.Add(7 * time.Second)
	if got := provider.SourceHashWorkState("alpha"); got.Active != 1 || got.Queued != 1 || got.OldestSchedulingWaitSeconds != 7 || got.RetainedHandles != 1 {
		t.Fatalf("buffered Source Hash Work state = %#v, want one active, one waiting, seven seconds wait, and one handle", got)
	}

	cancel()
	close(readRelease)
	for range walkResult.Results {
	}
	if got := provider.SourceHashWorkState("alpha"); got.Active != 0 || got.Queued != 0 || got.OldestSchedulingWaitSeconds != 0 || got.RetainedHandles != 0 {
		t.Fatalf("buffered Source Hash Work state after cleanup = %#v", got)
	}
}

func TestBufferedWalkReportsSpooledSourceHashWorkDuringTraversal(t *testing.T) {
	underlying := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true")
	if err := fs.WriteFile(underlying, "payload", make([]byte, protocol.MinBlockSize), 0o644); err != nil {
		t.Fatal(err)
	}
	traversalBlocked := make(chan struct{})
	traversalRelease := make(chan struct{})
	spooled := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-traversalRelease:
		default:
			close(traversalRelease)
		}
	})
	now := time.Unix(1_000, 0)
	coordinator := NewSourceHashCoordinator(1)
	concrete := coordinator.(*sourceHashCoordinator)
	concrete.now = func() time.Time { return now }
	provider := coordinator.(SourceHashWorkStateProvider)
	observedCoordinator := &discoverySpoolObservationCoordinator{
		SourceHashCoordinator: coordinator,
		spooled:               spooled,
	}
	cfg, cfgCancel := testConfig()
	defer cfgCancel()
	cfg.Folder = "alpha"
	cfg.Filesystem = &discoverySpoolObservationFilesystem{
		Filesystem: underlying,
		path:       "payload",
		spooled:    spooled,
		blocked:    traversalBlocked,
		release:    traversalRelease,
	}
	cfg.Hashers = 1
	cfg.SourceHashFolder = SourceHashFolder{ID: "alpha"}
	cfg.SourceHashCoordinator = observedCoordinator
	cfg.ProgressTickIntervalS = 1
	ctx, cancel := context.WithCancel(t.Context())
	walkResult := Walk(ctx, cfg)

	awaitScannerSignal(t, traversalBlocked, "discovery spool observation during traversal")
	now = now.Add(6 * time.Second)
	if got := provider.SourceHashWorkState("alpha"); got.Active != 0 || got.Queued != 1 || got.OldestSchedulingWaitSeconds != 6 {
		t.Fatalf("Source Hash Work state during buffered traversal = %#v, want one waiting for six seconds", got)
	}

	cancel()
	close(traversalRelease)
	for range walkResult.Results {
	}
	if got := provider.SourceHashWorkState("alpha"); got.Active != 0 || got.Queued != 0 || got.OldestSchedulingWaitSeconds != 0 {
		t.Fatalf("Source Hash Work state after traversal cancellation = %#v", got)
	}
}

func TestStreamingWalkSignalsTraversalBeforeHashingCompletes(t *testing.T) {
	testWalkSignalsTraversalBeforeHashingCompletes(t, -1)
}

func TestStreamingWalkRunsDiscoveredFileDuringTraversalWithoutSpool(t *testing.T) {
	underlying := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true")
	if err := fs.WriteFile(underlying, "payload", make([]byte, 128<<10), 0o644); err != nil {
		t.Fatal(err)
	}
	traversalBlocked := make(chan struct{})
	traversalRelease := make(chan struct{})
	readStarted := make(chan struct{})
	readRelease := make(chan struct{})
	before := discoverySpoolSnapshot(t)
	cfg, cancel := testConfig()
	defer cancel()
	cfg.Folder = "streaming-without-spool"
	cfg.Filesystem = &readBarrierFilesystem{
		Filesystem: &walkBarrierFilesystem{
			Filesystem: underlying,
			path:       "payload",
			blocked:    traversalBlocked,
			release:    traversalRelease,
		},
		started: readStarted,
		release: readRelease,
	}
	cfg.Hashers = 1
	cfg.ProgressTickIntervalS = -1
	walkResult := Walk(t.Context(), cfg)

	awaitScannerSignal(t, traversalBlocked, "streaming traversal barrier")
	awaitScannerSignal(t, readStarted, "streaming hashing during traversal")
	assertNoNewDiscoverySpool(t, before)
	close(traversalRelease)
	close(readRelease)
	for range walkResult.Results {
	}
}

func TestWalkWithoutHashingCancellationClosesResultsWhenConsumerStops(t *testing.T) {
	underlying := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true")
	if err := fs.WriteFile(underlying, "payload", []byte("payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg, cfgCancel := testConfig()
	defer cfgCancel()
	cfg.Filesystem = underlying
	ctx, cancel := context.WithCancel(t.Context())
	walkResult := WalkWithoutHashing(ctx, cfg)

	// Traversal completion proves the forwarding routine has accepted the only
	// discovered file and is waiting for a result consumer.
	awaitScannerSignal(t, walkResult.TraversalDone, "traversal completion")
	cancel()
	resultsAfterCancel := 0
	for {
		select {
		case _, ok := <-walkResult.Results:
			if !ok {
				if resultsAfterCancel > 1 {
					t.Fatalf("got %d results after cancellation, want at most the in-flight result", resultsAfterCancel)
				}
				return
			}
			resultsAfterCancel++
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for cancelled scan results to close")
		}
	}
}

func TestEmptyWalkClosesTraversalAndResults(t *testing.T) {
	cfg, cancel := testConfig()
	defer cancel()
	cfg.Filesystem = fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true")
	walkResult := Walk(t.Context(), cfg)

	awaitScannerSignal(t, walkResult.TraversalDone, "empty traversal completion")
	if result, ok := <-walkResult.Results; ok {
		t.Fatalf("empty scan returned a result: %v", result)
	}
}

func TestFailedWalkClosesTraversalAndResults(t *testing.T) {
	cfg, cancel := testConfig()
	defer cancel()
	cfg.Filesystem = fs.NewFilesystem(fs.FilesystemType("error"), ".")
	walkResult := Walk(t.Context(), cfg)
	resultsDone := make(chan struct{})
	go func() {
		for range walkResult.Results {
		}
		close(resultsDone)
	}()

	awaitScannerSignal(t, walkResult.TraversalDone, "failed traversal completion")
	awaitScannerSignal(t, resultsDone, "failed scan result closure")
}

func testWalkSignalsTraversalBeforeHashingCompletes(t *testing.T, progressTickInterval int) {
	t.Helper()
	underlying := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16)+"?content=true")
	if err := fs.WriteFile(underlying, "payload", make([]byte, 128<<10), 0o644); err != nil {
		t.Fatal(err)
	}
	readStarted := make(chan struct{})
	readRelease := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-readRelease:
		default:
			close(readRelease)
		}
	})

	cfg, cancel := testConfig()
	defer cancel()
	cfg.Filesystem = &readBarrierFilesystem{
		Filesystem: underlying,
		started:    readStarted,
		release:    readRelease,
	}
	cfg.Hashers = 1
	cfg.ProgressTickIntervalS = progressTickInterval
	walkResult := Walk(t.Context(), cfg)

	awaitScannerSignal(t, readStarted, "hashing to start")
	awaitScannerSignal(t, walkResult.TraversalDone, "traversal completion")
	select {
	case result, ok := <-walkResult.Results:
		t.Fatalf("scan result arrived while hashing was paused: %v, open=%v", result, ok)
	default:
	}

	close(readRelease)
	var results []ScanResult
	for result := range walkResult.Results {
		results = append(results, result)
	}
	if len(results) != 1 || results[0].Err != nil || results[0].File.Name != "payload" {
		t.Fatalf("scan results = %v, want one completed payload", results)
	}
}

type readBarrierFilesystem struct {
	fs.Filesystem
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type sourceHashStateBarrierFilesystem struct {
	fs.Filesystem
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

type discoverySpoolObservationFilesystem struct {
	fs.Filesystem
	path    string
	spooled <-chan struct{}
	blocked chan struct{}
	release <-chan struct{}
	once    sync.Once
}

type discoverySpoolObservationCoordinator struct {
	SourceHashCoordinator
	spooled chan struct{}
	once    sync.Once
}

type discoverySpoolObservationEpoch struct {
	SourceHashEpoch
	owner *discoverySpoolObservationCoordinator
}

type walkBarrierFilesystem struct {
	fs.Filesystem
	path    string
	blocked chan struct{}
	release chan struct{}
	walkErr error
	once    sync.Once
}

type openErrorFilesystem struct {
	fs.Filesystem
	path string
	err  error
}

type logicalInventoryFilesystem struct {
	fs.Filesystem
	files    int
	fileSize int64
	opens    atomic.Int64
}

func (f *logicalInventoryFilesystem) Walk(_ string, walkFn fs.WalkFunc) error {
	if err := walkFn(".", fakeInfo{name: "."}, nil); err != nil {
		return err
	}
	for i := range f.files {
		name := fmt.Sprintf("file-%08d-%0240d", i, i)
		if err := walkFn(name, fakeInfo{name: name, size: f.fileSize}, nil); err != nil {
			return err
		}
	}
	return nil
}

func (f *logicalInventoryFilesystem) Open(string) (fs.File, error) {
	f.opens.Add(1)
	return nil, errors.New("synthetic source must not open during discovery")
}

func (*logicalInventoryFilesystem) PlatformData(string, bool, bool, fs.XattrFilter) (protocol.PlatformData, error) {
	return protocol.PlatformData{}, nil
}

type inventorySourceHashCoordinator struct {
	enrolled  atomic.Int64
	submitted chan struct{}
}

func (*inventorySourceHashCoordinator) Configure(int, map[string]int) {}

func (*inventorySourceHashCoordinator) BeginSourceHashEpoch(SourceHashFolder) SourceHashEpoch {
	return noopSourceHashEpoch{}
}

func (c *inventorySourceHashCoordinator) Submit(ctx context.Context, request SourceHashRequest) SourceHashSubmission {
	c.enrolled.Add(1)
	c.submitted <- struct{}{}
	admitted := make(chan struct{})
	close(admitted)
	completion := make(chan SourceHashCompletion, 1)
	go func() {
		<-ctx.Done()
		request.Work.Close()
		completion <- SourceHashCompletion{Err: ctx.Err()}
		close(completion)
	}()
	return SourceHashSubmission{Admitted: admitted, Completion: completion}
}

type noopSourceHashEpoch struct{}

func (noopSourceHashEpoch) Close() {}

func (f *openErrorFilesystem) Open(name string) (fs.File, error) {
	if name == f.path {
		return nil, f.err
	}
	return f.Filesystem.Open(name)
}

func (f *walkBarrierFilesystem) Walk(root string, walkFn fs.WalkFunc) error {
	err := f.Filesystem.Walk(root, func(path string, info fs.FileInfo, err error) error {
		walkErr := walkFn(path, info, err)
		if path == f.path {
			f.once.Do(func() {
				close(f.blocked)
				<-f.release
			})
		}
		return walkErr
	})
	if err != nil {
		return err
	}
	return f.walkErr
}

func (f *readBarrierFilesystem) Open(name string) (fs.File, error) {
	file, err := f.Filesystem.Open(name)
	if err != nil {
		return file, err
	}
	return &readBarrierFile{File: file, owner: f}, nil
}

func (f *sourceHashStateBarrierFilesystem) Open(name string) (fs.File, error) {
	file, err := f.Filesystem.Open(name)
	if err != nil {
		return nil, err
	}
	return &sourceHashStateBarrierFile{File: file, owner: f}, nil
}

func (f *discoverySpoolObservationFilesystem) Walk(root string, walkFn fs.WalkFunc) error {
	return f.Filesystem.Walk(root, func(path string, info fs.FileInfo, err error) error {
		walkErr := walkFn(path, info, err)
		if path == f.path {
			f.once.Do(func() {
				<-f.spooled
				close(f.blocked)
				<-f.release
			})
		}
		return walkErr
	})
}

func (c *discoverySpoolObservationCoordinator) BeginSourceHashEpoch(folder SourceHashFolder) SourceHashEpoch {
	return &discoverySpoolObservationEpoch{
		SourceHashEpoch: c.SourceHashCoordinator.BeginSourceHashEpoch(folder),
		owner:           c,
	}
}

func (c *discoverySpoolObservationCoordinator) Submit(ctx context.Context, request SourceHashRequest) SourceHashSubmission {
	if epoch, ok := request.DiscoverySpoolEpoch.(*discoverySpoolObservationEpoch); ok {
		request.DiscoverySpoolEpoch = epoch.SourceHashEpoch
	}
	return c.SourceHashCoordinator.Submit(ctx, request)
}

func (e *discoverySpoolObservationEpoch) SetDiscoverySpoolSourceHashWork(count int) {
	e.SourceHashEpoch.(interface{ SetDiscoverySpoolSourceHashWork(int) }).SetDiscoverySpoolSourceHashWork(count)
	if count > 0 {
		e.owner.once.Do(func() { close(e.owner.spooled) })
	}
}

type sourceHashStateBarrierFile struct {
	fs.File
	owner *sourceHashStateBarrierFilesystem
}

func (f *sourceHashStateBarrierFile) Read(buf []byte) (int, error) {
	f.owner.once.Do(func() {
		close(f.owner.started)
		<-f.owner.release
	})
	return f.File.Read(buf)
}

type readBarrierFile struct {
	fs.File
	owner *readBarrierFilesystem
}

func (f *readBarrierFile) Read(buf []byte) (int, error) {
	f.owner.once.Do(func() {
		close(f.owner.started)
		<-f.owner.release
	})
	return f.File.Read(buf)
}

func awaitScannerSignal(t *testing.T, done <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func discoverySpoolSnapshot(t *testing.T) map[string]struct{} {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(os.TempDir(), "syncthing-discovery-*"))
	if err != nil {
		t.Fatal(err)
	}
	snapshot := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		snapshot[path] = struct{}{}
	}
	return snapshot
}

func newDiscoverySpools(t *testing.T, before map[string]struct{}) []string {
	t.Helper()
	var discovered []string
	for path := range discoverySpoolSnapshot(t) {
		if _, ok := before[path]; !ok {
			discovered = append(discovered, path)
		}
	}
	return discovered
}

func awaitNewDiscoverySpool(t *testing.T, before map[string]struct{}) string {
	t.Helper()
	paths := newDiscoverySpools(t, before)
	if len(paths) != 0 {
		return paths[0]
	}
	t.Fatal("buffered scan did not create a discovery spool")
	return ""
}

func assertNoNewDiscoverySpool(t *testing.T, before map[string]struct{}) {
	t.Helper()
	if paths := newDiscoverySpools(t, before); len(paths) != 0 {
		t.Fatalf("streaming scan created discovery spools %q", paths)
	}
}

func TestIssue4799(t *testing.T) {
	fs := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16))

	fd, err := fs.Create("foo")
	if err != nil {
		t.Fatal(err)
	}
	fd.Close()

	files := walkDir(fs, "/foo", nil, nil, 0)
	if len(files) != 1 || files[0].Name != "foo" {
		t.Error(`Received unexpected file infos when walking "/foo"`, files)
	}
}

func TestRecurseInclude(t *testing.T) {
	stignore := `
	!/dir1/cfile
	!efile
	!ffile
	*
	`
	testFs := newTestFs()
	ignores := ignore.New(testFs)
	if err := ignores.Parse(bytes.NewBufferString(stignore), ".stignore"); err != nil {
		t.Fatal(err)
	}

	files := walkDir(testFs, ".", nil, ignores, 0)

	expected := []string{
		filepath.Join("dir1"),
		filepath.Join("dir1", "cfile"),
		filepath.Join("dir2"),
		filepath.Join("dir2", "dir21"),
		filepath.Join("dir2", "dir21", "dir22"),
		filepath.Join("dir2", "dir21", "dir22", "dir23"),
		filepath.Join("dir2", "dir21", "dir22", "dir23", "efile"),
		filepath.Join("dir2", "dir21", "dir22", "efile"),
		filepath.Join("dir2", "dir21", "dir22", "efile", "efile"),
		filepath.Join("dir2", "dir21", "dira"),
		filepath.Join("dir2", "dir21", "dira", "efile"),
		filepath.Join("dir2", "dir21", "dira", "ffile"),
		filepath.Join("dir2", "dir21", "efile"),
		filepath.Join("dir2", "dir21", "efile", "ign"),
		filepath.Join("dir2", "dir21", "efile", "ign", "efile"),
	}
	if len(files) != len(expected) {
		var filesString []string
		for _, file := range files {
			filesString = append(filesString, file.Name)
		}
		t.Fatalf("Got %d files %v, expected %d files at %v", len(files), filesString, len(expected), expected)
	}
	for i := range files {
		if files[i].Name != expected[i] {
			t.Errorf("Got %v, expected file at %v", files[i], expected[i])
		}
	}
}

func TestIssue4841(t *testing.T) {
	fs := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(16))

	fd, err := fs.Create("foo")
	if err != nil {
		panic(err)
	}
	fd.Close()

	cfg, cancel := testConfig()
	defer cancel()
	cfg.Filesystem = fs
	cfg.AutoNormalize = true
	cfg.CurrentFiler = fakeCurrentFiler{"foo": {
		Name:       "foo",
		Type:       protocol.FileInfoTypeFile,
		LocalFlags: protocol.FlagLocalIgnored,
		Version:    protocol.Vector{}.Update(1),
	}}
	cfg.ShortID = protocol.LocalDeviceID.Short()
	fchan := Walk(context.TODO(), cfg).Results

	var files []protocol.FileInfo
	for f := range fchan {
		if f.Err != nil {
			t.Errorf("Error while scanning %v: %v", f.Err, f.Path)
		}
		files = append(files, f.File)
	}
	slices.SortFunc(fileList(files), compareByName)

	if len(files) != 1 {
		t.Fatalf("Expected 1 file, got %d: %v", len(files), files)
	}
	if expected := (protocol.Vector{}.Update(protocol.LocalDeviceID.Short())); !files[0].Version.Equal(expected) {
		t.Fatalf("Expected Version == %v, got %v", expected, files[0].Version)
	}
}

// TestNotExistingError reproduces https://github.com/syncthing/syncthing/issues/5385
func TestNotExistingError(t *testing.T) {
	sub := "notExisting"
	testFs := newTestFs()
	if _, err := testFs.Lstat(sub); !fs.IsNotExist(err) {
		t.Fatalf("Lstat returned error %v, while nothing should exist there.", err)
	}

	cfg, cancel := testConfig()
	defer cancel()
	cfg.Subs = []string{sub}
	fchan := Walk(context.TODO(), cfg).Results
	for f := range fchan {
		t.Fatalf("Expected no result from scan, got %v", f)
	}
}

func TestSkipIgnoredDirs(t *testing.T) {
	fss := fs.NewFilesystem(fs.FilesystemTypeFake, "")

	name := "foo/ignored"
	err := fss.MkdirAll(name, 0o777)
	if err != nil {
		t.Fatal(err)
	}

	stat, err := fss.Lstat(name)
	if err != nil {
		t.Fatal(err)
	}

	w := &walker{}

	pats := ignore.New(fss)

	stignore := `
	/foo/ign*
	!/f*
	*
	`
	if err := pats.Parse(bytes.NewBufferString(stignore), ".stignore"); err != nil {
		t.Fatal(err)
	}
	if m := pats.Match("whatever"); !m.CanSkipDir() {
		t.Error("CanSkipDir should be true", m)
	}

	w.Matcher = pats

	fn := w.walkAndHashFiles(context.Background(), nil, nil)

	if err := fn(name, stat, nil); err != fs.SkipDir {
		t.Errorf("Expected %v, got %v", fs.SkipDir, err)
	}
}

// https://github.com/syncthing/syncthing/issues/6487
func TestIncludedSubdir(t *testing.T) {
	fss := fs.NewFilesystem(fs.FilesystemTypeFake, "")

	name := filepath.Clean("foo/bar/included")
	err := fss.MkdirAll(name, 0o777)
	if err != nil {
		t.Fatal(err)
	}

	pats := ignore.New(fss)

	stignore := `
	!/foo/bar
	*
	`
	if err := pats.Parse(bytes.NewBufferString(stignore), ".stignore"); err != nil {
		t.Fatal(err)
	}

	fchan := Walk(context.TODO(), Config{
		CurrentFiler: make(fakeCurrentFiler),
		Filesystem:   fss,
		Matcher:      pats,
	}).Results

	found := false
	for f := range fchan {
		if f.Err != nil {
			t.Fatalf("Error while scanning %v: %v", f.Err, f.Path)
		}
		if f.File.IsIgnored() {
			t.Error("File is ignored:", f.File.Name)
		}
		if f.File.Name == name {
			found = true
		}
	}

	if !found {
		t.Errorf("File not present in scan results")
	}
}

// Verify returns nil or an error describing the mismatch between the block
// list and actual reader contents
func verify(r io.Reader, blocksize int, blocks []protocol.BlockInfo) error {
	hf := sha256.New()
	// A 32k buffer is used for copying into the hash function.
	buf := make([]byte, 32<<10)

	for i, block := range blocks {
		lr := &io.LimitedReader{R: r, N: int64(blocksize)}
		_, err := io.CopyBuffer(hf, lr, buf)
		if err != nil {
			return err
		}

		hash := hf.Sum(nil)
		hf.Reset()

		if !bytes.Equal(hash, block.Hash) {
			return fmt.Errorf("hash mismatch %x != %x for block %d", hash, block.Hash, i)
		}
	}

	// We should have reached the end  now
	bs := make([]byte, 1)
	n, err := r.Read(bs)
	if n != 0 || err != io.EOF {
		return errors.New("file continues past end of blocks")
	}

	return nil
}

type fakeCurrentFiler map[string]protocol.FileInfo

func (fcf fakeCurrentFiler) CurrentFile(name string) (protocol.FileInfo, bool) {
	f, ok := fcf[name]
	return f, ok
}

func testConfig() (Config, context.CancelFunc) {
	evLogger := events.NewLogger()
	ctx, cancel := context.WithCancel(context.Background())
	go evLogger.Serve(ctx)
	return Config{
		Filesystem:  newTestFs(),
		Hashers:     2,
		EventLogger: evLogger,
	}, cancel
}

func BenchmarkWalk(b *testing.B) {
	testFs := fs.NewFilesystem(fs.FilesystemTypeFake, rand.String(32))

	for i := range 100 {
		if err := testFs.Mkdir(fmt.Sprintf("dir%d", i), 0o755); err != nil {
			b.Fatal(err)
		}
		for j := range 100 {
			if fd, err := testFs.Create(fmt.Sprintf("dir%d/file%d", i, j)); err != nil {
				b.Fatal(err)
			} else {
				fd.Close()
			}
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		walkDir(testFs, "/", nil, nil, 0)
	}
}
