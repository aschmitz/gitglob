package globpack

import (
  "bytes"
  "crypto/sha256"
  "encoding/binary"
  "errors"
  "fmt"
  "hash"
  "math/rand"
  "os"
  "runtime"
  "time"
  "sync"
  "sync/atomic"
  
  "gitlab.lardbucket.org/aschmitz/gitglob/debugging/flate"
  r "github.com/dancannon/gorethink"
)

const (
  maxDepth = 30 // The maximum length of a delta chain.
  // Accumulate up to 4 MB chunks at a time. We can accumulate more than this,
  // but we'll stop looking for more and just write data at that time.
  writingBlockSize = 1024*1024*4
  storagePath = "output/globpack/" // The location to write globpacks to
  writeChanBuffer = 1024 // The number of writes to buffer at a time
  writerLookupChanBuffer = 1024 // The max number of lookup responses to buffer
  // The maximim size we want to write. If an object larger than this needs to
  // be written, it will be stored, but will be in its own file.
  maxGlobpackSize = 1024*1024*1024
  // The number of goroutines to run for compressing data. If set to 0, it will
  // use runtime.NumCPU() to run as many goroutines as there are CPU cores.
  compressGoroutineCount = 0
)

var rSession *r.Session

type globpackWriteData struct {
  Hash [hashLen]byte  // The hash of the object to be inserted
  GlobpackData []byte // The data that should be written to the globpack
  // A channel to write to for indicating the write is done
  AckChan chan<- *globpackObjLoc
}

type globpackWriteRecord struct {
  Ack *globpackObjLoc
  Channel chan<- *globpackObjLoc
}

type globpackWriteRequest struct {
  CanClearUncompressed bool // Can we clear the uncompressed data after writing?
  Object *gitObject // The gitObject to write
  // A channel to write to for indicating the write is done
  AckChan chan<- *globpackObjLoc
}

type globpack struct {
  Filename string // The filename of this globpack
  Length uint64   // The length of the globpack (so far)
  File *os.File   // The actual file object
  Hash hash.Hash  // The checksum generator for this file
  ObjCount int    // The number of objects in this file
}

var haveCurrentGlobpack = false
var currentGlobpack *globpack
var compressChan chan *globpackWriteRequest
var writeChan chan *globpackWriteData
var writerLookupChan chan *globpackObjLoc
var writerMutex sync.Mutex

type globpackWriterStatsType struct {
  // In order to keep Written and Deltas (closely) in sync, we don't actually
  // increment Written when writing an object, but when determining the data
  // needed to actually put it in a globpack. This should happen very shortly
  // before actually writing it to disk, but is not synonymous with that.
  Written uint64 // Number of objects written (deltas and full objects)
  Deltas uint64  // Number of deltas written
  
  // Note that neither "Size" includes the size of the globpack object's header
  UncompressedSize uint64 // Total size of objects written, before compression
  CompressedSize uint64   // Total size of objects written, after compression
}
var globpackWriterStats *globpackWriterStatsType

// Take incoming compression requests, compress them, and add them to the
// writing queue.
func handleCompression() {
  // Level 6 (the zlib default) seems to be a good tradeoff between size and
  // compression time. Some unscientific samples:
  // 
  // Seconds Size      Level
  // 270.71  207487903 1
  // 274.92  202771936 2
  // 275.30  199366288 3
  // 277.31  193367575 4
  // 285.58  189670000 5
  // 296.31  188443152 6
  // 304.79  188108040 7
  // 320.13  187908348 8
  // 326.75  187894599 9
  compressWriter, _ := flate.NewWriter(nil, 6)
  
  for {
    writeReq := <- compressChan
    obj := writeReq.Object
    
    // The globpack format for non-delta objects is as follows:
    // bytes      | description
    // 0-19       | Commit hash
    // 20         | Object type
    // 21-(20+N)  | Varint object stored data length
    // (21+N)-end | Stored object data
    
    // The globpack format for delta objects is as follows:
    // bytes      | description
    // 0-19       | Commit hash
    // 20         | Object type
    // 21-40      | Base object hash
    // 41-(20+N)  | Varint object stored data length
    // (21+N)-end | Stored object delta
    
    // Start with the object hash
    buf := obj.Hash[:]
    
    // Get the type of this object
    typeByte := byte(obj.Type)
    var toWrite []byte
    var toWriteType int
    
    // Write the right type byte.
    atomic.AddUint64(&globpackWriterStats.Written, 1)
    if (obj.Delta == nil) || (obj.Depth % maxDepth == 0) {
      // We want to write a full version of this object: we didn't get a diff,
      // or this is at our maximum depth.
      toWrite = obj.Data
      toWriteType = objCompressedFull
    } else {
      // This is a diff. Write the correct flag.
      typeByte |= 0x8
      toWrite = obj.Delta
      toWriteType = objCompressedDelta
      atomic.AddUint64(&globpackWriterStats.Deltas, 1)
    }
    
    // We now know what we want to write. Can we clear this from the object?
    if writeReq.CanClearUncompressed &&
      obj.CompressedType != objCompressedNone {
      obj.Delta = nil
      obj.Data = nil
    }
    
    atomic.AddUint64(&globpackWriterStats.UncompressedSize,
      uint64(len(toWrite)))
    
    writingCompressed := false
    
    // Do we already have a compressed copy from the gitpack?
    if toWriteType == obj.CompressedType {
      // We do. Did we like the compression level?
      if obj.CompressedLevel >= zlibDefaultLevel {
        // Yep. Use that then, assuming there were any savings.
        if len(obj.CompressedData) < len(toWrite) {
          toWrite = obj.CompressedData
          writingCompressed = true
        }
      }
    }
    
    // Compress and see if it get smaller. However, only try compressing if the
    // object is at least 34 bytes long. A test of 180k objects showed minimal
    // savings on smaller objects (under 0.1% of cases saved any bytes, all
    // under 10 bytes saved).
    if (!writingCompressed) && (len(toWrite) >= 34) {
      var compressBuf bytes.Buffer
      compressWriter.Reset(&compressBuf)
      compressWriter.Write(toWrite)
      compressWriter.Close()
      
      if compressBuf.Len() < len(toWrite) {
        toWrite = compressBuf.Bytes()
        writingCompressed = true
      }
    }
    
    if writingCompressed {
      typeByte |= 0x10 // Mark the object as compressed.
    }
    
    atomic.AddUint64(&globpackWriterStats.CompressedSize, uint64(len(toWrite)))
    
    // Add the type
    buf = append(buf, typeByte)
    
    // If necessary, add the referenced hash
    if typeByte & 0x8 > 0 {
      buf = append(buf, obj.Refs[:]...)
    }
    
    // Add the length
    lenBytes := make([]byte, binary.MaxVarintLen64)
    lenLen := binary.PutUvarint(lenBytes, uint64(len(toWrite)))
    buf = append(buf, lenBytes[0:lenLen]...)
    
    // Finally, add (and return) the object data
    buf = append(buf, toWrite...)
    writeData := globpackWriteData {
      Hash: writeReq.Object.Hash,
      GlobpackData: buf,
      AckChan: writeReq.AckChan,
    }
    writeChan <- &writeData
  }
}

func rotateGlobpackRegularly() {
  for {
    // Vaguely based on http://stackoverflow.com/a/19549474
    now := time.Now().UTC()
    nextTime := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0,
      0, time.UTC).Add(time.Hour)
    diffTime := nextTime.Sub(now)
    
    // Make sure we're actually waiting a bit: leap seconds, clock drift, etc.
    // are annoying.
    if diffTime < time.Minute {
      // Add produces a Time, not a Duration, so we do this instead.
      diffTime = nextTime.Add(time.Hour).Sub(now)
    }
    
    <- time.After(diffTime)
    
    // Finally, we should be at the top of an hour. Rotate.
    rotateGlobpack()
  }
}

func init() {
  var err error
  if rSession == nil {
    rSession, err = r.Connect(r.ConnectOpts{
      Address: "localhost:28015",
      MaxIdle: 100,
      MaxOpen: 100,
    })
    if err != nil {
      panic(err.Error())
    }
  }
}

func globpackWriterStatsReporter() {
  oldWritten := uint64(0)
  oldDeltas := uint64(0)
  oldUncompressedSize := uint64(0)
  oldCompressedSize := uint64(0)
  for {
    time.Sleep(time.Minute) // Every minute,
    
    // Read the new values and calculate what to store
    curWritten := atomic.LoadUint64(&globpackWriterStats.Written)
    deltaWritten := curWritten - oldWritten
    oldWritten = curWritten
    curDeltas := atomic.LoadUint64(&globpackWriterStats.Deltas)
    deltaDeltas := curDeltas - oldDeltas
    oldDeltas = curDeltas
    curUncompressedSize :=
      atomic.LoadUint64(&globpackWriterStats.UncompressedSize)
    deltaUncompressedSize := curUncompressedSize - oldUncompressedSize
    oldUncompressedSize = curUncompressedSize
    curCompressedSize := atomic.LoadUint64(&globpackWriterStats.CompressedSize)
    deltaCompressedSize := curCompressedSize - oldCompressedSize
    oldCompressedSize = curCompressedSize
    
    // Then write the stats to InfluxDB.
    influxWritePoint("globpack_writer", map[string]string{},
      map[string]interface{}{
        "written": int(deltaWritten),
        "deltas": int(deltaDeltas),
        "uncompressed_size": int(deltaUncompressedSize),
        "compressed_size": int(deltaCompressedSize),
      })
  }
}

func initGlobpackWriter() error {
  var err error
  if haveCurrentGlobpack == false {
    currentGlobpack, err = initGlobpack(); if err != nil {
      return err
    }
  
    globpackWriterStats = &globpackWriterStatsType{}
    go globpackWriterStatsReporter()
    
    go rotateGlobpackRegularly()
    
    haveCurrentGlobpack = true
  }
  
  if writeChan == nil {
    writeChan = make(chan *globpackWriteData, writeChanBuffer)
    writerLookupChan = make(chan *globpackObjLoc, writerLookupChanBuffer)
    compressChan = make(chan *globpackWriteRequest, writerLookupChanBuffer)
    go objectExistenceHandler()
    go writer((<-chan *globpackWriteData)(writeChan))
    
    realCompressGoroutineCount := compressGoroutineCount
    if realCompressGoroutineCount == 0 {
      realCompressGoroutineCount = runtime.NumCPU()
    }
    for i := 0; i < realCompressGoroutineCount; i++ {
      go handleCompression()
fmt.Printf("Started compression goroutine %d.\n", i)
    }
  }
  
  return nil
}

func CloseGlobpackWriter() error {
  if haveCurrentGlobpack {
    haveCurrentGlobpack = false
    if err := closeGlobpack(currentGlobpack); err != nil {
      return err
    }
  }
  
  return nil
}

func initGlobpack() (*globpack, error) {
  pack := new(globpack)
  errorCount := 0
  for {
    packTime := time.Now().UTC()
    pack.Filename = "gitglob_"+packTime.Format("20060102150405")+"_"+
      fmt.Sprintf("%03d", rand.Intn(1000))+".globpack"
    
    packDir, err := GlobpackNameToDirectory(pack.Filename); if err != nil {
      return nil, err
    }
    if err := os.MkdirAll(packDir, 0755); err != nil {
      return nil, errors.New("Unable to create globpack folder.")
    }
    
    packFullPath, err := GlobpackNameToFullPath(pack.Filename); if err != nil {
      return nil, err
    }
    
    fmt.Println(packFullPath)
    
    // See if the file exists already for some reason.
    if _, err := os.Stat(packFullPath); os.IsNotExist(err) {
      // The file doesn't exist, so we'll use it.
      pack.File, err = os.OpenFile(packFullPath,
        os.O_WRONLY | os.O_CREATE | os.O_EXCL, 0644)
      
      if err == nil {
        break
      } else {
        errorCount += 1
        if errorCount >= 3 {
          return nil, errors.New("Unable to create a globpack on disk.")
        }
      }
    }
  }
  pack.Length = 0
  pack.ObjCount = 0
  pack.Hash = sha256.New()
  
  // Globpack header:
  // bytes | description
  // 0-7   | Static header "gpak\x00\x0d\x0a\xa5"
  // 8-11  | Version number "\x00\x00\x00\x01"
  // 12-19 | File length, network byte order
  // 20-51 | Checksum
  // In calculating the hash, the file length should be assumed to be 2^64-1,
  // and the checksum field used in calculating the checksum should contain all
  // "\x00" bytes.
  headerBytes := []byte("gpak\x00\x0d\x0a\xa5"+ // Static header
    "\x00\x00\x00\x01"+                         // Version number
    "\xff\xff\xff\xff\xff\xff\xff\xff"+         // File length (2^64-1 for now)
    "\x00\x00\x00\x00\x00\x00\x00\x00"+         // Checksum (blank)
    "\x00\x00\x00\x00\x00\x00\x00\x00"+
    "\x00\x00\x00\x00\x00\x00\x00\x00"+
    "\x00\x00\x00\x00\x00\x00\x00\x00")
  writeLen, err := pack.File.Write(headerBytes); if err != nil {
    return nil, err
  }
  if writeLen != len(headerBytes) {
    return nil, errors.New("Unable to write full globpack header.")
  }
  writeLen, err = pack.Hash.Write(headerBytes); if err != nil {
    return nil, err
  }
  if writeLen != len(headerBytes) {
    return nil, errors.New("Unable to write globpack header to hash.")
  }
  pack.Length = uint64(len(headerBytes))
  
  fmt.Println(pack.Filename)
  
  return pack, nil
}

func closeGlobpack(pack *globpack) error {
fmt.Printf("closeGlobpack() called for %s\n", pack.Filename)
  // Determine whether we wrote any data.
  if pack.ObjCount == 0 {
    // We didn't write anything, so we can just delete the file and move on.
    pack.File.Close()
    fullPath, err := GlobpackNameToFullPath(pack.Filename); if err != nil {
      return err
    }
    os.Remove(fullPath)
    return nil
  }
  
  // Update the packfile header. File length begins at offset 12.
  if _, err := pack.File.Seek(12, os.SEEK_SET); err != nil {
    return err
  }
  
  // Write the file's length
  if err := binary.Write(pack.File, binary.BigEndian, pack.Length); err != nil {
    return err
  }
  
  // Write the file's checksum
  writeLen, err := pack.File.Write(pack.Hash.Sum(nil)); if err != nil {
    return err
  }
  if writeLen != sha256.Size {
    return errors.New("Unable to write full hash to file.")
  }
  
  // Close the file
  pack.File.Close()
  
  return nil
}

func rotateGlobpack() error {
  writerMutex.Lock()
  // We're not going to defer the unlock for this, because there is no safe way
  // to recover from a failure here: failure almost certainly means we don't
  // have a safely open globpack, which is not a state we want to write in.
  
  closeGlobpack(currentGlobpack)
  // We declare err here, because := for initGlobpack() would [probably?] use a
  // local currentGlobpack, not the module one we use.
  var err error
  currentGlobpack, err = initGlobpack(); if err != nil {
    return err
  }
  
  writerMutex.Unlock()
  return nil
}

// Actually write out the buffer from writer() to the current globpack.
func drainWriterBuf(buf []byte, pendingAcks []*globpackWriteRecord) {
  writerMutex.Lock()
  // Again, we don't defer the unlock, because there's no safe recovery from a
  // failure here.
  
  if haveCurrentGlobpack == false {
    panic("no currently open globpack")
  }
  writeLen, err := currentGlobpack.File.Write(buf)
  if err != nil {
    panic(err)
  }
  if writeLen != len(buf) {
    panic(errors.New("unable to write full data to globpack"))
  }
  writeLen, err = currentGlobpack.Hash.Write(buf); if err != nil {
    panic(err)
  }
  if writeLen != len(buf) {
    panic(errors.New("unable to write full data to globpack hash"))
  }
  currentGlobpack.File.Sync()
  writerMutex.Unlock()
  
  // Insert these into the database before we alert the process that intended
  // to write them. This should give the effect that reading an object is
  // possible immediately after it as been acknowledged
  
  // Get a list of objects to write. This allows sending all the inserts at
  // once.
  var insertObjects []map[string]interface{}
  for _, pendingAck := range pendingAcks {
    insertObjects = append(insertObjects, map[string]interface{}{
        "id": pendingAck.Ack.Hash[:],
        "file": pendingAck.Ack.Filename,
        "loc": pendingAck.Ack.Position,
      })
  }
  
  // Actually do the writing to the database.
  res, err := r.Db("gitglob").Table("objects").Insert(insertObjects).
    RunWrite(rSession)
  if err != nil {
    fmt.Println(err.Error())
  }
  fmt.Printf("Inserted %d\n", res.Inserted)
  
  // Notify the writers that their requests have been written.
  for _, pendingAck := range pendingAcks {
    pendingAck.Channel <- pendingAck.Ack
  }
}

func WriteObject(writeReq *globpackWriteRequest) {
  lookupChan <- &globpackLookupRequest{
    Hash: writeReq.Object.Hash,
    ResChan: writerLookupChan,
    Context: writeReq,
  }
}

func objectExistenceHandler() {
  for {
    lookupResp := <- writerLookupChan
    writeReq := lookupResp.Context.(*globpackWriteRequest)
    if lookupResp.Existed {
      lookupResp.Context = nil
      writeReq.AckChan <- lookupResp
    } else {
      compressChan <- writeReq
    }
  }
}

// Handle incoming write requests, turn them into the globpack format, and
// prepare them to be written to the globpack file.
func writer(writes <-chan *globpackWriteData) {
  var err error
  
  // This function's "accumulate until a buffer is filled or there is nothing to
  // write immediately" mechanism is based on a post from "rog" to golang-nuts
  // on August 21, 2012:
  // https://groups.google.com/d/msg/golang-nuts/W2WmiBlfKDk/WD_DJr3kQzUJ
  
  fmt.Println("Writer starting.")
  var buf []byte
  var pendingAcks []*globpackWriteRecord
  for {
    writeData := <-writes
    objBuf := writeData.GlobpackData
    
    // Does this spill into a new file? Note that for the first object in a
    // writable block, we also check to make sure this isn't the first object in
    // the file. (If it is, we'll have to write it anyway: the file can't get
    // any smaller.)
    if (currentGlobpack.Length + uint64(len(objBuf)) > maxGlobpackSize) &&
      (currentGlobpack.ObjCount > 0) {
      // Rotate globpacks.
      drainWriterBuf(buf, pendingAcks)
      buf = buf[:0]
      pendingAcks = pendingAcks[:0]
      err = rotateGlobpack()
      if err != nil {
        panic(err)
      }
    }
    
    buf = objBuf
    ack := globpackObjLoc{
      Filename: currentGlobpack.Filename,
      Position: currentGlobpack.Length,
      Hash: writeData.Hash,
    }
    currentGlobpack.Length += uint64(len(objBuf))
    currentGlobpack.ObjCount += 1
    pendingAcks = append(pendingAcks, &globpackWriteRecord{
      Channel: writeData.AckChan,
      Ack: &ack,
    })
    
    // Accumulate data from any goroutines that are ready to send it (up to a
    // limit), so that we use fewer writes and wait for Sync() less.
  drain:
    for len(buf) < writingBlockSize {
      select {
      case writeData = <-writes:
        objBuf = writeData.GlobpackData
        
        // Does this spill into a new file? Here, we don't check to see if this
        // is the first object in a globfile, as it can't be: we just read at
        // least one object to start this buffer.
        if currentGlobpack.Length + uint64(len(objBuf)) > maxGlobpackSize {
          // Rotate globpacks.
          drainWriterBuf(buf, pendingAcks)
          buf = buf[:0]
          pendingAcks = pendingAcks[:0]
          err = rotateGlobpack()
          if err != nil {
            panic(err)
          }
        }
        
        buf = append(buf, objBuf...)
        ack := globpackObjLoc{
          Filename: currentGlobpack.Filename,
          Position: currentGlobpack.Length,
          Hash: writeData.Hash,
        }
        currentGlobpack.Length += uint64(len(objBuf))
        currentGlobpack.ObjCount += 1
        pendingAcks = append(pendingAcks, &globpackWriteRecord{
          Channel: writeData.AckChan,
          Ack: &ack,
        })
      default:
        break drain
      }
    }
    
    drainWriterBuf(buf, pendingAcks)
    buf = buf[:0]
    pendingAcks = pendingAcks[:0]
  }
}
