package globpack

import (
  "bytes"
  "github.com/aschmitz/gitglob/debugging/flate"
  "github.com/aschmitz/gitglob/debugging/zlib"
  "crypto/sha1"
  "encoding/binary"
// "encoding/hex"
  "errors"
  "fmt"
  "io"
  "io/ioutil"
  "net/url"
  "time"
  "sync/atomic"
  
  "github.com/aschmitz/gitglob/mmap"
  influxdb "github.com/influxdb/influxdb/client"
)

const (
  hashLen = sha1.Size // The length of an object hash
)

const (
  gitTypeCommit      = 1
  gitTypeTree        = 2
  gitTypeBlob        = 3
  gitTypeTag         = 4
  gitTypeOffsetDelta = 6
  gitTypeRefDelta    = 7
  
  objCompressedNone  = 0
  objCompressedFull  = 1
  objCompressedDelta = 2
  
  zlibFastestLevel = 0
  zlibFastLevel    = 1
  zlibDefaultLevel = 2
  zlibBestLevel    = 3
)

type gitObject struct {
  Type int           // The git ID for the object type
  Hash [hashLen]byte // Object hash, may be nil
  Data []byte        // The full object data, may be nil
  Delta []byte       // A delta, if any
  Refs [hashLen]byte // The object ID that this delta references
  Depth int          // The number of objects "below" this delta
  CompressedData []byte // A slice of the *deflated* data for this object
  CompressedType int // The type of compressed data stored
  CompressedLevel int // The compression level claimed for the compressed data
  GitpackSize int    // TODO: For debugging, should probably be removed
  // The number of outstanding locks on the object's decompressed data
  DecompressedLockCount uint32
}

type gitPackfile struct {
  // A copy of the packfile itself.
  Data []byte
  // A reader for the packfile data.
  Reader io.ReadSeeker
  // File positions of objects which are not deltas.
  BaseObjects []*gitObject
  // Locations of objects which are deltas, indexed by their base hash.
  DescendedFrom map[[hashLen]byte][]*gitObject
}

type GlobpackObjLoc struct {
  Filename string     // The filename this object was written to
  Filenum uint64      // The number from the file this object was written to
  Position uint64     // The byte in the file this object starts on
  Hash [hashLen]byte  // Object hash, may be nil
  Existed bool        // Did the object already exist?
  Context interface{} // Extra data from the request
}

type gitpackReaderStatsType struct {
  Objects uint64 // Number of objects read from gitpacks
  Deltas uint64  // Number of delta objects read from globpacks
}
var gitpackReaderStats *gitpackReaderStatsType

var BadPackfileChecksumError = errors.New("bad packfile checksum")
var CouldntResolveExternalDeltasError =
  errors.New("couldn't resolve external deltas")

var influxdbClient *influxdb.Client

func init() {
  var err error
  influxUrl, err := url.Parse("http://localhost:8086"); if err != nil {
    panic(err.Error())
  }
  influxdbClient, err = influxdb.NewClient(influxdb.Config{
      URL: *influxUrl,
  }); if err != nil {
    panic(err.Error())
  }
  
  gitpackReaderStats = &gitpackReaderStatsType{}
  go gitpackReaderStatsReporter()
}

func influxWritePoint(measurement string, tags map[string]string,
  fields map[string]interface{}) {
  point := influxdb.Point{
    Measurement: measurement,
    Tags: tags,
    Fields: fields,
    Time: time.Now(),
    Precision: "s",
  }

  bps := influxdb.BatchPoints{
    Points:   []influxdb.Point{point},
    Database: "gitglob",
  }
  _, err := influxdbClient.Write(bps); if err != nil {
    if err.Error() == "timeout" {
      // Try once more
      _, err = influxdbClient.Write(bps)
    }
    
    if (err != nil) && (err.Error() == "timeout") {
      // Ignore this: we shouldn't have timed out, but it's better to keep going
      // than to die here.
    } else {
      // panic(err.Error())
    }
  }
}

func gitpackReaderStatsReporter() {
  oldObjects := uint64(0)
  oldDeltas := uint64(0)
  for {
    time.Sleep(15 * time.Second) // Every 15 seconds,
    
    // Read the new values and calculate what to store
    curObjects := atomic.LoadUint64(&gitpackReaderStats.Objects)
    deltaObjects := curObjects - oldObjects
    oldObjects = curObjects
    curDeltas := atomic.LoadUint64(&gitpackReaderStats.Deltas)
    deltaDeltas := curDeltas - oldDeltas
    oldDeltas = curDeltas
    
    // Then write the stats to InfluxDB.
    influxWritePoint("gitpack_reader", map[string]string{},
      map[string]interface{}{
        "objects": int(deltaObjects),
        "deltas": int(deltaDeltas),
      })
  }
}

func GlobpackNameToDirectory(name string) (string, error) {
  if len(name) != len("gitglob_YYYYMMDDHHIISS123.globpack") {
    return "", errors.New("Incorrect globpack name length.")
  }
  
  return storagePath + "/" + name[8:8+6], nil
}
func GlobpackNameToFullPath(name string) (string, error) {
  dir, err := GlobpackNameToDirectory(name); if err != nil {
    return "", err
  }
  
  return dir + "/" + name, nil
}

// Read from a packfile, return the number of objects in it.
func ReadPackfileHeader(reader io.Reader) (int, error) {
  magic := make([]byte, 4)
  bytesRead, err := reader.Read(magic); if err != nil {
    return 0, err
  }
  if bytesRead != 4 {
    return 0, errors.New("Unable to read full magic header.")
  }
  
  if !bytes.Equal(magic, []byte("PACK")) {
    return 0, errors.New("Unexpected packfile header: "+string(magic))
  }
  
  versionBytes := make([]byte, 4)
  bytesRead, err = reader.Read(versionBytes); if err != nil {
    return 0, err
  }
  if bytesRead != 4 {
    return 0, errors.New("Unable to read full version header")
  }
  packfileVersion := binary.BigEndian.Uint32(versionBytes)
  if packfileVersion != 2 {
    return 0, errors.New(fmt.Sprintf("Unexpected packfile version: %d",
      packfileVersion))
  }
  
  numObjectsBytes := make([]byte, 4)
  bytesRead, err = reader.Read(numObjectsBytes); if err != nil {
    return 0, err
  }
  if bytesRead != 4 {
    return 0, errors.New("Unable to read full number of objects header")
  }
  numObjects := binary.BigEndian.Uint32(numObjectsBytes)
  
  return int(numObjects), nil
}

func VerifyPackfileChecksum(packfile []byte) error {
  if len(packfile) < hashLen {
    return BadPackfileChecksumError
  }
  
  correctChecksum := sha1.Sum(packfile[0:len(packfile)-hashLen])
  if !bytes.Equal(packfile[len(packfile)-hashLen:len(packfile)],
    correctChecksum[:]) {
    influxWritePoint("errors", map[string]string{
      "class": "gitpack",
      "name": "checksum",
    }, map[string]interface{}{
      "value": 1,
    })
    return BadPackfileChecksumError
  }
  
  return nil
}

func ReadPackfileObject(packfile *gitPackfile) (*gitObject, error) {
  reader := packfile.Reader
startLoc, _ := reader.Seek(0, 1)
  // First, read the object type and *inflated* size.
  readByte := make([]byte, 1);
  _, err := reader.Read(readByte); if err != nil {
    return nil, err
  }
  sizeByte := readByte[0]
  
  // For the first byte, the MSB is a continuation bit, the next three bits
  //  identify the object type, and the remaining four bits are the least
  //  significant bits of the size.
  // For the continuation bytes, the MSB is a continuation bit, and the
  //  remaining seven bits are the next least significant bits of the size.
  obj := new(gitObject)
  obj.Type = int((sizeByte & 0x70) >> 4)
  size := uint(sizeByte & 0x0f)
  shift := uint(4)
  for (sizeByte & 0x80) > 0 {
    _, err = reader.Read(readByte); if err != nil {
      return nil, err
    }
    sizeByte = readByte[0]
    
    size += uint(sizeByte & 0x7f) << shift
    shift += 7
  }
  
  // Is this a reference to another object?
  // Note that we currently don't support gitTypeOffsetDelta.
  var isDelta = false
  if obj.Type == gitTypeRefDelta {
    isDelta = true
    objRef := make([]byte, hashLen)
    sizeRead, err := reader.Read(objRef); if err != nil {
      return nil, err
    }
    if sizeRead != hashLen {
      return nil, errors.New("Unable to read full referenced object ID")
    }
    copy(obj.Refs[:hashLen], objRef)
  }
  
  // Now make a reader for the actual data.
  zlibDataStart, _ := reader.Seek(0, 1)
  decompressedReader, err := zlib.NewReader(reader); if err != nil {
    return nil, err
  }
  defer decompressedReader.Close()
  
  // Read out the data from this object.
  decompressedData, err := ioutil.ReadAll(decompressedReader); if err != nil {
    return nil, err
  }
  if len(decompressedData) != int(size) {
    return nil, errors.New("Incorrect size read from object.")
  }
  zlibDataEnd, _ := reader.Seek(0, 1)
  
  // Put the data in the right field.
  if isDelta {
    obj.Delta = decompressedData
    obj.Depth = -1
    obj.CompressedType = objCompressedDelta
    atomic.AddUint64(&gitpackReaderStats.Deltas, 1)
  } else {
    obj.Data = decompressedData
    obj.Depth = 0
    obj.CompressedType = objCompressedFull
  }
  
  // Extract the compressed data to reference it as well.
  // First, check the compression method to make sure it's deflate (it will be,
  // but it's a good idea to verify this).
  if packfile.Data[zlibDataStart] & 0xf == 0x8 {
    // Make sure there's no dictionary used.
    if packfile.Data[zlibDataStart+1] & 0x20 == 0 {
      // Read out the compression level
      obj.CompressedLevel = int(packfile.Data[zlibDataStart+1] >> 6)
      // Reference the deflated data: skip the first two bytes (zlib flag bytes)
      // and the last four bytes (zlib ADLER32 checksum)
      obj.CompressedData = packfile.Data[zlibDataStart+2:zlibDataEnd-4]
    } else {
      // A dictionary was used, which we don't support. This shouldn't ever
      // happen either, but just in case, we'll throw an error
      return nil, errors.New("unexpected compression dictionary usage")
    }
  } else {
    return nil, errors.New("unexpected non-DEFLATE zlib")
  }

// if obj.CompressedType == objCompressedFull {
//   obj.AddHash()
//   if obj.Hash == [0x3b, 0xed, 0x8f, 0x6a, 0x62, 0x34, 0xfc, 0x1b, 0x32, 0x23, 0xdb, 0x5f, 0xd2, 0xfb, 0xcc, 0x53, 0x70, 0xb3, 0x1b, 0x1f] {
//     fmt.Printf("Found object %40x. Surrounding packfile data:\n%s", obj.Hash, hex.Dump(packfile.Data[zlibDataStart]))
//   }
// }
  
  atomic.AddUint64(&gitpackReaderStats.Objects, 1)
  
endLoc, _ := reader.Seek(0, 1)
obj.GitpackSize = int(endLoc - startLoc)

  return obj, nil
}

func (obj *gitObject) HasDelta() bool {
  return (obj.Delta != nil) || (obj.CompressedType == objCompressedDelta)
}

func (obj *gitObject) DecompressIfNecessary() error {
  // Do we even need to decompress anything?
  switch obj.CompressedType {
    case objCompressedDelta: if obj.Delta != nil { return nil }
    case objCompressedFull: if obj.Data != nil { return nil }
    case objCompressedNone: return nil
    default: return errors.New("unexpected unknown compressed data type")
  }
  
  // Decompress the data
// fmt.Printf("Reading deflated data from %40x (%d bytes):\n%s", obj.Hash, len(obj.CompressedData), hex.Dump(obj.CompressedData))
  decompressedReader := flate.NewReader(bytes.NewReader(obj.CompressedData))
  decompressedData, err := ioutil.ReadAll(decompressedReader)
  if err != nil {
    return err
  }
  
  // Store it in the right place
  switch obj.CompressedType {
    case objCompressedDelta: obj.Delta = decompressedData
    case objCompressedFull: obj.Data = decompressedData
  }
  
  return nil
}

func (obj *gitObject) LockDecompressedData() {
  atomic.AddUint32(&obj.DecompressedLockCount, 1)
}

func (obj *gitObject) UnlockDecompressedData() {
  // From the go atomic documentation:
  // "In particular, to decrement x, do AddUint32(&x, ^uint32(0))."
  remaining := atomic.AddUint32(&obj.DecompressedLockCount, ^uint32(0))
  
  // Can we clear the decompressed data?
  if remaining == 0 && obj.CompressedType != objCompressedNone {
    obj.Delta = nil
    obj.Data = nil
  }
}

func GetObjectTypeString(objType int) string {
  switch objType {
    case gitTypeCommit: return "commit"
    case gitTypeTree: return "tree"
    case gitTypeBlob: return "blob"
    case gitTypeTag: return "tag"
    default: return "unknown"
  }
}

func (obj *gitObject) AddHash() {
  hash := sha1.New()
  fmt.Fprintf(hash, "%s %d\x00", GetObjectTypeString(obj.Type), len(obj.Data))
  hash.Write(obj.Data)
  fullHash := hash.Sum(nil)
  copy(obj.Hash[:hashLen], fullHash)
}

func ApplyDelta(sourceObj *gitObject, destObj *gitObject) error {
  source := sourceObj.Data
  deltaReader := bytes.NewReader(destObj.Delta)
  sourceLen, err := binary.ReadUvarint(deltaReader); if err != nil {
    return err
  }
  destLen, err := binary.ReadUvarint(deltaReader); if err != nil {
    return err
  }
  
  if uint64(len(source)) != sourceLen {
    fmt.Printf("Source object: %+v\n", sourceObj)
    return errors.New(fmt.Sprintf("Unexpected mismatch in delta source length."+
      " Expected %d, got %d.", len(source), sourceLen))
  }
  
  dest := new(bytes.Buffer)
  
  readByte := make([]byte, 1);
  _, err = deltaReader.Read(readByte); if err != nil {
    return err
  }
  operationByte := readByte[0]
  
  for {
    if (operationByte & 0x80) == 0 {
      if operationByte == 0 {
        // This is reserved.
        return errors.New("Unexpected zero operation byte in delta.")
      }
      // No MSB set: this is an insert command. Insert this many bytes from the
      //  delta stream into the output.
      insertData := make([]byte, operationByte)
      if _, err := deltaReader.Read(insertData); err != nil {
        return err
      }
      
      dest.Write(insertData)
    } else {
      // MSB set: this is a copy command. These are a bit strange.
      // Bottom four bits: If the bottom bit is set, the next byte is the low
      //  byte for the offset to read from. If 0x2 is set, the next byte is the
      //  second-lowest byte for the offset to read from, and so on.
      // Similarly, the remaining top three bits are indicators of the bytes for
      //  the value to read from. It's a bit confusing, slightly easier in code:
      copyOff := uint(0)
      copyLen := uint(0)
      if (operationByte & 0x1) > 0 {
        _, err := deltaReader.Read(readByte); if err != nil {
          return err
        }
        copyOff = uint(readByte[0])
      }
      if (operationByte & 0x2) > 0 {
        _, err := deltaReader.Read(readByte); if err != nil {
          return err
        }
        copyOff += uint(readByte[0]) << 8
      }
      if (operationByte & 0x4) > 0 {
        _, err := deltaReader.Read(readByte); if err != nil {
          return err
        }
        copyOff += uint(readByte[0]) << 16
      }
      if (operationByte & 0x8) > 0 {
        _, err := deltaReader.Read(readByte); if err != nil {
          return err
        }
        copyOff += uint(readByte[0]) << 24
      }
      
      
      if (operationByte & 0x10) > 0 {
        _, err := deltaReader.Read(readByte); if err != nil {
          return err
        }
        copyLen = uint(readByte[0])
      }
      if (operationByte & 0x20) > 0 {
        _, err := deltaReader.Read(readByte); if err != nil {
          return err
        }
        copyLen += uint(readByte[0]) << 8
      }
      if (operationByte & 0x40) > 0 {
        _, err := deltaReader.Read(readByte); if err != nil {
          return err
        }
        copyLen += uint(readByte[0]) << 16
      }
      if copyLen == 0 {
        copyLen = 0x10000
      }
      
      dest.Write(source[copyOff:(copyOff + copyLen)])
    }
    
    _, err = deltaReader.Read(readByte); if err != nil {
      if err == io.EOF {
        break
      }
      
      return err
    }
    operationByte = readByte[0]
  }
  
  resultBytes := dest.Bytes()
  
  if uint64(len(resultBytes)) != destLen {
    return errors.New("Incorrect result length when applying delta.")
  }
  
  destObj.Data = resultBytes
  destObj.Type = sourceObj.Type
  destObj.Depth = sourceObj.Depth + 1
  destObj.AddHash()
  
  return nil
}

func resolvePackfileObjectsFromBase(packfile *gitPackfile, base *gitObject,
  ackChan chan *GlobpackObjLoc) error {
  for _, obj := range packfile.DescendedFrom[base.Hash] {
    obj.DecompressIfNecessary()
    
    if err := ApplyDelta(base, obj); err != nil {
      return err
    }
    
    // Write the object. Don't let the writer clear the uncompressed data, as we
    // might need it for a descendant diff.
    obj.LockDecompressedData()
    WriteObject(&globpackWriteRequest{
      Object: obj,
      AckChan: ackChan,
    })
    
    if _, ok := packfile.DescendedFrom[obj.Hash]; ok {
      resolvePackfileObjectsFromBase(packfile, obj, ackChan)
    }
    
    // We're done with any descendants, so free the data if we can.
    obj.UnlockDecompressedData()
  }
  
  delete(packfile.DescendedFrom, base.Hash)
  return nil
}

func LoadPackfile(packMmapped *mmap.MmappedFile, repoPath string) error {
  packfile := packMmapped.Data
  packfileReader := bytes.NewReader(packfile)
  err := initGlobpackWriter(); if err != nil {
    return err
  }
  
  packMmapped.AdviseSequential()
  err = VerifyPackfileChecksum(packfile); if err != nil {
    return err
  }
  
  packMmapped.AdviseSequential()
  numObjects, err := ReadPackfileHeader(packfileReader); if err != nil {
    return err
  }
  
  fmt.Printf("Packfile length: %d\n", len(packfile))
  fmt.Printf("Packfile has %d objects.\n", numObjects)
  
  ackChan := make(chan *GlobpackObjLoc, 1024)
  doneChan := make(chan bool)
  go func(writeAcksExpected int) {
    for i := 0; i < writeAcksExpected; i++ {
      <- ackChan
    }
    doneChan <- true
  }(numObjects)
  
  // Multi-pass pack reading.
  // Read whole pack:
  //   For non-delta objects:
  //     Write object
  //     Store thin object in BaseObjects[]
  //   For delta objects:
  //     Store thin object in DescendedFrom[baseHash][]
  packObj := new(gitPackfile)
  packObj.Data = packfile
  packObj.Reader = packfileReader
  packObj.DescendedFrom = make(map[[hashLen]byte][]*gitObject)
  for objOn :=0; objOn < numObjects; objOn++ {
    // Read an object from the packfile.
    obj, err := ReadPackfileObject(packObj); if err != nil {
      return err
    }
    
    if obj.HasDelta() {
      // This is a delta object. Clean out the decompressed delta data, then
      // store the object.
      obj.Delta = nil
      packObj.DescendedFrom[obj.Refs] =
        append(packObj.DescendedFrom[obj.Refs], obj)
    } else {
      // This is a regular non-delta object.
      // Get ready to write this object out.
      obj.AddHash()
      
      // Write the object.
      WriteObject(&globpackWriteRequest{
        Object: obj,
        AckChan: ackChan,
      })
      
      // Store this as a base object.
      packObj.BaseObjects = append(packObj.BaseObjects, obj)
    }
    // fmt.Printf("Read object %40x\n", obj.Hash)
  }
fmt.Printf("First pass: %d plain objects, %d distinct delta bases\n", len(packObj.BaseObjects), len(packObj.DescendedFrom))
  numBaseObjects := len(packObj.BaseObjects)
  numDeltaBases := len(packObj.DescendedFrom)
  
  // For each obj := range BaseObjects:
  //   DFS mine DescendedFrom:
  //     if obj.hash in DescendedFrom, for obj := range DescendedFrom[obj.hash]:
  //       Apply delta
  //       Write object
  //       DFS mine DescendedFrom.
  
  // We're basically going to read sequentially again. It would be nice to
  // advise that we're not going to touch areas we're skipping, but for now
  // this is a good start.
  packMmapped.AdviseSequential()
  for _, obj := range packObj.BaseObjects {
    if _, ok := packObj.DescendedFrom[obj.Hash]; ok {
      obj.DecompressIfNecessary()
      
      // Hold on to the data for the moment.
      obj.LockDecompressedData()
      
      // fmt.Printf("Recursing through object %40x\n", obj.Hash)
      err := resolvePackfileObjectsFromBase(packObj, obj, ackChan)
      if err != nil {
        panic(err)
      }
      
      // Free our lock on the decompressed data, as we're done with it here.
      obj.UnlockDecompressedData()
    }
  }
fmt.Printf("Second pass: %d distinct delta bases remain.\n", len(packObj.DescendedFrom))
  numExternalBases := len(packObj.DescendedFrom)
  
  // For each baseHash, objs := DescendedFrom:
  //   Lookup baseHash in existing globpacks:
  //     If present:
  //       Read object
  //       DFS mine DescendedFrom
  //     If not present:
  //       Next iteration
  for baseHash, _ := range packObj.DescendedFrom {
    if base, err := GetObject(baseHash); err == nil {
      err = resolvePackfileObjectsFromBase(packObj, base, ackChan)
      if err != nil {
        panic(err)
      }
    } else {
      fmt.Printf("Second pass unknown base object %x, err: %+v\n", baseHash, err)
    }
  }
  
  // If we couldn't read the pack because of a missing external reference, we'll
  // just return an error here. We won't wait for in-progress writes, although
  // they'll [probably] happen anyway: we don't have the full content, and any
  // failed writes will be caught up in the full pack we're about to re-queue.
  if len(packObj.DescendedFrom) > 0 {
    influxWritePoint("errors", map[string]string{
      "class": "gitpack",
      "name": "external_deltas",
    }, map[string]interface{}{
      "value": 1,
      "repo_path": repoPath,
    })
    fmt.Printf("Couldn't resolve external refs:\n")
    for baseHash, _ := range packObj.DescendedFrom {
      fmt.Printf("  %x\n", baseHash)
    }
    return CouldntResolveExternalDeltasError
  }
  
  <- doneChan
  
  influxWritePoint("gitpack", map[string]string{}, map[string]interface{}{
    "size": len(packfile),
    "objects": numObjects,
    "plain_objects": numBaseObjects,
    "delta_bases": numDeltaBases,
    "external_bases": numExternalBases,
    "repo_path": repoPath,
  })
  
  return nil
}
