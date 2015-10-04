package globpack

import (
  "bufio"
  "bytes"
  "encoding/binary"
  "errors"
  "fmt"
  "io"
  "io/ioutil"
  "os"
  "time"
  "strconv"
  "sync/atomic"
  
  "gitlab.lardbucket.org/aschmitz/gitglob/debugging/flate"
  "github.com/garyburd/redigo/redis"
)

const (
  parallelLookupGoroutines = 100
  lookupChanBuffer = 1024
)

type globpackReaderStatsType struct {
  LookedUp uint64 // The number of objects looked up
  Existed uint64  // The number of objects that existed when looked up
  Read uint64     // The number of objects that were read from the disk
}
var globpackReaderStats *globpackReaderStatsType

type globpackLookupRequest struct {
  Hash [hashLen]byte // The hash to look up
  // A channel to write to for the response
  ResChan chan<- *globpackObjLoc
  Context interface{} // Will be returned with the response
}

var lookupChan chan *globpackLookupRequest

func init() {
  if lookupChan == nil {
    lookupChan = make(chan *globpackLookupRequest, lookupChanBuffer)
    for threadOn := 0; threadOn < parallelLookupGoroutines; threadOn++ {
      go lookupWatcher(lookupChan)
    }
    
    globpackReaderStats = &globpackReaderStatsType{}
    go globpackReaderStatsReporter()
  }
}

func globpackReaderStatsReporter() {
  oldLookedUp := uint64(0)
  oldExisted := uint64(0)
  oldRead := uint64(0)
  for {
    time.Sleep(15 * time.Second) // Every 15 seconds,
    
    // Read the new values and calculate what to store
    curLookedUp := atomic.LoadUint64(&globpackReaderStats.LookedUp)
    deltaLookedUp := curLookedUp - oldLookedUp
    oldLookedUp = curLookedUp
    curExisted := atomic.LoadUint64(&globpackReaderStats.Existed)
    deltaExisted := curExisted - oldExisted
    oldExisted = curExisted
    curRead := atomic.LoadUint64(&globpackReaderStats.Read)
    deltaRead := curRead - oldRead
    oldRead = curRead
    
    // Then write the stats to InfluxDB.
    influxWritePoint("globpack_reader", map[string]string{},
      map[string]interface{}{
        "looked_up": int(deltaLookedUp),
        "existed": int(deltaExisted),
        "read": int(deltaRead),
      })
  }
}

func LookupObjLocation(id [hashLen]byte) (*globpackObjLoc, error) {
  resChan := make(chan *globpackObjLoc)
  lookupReq := globpackLookupRequest{
    Hash: id,
    ResChan: resChan,
  }
  lookupChan <- &lookupReq
  res := <- resChan
  return res, nil
}

func FilenumToFilename(filenum uint64) string {
  return "gitglob_"+strconv.FormatUint(filenum, 10)+".globpack"
}

func lookupWatcher(lookups <-chan *globpackLookupRequest) {
  redisConn, err := redis.Dial("tcp", ":16379"); if err != nil {
    panic(err)
  }
  defer redisConn.Close()
  
  for {
    lookupReq := <-lookups
    
    redisRes, err := redisConn.Do("GET", lookupReq.Hash[:]); if err != nil {
      panic(err)
    }
    
    atomic.AddUint64(&globpackReaderStats.LookedUp, 1)
    if redisRes != nil {
      redisVal := redisRes.([]byte)
      atomic.AddUint64(&globpackReaderStats.Existed, 1)
      filenum := binary.LittleEndian.Uint64(redisVal[0:8])
      locObj := globpackObjLoc {
        Filename: FilenumToFilename(filenum),
        Filenum: filenum,
        Position: binary.LittleEndian.Uint64(redisVal[8:16]),
        Existed: true,
        Context: lookupReq.Context,
      }
      locObj.Context = lookupReq.Context
      lookupReq.ResChan <- &locObj
    } else {
      locObj := globpackObjLoc {
        Existed: false,
        Context: lookupReq.Context,
      }
      lookupReq.ResChan <- &locObj
    }
  }
}

func GetObject(id [hashLen]byte) (*gitObject, error) {
  loc, err := LookupObjLocation(id); if err != nil {
    return nil, err
  }
  
  if loc.Existed == false {
    return nil, errors.New("object not recorded locally")
  }
  
  path, err := GlobpackNameToFullPath(loc.Filename); if err != nil {
    return nil, err
  }
  
  file, err := os.Open(path); if err != nil {
    return nil, err
  }
  defer file.Close()
  
  pos, err := file.Seek(int64(loc.Position), os.SEEK_SET); if err != nil {
    return nil, err
  }
  if uint64(pos) != loc.Position {
    return nil, errors.New("Unable to seek to correct location in globpack.")
  }
  fileReader := bufio.NewReader(file)
  
  var hash [hashLen]byte
  _, err = fileReader.Read(hash[:]); if err != nil {
    return nil, err
  }
  if !bytes.Equal(hash[:], id[:]) {
    return nil, errors.New("Read object hash didn't match requested hash.")
  }
  
  storedType, err := fileReader.ReadByte(); if err != nil {
    return nil, err
  }
  
  obj := &gitObject{}
  if (storedType & 0x08) > 0 {
    // This is a delta object. Resolve the base object first.
    var baseHash [hashLen]byte
    _, err = fileReader.Read(baseHash[:]); if err != nil {
      return nil, err
    }
    baseObj, err := GetObject(baseHash); if err != nil {
      return nil, err
    }
    
    // Now read the delta.
    deltaLen, err := binary.ReadUvarint(fileReader); if err != nil {
      return nil, err
    }
    obj.Delta = make([]byte, deltaLen)
    _, err = io.ReadFull(fileReader, obj.Delta); if err != nil {
      return nil, err
    }
    
    // Deal with compression.
    if (storedType & 0x10) > 0 {
      decompressedDeltaReader := flate.NewReader(bytes.NewReader(obj.Delta))
      decompressedDelta, err := ioutil.ReadAll(decompressedDeltaReader)
      if err != nil {
        return nil, err
      }
      
      obj.Delta = decompressedDelta
    }
    
    // Apply the delta.
    ApplyDelta(baseObj, obj)
  } else {
    // Read the object data.
    dataLen, err := binary.ReadUvarint(fileReader); if err != nil {
      return nil, err
    }
    obj.Data = make([]byte, dataLen)
    _, err = io.ReadFull(fileReader, obj.Data); if err != nil {
      return nil, err
    }
    
    // Deal with compression.
    if (storedType & 0x10) > 0 {
      decompressedDataReader := flate.NewReader(bytes.NewReader(obj.Data))
      decompressedData, err := ioutil.ReadAll(decompressedDataReader)
      if err != nil {
        return nil, err
      }
      
      obj.Data = decompressedData
    }
    
    obj.Depth = 0
    // The low 3 bytes are the git object type.
    obj.Type = int(storedType & 0x07)
    obj.AddHash()
  }
  
  if !bytes.Equal(obj.Hash[:], id[:]) {
    return nil, errors.New(fmt.Sprintf("Mismatched resulting object hash "+
      "(wanted %x) for object %+v.", id[:], obj))
  }
  
  atomic.AddUint64(&globpackReaderStats.Read, 1)
  
  return obj, nil
}
