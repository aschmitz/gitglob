package git

import (
  "bytes"
  "crypto/sha1"
  "errors"
  "fmt"
  "github.com/aschmitz/gitglob/debugging/flate"
  "io/ioutil"
  "sync/atomic"
)

const (
  GitTypeCommit      = 1
  GitTypeTree        = 2
  GitTypeBlob        = 3
  GitTypeTag         = 4
  GitTypeOffsetDelta = 6
  GitTypeRefDelta    = 7
  
  ObjCompressedNone  = 0
  ObjCompressedFull  = 1
  ObjCompressedDelta = 2
  
  ZlibFastestLevel = 0
  ZlibFastLevel    = 1
  ZlibDefaultLevel = 2
  ZlibBestLevel    = 3
  
  HashLen = 20
)

type Object struct {
  Type int           // The git ID for the object type
  Hash [HashLen]byte // Object hash, may be nil
  Data []byte        // The full object data, may be nil
  Delta []byte       // A delta, if any
  Refs [HashLen]byte // The object ID that this delta references
  Depth int          // The number of objects "below" this delta
  CompressedData []byte // A slice of the *deflated* data for this object
  CompressedType int // The type of compressed data stored
  CompressedLevel int // The compression level claimed for the compressed data
  Context interface{} // Any data the user wants to associate with this object
  // The number of outstanding locks on the object's decompressed data
  DecompressedLockCount uint32
}

func (obj *Object) HasDelta() bool {
  return (obj.Delta != nil) || (obj.CompressedType == ObjCompressedDelta)
}

func (obj *Object) DecompressIfNecessary() error {
  // Do we even need to decompress anything?
  switch obj.CompressedType {
    case ObjCompressedDelta: if obj.Delta != nil { return nil }
    case ObjCompressedFull: if obj.Data != nil { return nil }
    case ObjCompressedNone: return nil
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
    case ObjCompressedDelta: obj.Delta = decompressedData
    case ObjCompressedFull: obj.Data = decompressedData
  }
  
  return nil
}

func (obj *Object) LockDecompressedData() {
  atomic.AddUint32(&obj.DecompressedLockCount, 1)
}

func (obj *Object) UnlockDecompressedData() {
  // From the go atomic documentation:
  // "In particular, to decrement x, do AddUint32(&x, ^uint32(0))."
  remaining := atomic.AddUint32(&obj.DecompressedLockCount, ^uint32(0))
  
  // Can we clear the decompressed data?
  if remaining == 0 && obj.CompressedType != ObjCompressedNone {
    obj.Delta = nil
    obj.Data = nil
  }
}

func (obj *Object) ObjectTypeString() string {
  switch obj.Type {
    case GitTypeCommit: return "commit"
    case GitTypeTree: return "tree"
    case GitTypeBlob: return "blob"
    case GitTypeTag: return "tag"
    default: return "unknown"
  }
}

func (obj *Object) AddHash() {
  hash := sha1.New()
  fmt.Fprintf(hash, "%s %d\x00", obj.ObjectTypeString(), len(obj.Data))
  hash.Write(obj.Data)
  fullHash := hash.Sum(nil)
  copy(obj.Hash[:HashLen], fullHash)
}
