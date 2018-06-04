package git

import (
  "bufio"
  "bytes"
  "errors"
  "fmt"
  "io"
  "strconv"
)

const (
  GitModeFile        = 0100644
  GitModeExecutable  = 0100755
  GitModeTree        = 0040000
  GitModeSymlink     = 0120000
  GitModeSubmodule   = 0160000
)

type TreeEntry struct {
  Mode int           // The file type for this entry
  Name []byte        // The name for this file, may contain any non-null bytes
  Hash [HashLen]byte // Object hash
}

func (obj *Object) ReadTree() (entries []TreeEntry, err error) {
  var modeWithSpace []byte
  var tmpMode int64
  var tmpName []byte
  
  if obj.Type != GitTypeTree {
    err = errors.New("attempted to read tree from non-tree object")
    return
  }
  
  blobBufReader := bufio.NewReader(bytes.NewReader(obj.Data))
  
  for {
    entry := TreeEntry{}
    
    // Modes have a trailing space, read this one
    modeWithSpace, err = blobBufReader.ReadBytes(' ')
    if err != nil {
      if err == io.EOF {
        // This is normal: we may not be able to read more if we're at the end.
        err = nil
        break
      } else {
        // err is already set
        return
      }
    }
    // Most modes are 7 digits, trees are 6.
    if (len(modeWithSpace) != 7) && (len(modeWithSpace) != 6) {
      err = errors.New(fmt.Sprintf("unexpected mode length: %d ('%s')",
        len(modeWithSpace), modeWithSpace))
      return
    }
    
    // Parse the mode out, and set the value.
    tmpMode, err = strconv.ParseInt(
      string(modeWithSpace[:len(modeWithSpace)-1]), 8, 32)
    if err != nil {
      err = errors.New(fmt.Sprintf("unexpected mode value: '%s'",
        modeWithSpace[:6]))
      return
    }
    entry.Mode = int(tmpMode)
    
    // Filenames have a trailing NULL, read this one.
    tmpName, err = blobBufReader.ReadBytes(0)
    if err != nil {
      err = errors.New("unable to read filename")
      return
    }
    
    // Strip the trailing NULL
    entry.Name = tmpName[:len(tmpName)-1]
    
    // Read the object hash in question
    _, err = io.ReadFull(blobBufReader, entry.Hash[:])
    if err != nil {
      return
    }
    
    entries = append(entries, entry)
  }
  
  return
}
