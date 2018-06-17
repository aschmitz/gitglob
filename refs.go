package main

import (
  "bytes"
  "database/sql"
  "encoding/binary"
  "errors"
  "fmt"
  "math/rand"
  "os"
  "sync"
  "time"
  
  "github.com/lib/pq"
)

const (
  refpackStoragePath = "output/refpack/"
)

type refpack struct {
  Filename string // The filename of this refpack
  NumUpdates uint64   // The number of updates recorded in the refpack
  File *os.File   // The actual file object
}

var haveCurrentRefpack = false
var currentRefpack *refpack
var refpackWriterMutex sync.Mutex

func rotateRefpackRegularly() {
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
    rotateRefpack()
  }
}

func rotateRefpack() error {
  refpackWriterMutex.Lock()
  // We're not going to defer the unlock for this, because there is no safe way
  // to recover from a failure here: failure almost certainly means we don't
  // have a safely open refpack, which is not a state we want to write in.
  
  err := closeRefpack(currentRefpack); if err != nil {
    return err
  }
  
  currentRefpack, err = initRefpack(); if err != nil {
    return err
  }
  
  refpackWriterMutex.Unlock()
  return nil
}

func CloseRefpackWriter() error {
  // Here we intend to stop writing forever, generally because the program is
  // closing. Therefore, we can acquire this lock and not let it go.
  refpackWriterMutex.Lock()
  
  if haveCurrentRefpack {
    haveCurrentRefpack = false
    err := closeRefpack(currentRefpack); if err != nil {
      return err
    }
  }
  
  return nil
}

func RefpackNameToDirectory(name string) (string, error) {
  if len(name) != len("gitglob_YYYYMMDDHHIISS_123.refpack") {
    return "", errors.New("Incorrect refpack name length.")
  }
  
  return refpackStoragePath + "/" + name[8:8+6], nil
}
func RefpackNameToFullPath(name string) (string, error) {
  dir, err := RefpackNameToDirectory(name); if err != nil {
    return "", err
  }
  
  return dir + "/" + name, nil
}

func initRefpack() (*refpack, error) {
  pack := new(refpack)
  errorCount := 0
  for {
    packTime := time.Now().UTC()
    pack.Filename = "gitglob_"+packTime.Format("20060102150405")+"_"+
      fmt.Sprintf("%03d", rand.Intn(1000))+".refpack"
    
    packDir, err := RefpackNameToDirectory(pack.Filename); if err != nil {
      return nil, err
    }
    if err := os.MkdirAll(packDir, 0755); err != nil {
      return nil, errors.New("Unable to create refpack folder.")
    }
    
    packFullPath, err := RefpackNameToFullPath(pack.Filename); if err != nil {
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
          return nil, errors.New("Unable to create a refpack on disk.")
        }
      }
    }
  }
  
  fmt.Println(pack.Filename)
  
  pack.NumUpdates = 0
  
  return pack, nil
}

func initRefpackWriting() error {
  if !haveCurrentRefpack {
    var err error
    currentRefpack, err = initRefpack(); if err != nil {
      return err
    }
    go rotateRefpackRegularly()
    
    haveCurrentRefpack = true
  }
  
  return nil
}

func closeRefpack(pack *refpack) error {
fmt.Printf("closeRefpack() called for %s\n", pack.Filename)
  // Close the file
  pack.File.Close()
  
  // Determine whether we wrote any data.
  if pack.NumUpdates == 0 {
    // We didn't write anything, so we can just delete the file.
    fullPath, err := RefpackNameToFullPath(pack.Filename); if err != nil {
      return err
    }
    os.Remove(fullPath)
  }
  
  return nil
}

func CalcRefDiffs(oldRefs, newRefs map[string][]byte,
  oldTime time.Time) refDiffs {
  diffs := refDiffs {
    Type: RefDiffTypeDelta,
    From: pq.NullTime{Time: oldTime, Valid: true},
    NewRefs: make(map[string][]byte),
    ChangedRefs: make(map[string][]byte),
    DeletedRefs: make([]string, 0),
  }
  
  newHashMap := make(map[[hashLen]byte]bool)
  oldHashMap := make(map[[hashLen]byte]bool)
  
  for newRef, newHash := range newRefs {
    newHashMap[sliceToHashArray(newHash)] = true
    
    // Look up the new ref in the old ref set.
    oldHash, ok := oldRefs[newRef]
    if ok {
      // This was in the old set: check to see if the hash is the same.
      if bytes.Equal(newHash, oldHash) {
        // They're equal, don't save this.
      } else {
        // The hashes are different, store this as a changed ref.
        diffs.ChangedRefs[newRef] = newHash
        
        // And note that we had the old hash from this ref.
        oldHashMap[sliceToHashArray(oldHash)] = true
      }
    } else {
      // This didn't exist in the old set: it must be new.
      diffs.NewRefs[newRef] = newHash
    }
    // Remove this so we don't consider it any longer.
    delete(oldRefs, newRef)
  }
  
  for oldRef, oldHash := range oldRefs {
    oldHashMap[sliceToHashArray(oldHash)] = true
    
    // This wasn't deleted by the pass above, so it must not have been in the
    // new ref set. Therefore, it must have been removed since then.
    diffs.DeletedRefs = append(diffs.DeletedRefs, oldRef)
  }
  
  for hash, _ := range newHashMap {
    diffs.NewHashes = append(diffs.NewHashes, hash)
  }
  for hash, _ := range oldHashMap {
    diffs.OldHashes = append(diffs.OldHashes, hash)
  }
  
  return diffs
}

func RecordRepoRefs(repoPath string, repoId int, timestamp time.Time,
  refs map[string][]byte) (refDiffs, map[string][]byte, error) {
  var (
    diffs refDiffs
    lastStamp time.Time
    fetchCount int64
    refNames []string
    refHashes pq.ByteaArray
  )
  oldRefs := make(map[string][]byte)
  
  // Get the last set of revisions in the database.
  err := preparedLatestRefsGet.QueryRow(repoId).Scan(pq.Array(&refNames),
    &refHashes, &lastStamp, &fetchCount)
  if (err != nil) && (err != sql.ErrNoRows) {
    return diffs, oldRefs, err
  }
  
  // Calculate the differences.
  if err == sql.ErrNoRows {
    // There was no previous record of this repository.
    diffs = refDiffs{
      Type: RefDiffTypeAbsolute,
      From: pq.NullTime{Valid: false},
      NewRefs: refs,
      ChangedRefs: make(map[string][]byte),
      DeletedRefs: make([]string, 0),
    }
    
    // Create a list of new hashes
    newHashMap := make(map[[hashLen]byte]bool)
    for _, newHash := range refs {
      newHashMap[sliceToHashArray(newHash)] = true
    }
    for hash, _ := range newHashMap {
      diffs.NewHashes = append(diffs.NewHashes, hash)
    }
  } else {
    if fetchCount % maxRefDepth == 0 {
      // We want to write an absolute diff because we have gone long enough
      // without one. This prevents corruption from affecting all of a
      // repository's history, and means that "in-between" reflist lookups don't
      // have to apply deltas all the way back to be beginning of a repository.
      diffs = refDiffs{
        Type: RefDiffTypeAbsolute,
        From: pq.NullTime{Time: lastStamp, Valid: true},
        NewRefs: refs,
        ChangedRefs: make(map[string][]byte),
        DeletedRefs: make([]string, 0),
      }
    } else {
      // We want to store an actual delta: calculate it.
      calcOldRefs := make(map[string][]byte)
      for refIndex, refName := range refNames {
        oldRefs[refName] = refHashes[refIndex]
        calcOldRefs[refName] = refHashes[refIndex]
      }
      diffs = CalcRefDiffs(calcOldRefs, refs, lastStamp)
    }
  }
  
  // Write the diffs to the database
  refNewNames := make([]string, len(diffs.NewRefs))
  refNewHashes := make([][]byte, len(diffs.NewRefs))
  refChangedNames := make([]string, len(diffs.ChangedRefs))
  refChangedHashes := make([][]byte, len(diffs.ChangedRefs))
  
  sliceIndex := 0
  for refNewName, refNewHash := range diffs.NewRefs {
    refNewNames[sliceIndex] = refNewName
    refNewHashes[sliceIndex] = refNewHash
    sliceIndex++
  }
  sliceIndex = 0
  for refChangedName, refChangedHash := range diffs.ChangedRefs {
    refChangedNames[sliceIndex] = refChangedName
    refChangedHashes[sliceIndex] = refChangedHash
    sliceIndex++
  }
  _, err = preparedHistoryRefsAdd.Exec(repoId, timestamp, diffs.Type,
    diffs.From, pq.StringArray(refNewNames), pq.ByteaArray(refNewHashes),
    pq.StringArray(refChangedNames), pq.ByteaArray(refChangedHashes),
    pq.StringArray(diffs.DeletedRefs))
  if err != nil {
    return diffs, oldRefs, err
  }
  
  // Write the file
  refpackWriterMutex.Lock()
  
  // Format:
  // [repository address]\x00[type byte][uint64 timestamp][uint64 fromtimestamp]
  // [vuarint new length][uvarint changed length][uvarint deleted length]
  // [new refs][changed refs][deleted refs]
  
  // New and changed refs: [20-byte commithash][refname]\n
  // Deleted refs: [refname]\n
  
  // Write the repository address
  _, err = fmt.Fprintf(currentRefpack.File, "%s\x00", repoPath); if err != nil {
    return diffs, oldRefs, err
  }
  
  // Write diff type byte
  err = binary.Write(currentRefpack.File, binary.BigEndian, byte(diffs.Type))
  if err != nil {
    return diffs, oldRefs, err
  }
  
  // Write timestamps
  err = binary.Write(currentRefpack.File, binary.BigEndian, timestamp.Unix())
  if err != nil {
    return diffs, oldRefs, err
  }
  err = binary.Write(currentRefpack.File, binary.BigEndian, int64(diffs.From.Time.Unix()))
  if err != nil {
    return diffs, oldRefs, err
  }
  
  // Write section lengths
  uvarintSlice := make([]byte, binary.MaxVarintLen64)
  newUvarintLen := binary.PutUvarint(uvarintSlice,
    uint64(len(diffs.NewRefs)))
  _, err = currentRefpack.File.Write(uvarintSlice[0:newUvarintLen]); if err != nil {
    return diffs, oldRefs, err
  }
  changedUvarintLen := binary.PutUvarint(uvarintSlice,
    uint64(len(diffs.ChangedRefs)))
  _, err = currentRefpack.File.Write(uvarintSlice[0:changedUvarintLen]); if err != nil {
    return diffs, oldRefs, err
  }
  deletedUvarintLen := binary.PutUvarint(uvarintSlice,
    uint64(len(diffs.DeletedRefs)))
  _, err = currentRefpack.File.Write(uvarintSlice[0:deletedUvarintLen]); if err != nil {
    return diffs, oldRefs, err
  }
  
  // Write new refs
  for refName, commithash := range diffs.NewRefs {
    _, err = currentRefpack.File.Write(commithash[:]); if err != nil {
      return diffs, oldRefs, err
    }
    _, err = fmt.Fprintf(currentRefpack.File, "%s\n", refName); if err != nil {
      return diffs, oldRefs, err
    }
  }
  // Write changed refs
  for refName, commithash := range diffs.ChangedRefs {
    _, err = currentRefpack.File.Write(commithash[:]); if err != nil {
      return diffs, oldRefs, err
    }
    _, err = fmt.Fprintf(currentRefpack.File, "%s\n", refName); if err != nil {
      return diffs, oldRefs, err
    }
  }
  // Write deleted refs
  for _, refName := range diffs.DeletedRefs {
    _, err = fmt.Fprintf(currentRefpack.File, "%s\n", refName); if err != nil {
      return diffs, oldRefs, err
    }
  }
  
  currentRefpack.NumUpdates += 1
  
  // We can Sync() after the unlock, because syncing in the middle of another
  // write is okay.
  refpackWriterMutex.Unlock()
  currentRefpack.File.Sync()
  
  influxWritePoint("update_refs", map[string]string{}, map[string]interface{}{
    "total": len(refs),
    "new": len(diffs.NewRefs),
    "changed": len(diffs.ChangedRefs),
    "deleted": len(diffs.DeletedRefs),
    "repo_path": repoPath,
  })
  
  return diffs, oldRefs, nil
}
