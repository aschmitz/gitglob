package git

import (
  "bytes"
  "encoding/hex"
  "errors"
)

type CommitHeader struct {
  Name []byte
  Value []byte
}

type Commit struct {
  Tree [HashLen]byte
  Parents [][HashLen]byte
  Author []byte
  Committer []byte
  AllHeaders []CommitHeader
  CommitMessage []byte
}

func readCommitHeader(blob []byte) (header CommitHeader, new_blob []byte, err error) {
  // Each commit header matches /[\S]+ [^\n]*(\n [^\n])*\n/
  // If the header has "\n " (note the space) after it, the second portion
  // is a continuation of the first, and should not include the "\n ".
  // If the header has no content (no space, nothing), it indicates the end of
  // the headers for the commit.
  
  nameLen := bytes.IndexByte(blob, ' ')
  if nameLen == -1 {
    err = errors.New("no space in commit header")
    return
  }
  
  // We have the name, move on.
  header.Name = blob[:nameLen]
  new_blob = blob[nameLen:]
  
  // Loop as long as we have a value beginning with a space.
  header.Value = []byte{}
  for new_blob[0] == ' ' {
    lineLen := bytes.IndexByte(new_blob, '\n')
    if lineLen == -1 {
      err = errors.New("no newline in commit header")
      return
    }
    
    // We include the newline in the value in case this is a multiline header.
    // We'll strip the last newline off before we return.
    header.Value = append(header.Value, new_blob[1:lineLen+1]...)
    new_blob = new_blob[lineLen+1:]
  }
  
  header.Value = header.Value[:len(header.Value)-1]
  
  return
}

func hexIdToBytes(id []byte) (as_bytes [HashLen]byte, err error) {
  if len(id) != 2 * HashLen {
    err = errors.New("unexpected object ID length")
    return
  }
  
  _, err = hex.Decode(as_bytes[:], id)
  
  return
}

func (obj *Object) ReadCommit() (commit Commit, err error) {
  var header CommitHeader
  var newCommitParent [HashLen]byte
  commitData := obj.Data
  
  // We're done reading headers when we have an "empty header" (a bare newline)
  for commitData[0] != '\n' {
    header, commitData, err = readCommitHeader(commitData)
    if err != nil { return }
    
    if string(header.Name) == "tree" {
      commit.Tree, err = hexIdToBytes(header.Value)
      if err != nil { return }
    }
    if string(header.Name) == "parent" {
      newCommitParent, err = hexIdToBytes(header.Value)
      if err != nil { return }
      
      commit.Parents = append(commit.Parents, newCommitParent)
    }
    if string(header.Name) == "author" {
      commit.Author = header.Value
    }
    if string(header.Name) == "committer" {
      commit.Committer = header.Value
    }
    commit.AllHeaders = append(commit.AllHeaders, header)
  }
  
  commit.CommitMessage = commitData[1:]
  
  return
}
