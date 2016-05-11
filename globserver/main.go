package main

import (
  "compress/zlib"
  "encoding/hex"
  "errors"
  "fmt"
  "github.com/aschmitz/gitglob/git"
  "net/http"
  "strconv"
  "strings"
  "runtime"
  
  "github.com/aschmitz/gitglob/globpack"
  r "github.com/dancannon/gorethink"
)

var rSession *r.Session

func writeInfoRefs(repoPath string, w http.ResponseWriter, req *http.Request) {
  res, err := r.DB("gitglob").Table("refs_latest").Get(repoPath).Run(rSession)
  if err != nil {
    http.Error(w, "500 refs lookup error", http.StatusInternalServerError)
    return
  }
  
  var oldVal interface{}
  err = res.One(&oldVal)
  
  switch {
  case err == r.ErrEmptyResult:
    http.NotFound(w, req)
  case err != nil:
    http.Error(w, "500 refs lookup error: reading",
      http.StatusInternalServerError)
  default:
    oldValMap := oldVal.(map[string]interface{})
    for refName, commithash :=
      range oldValMap["refs"].(map[string]interface{}) {
      fmt.Fprintf(w, "%40x\t%s\n", commithash.([]byte), refName)
    }
  }
}

func getObjectByHex(objHashStr string) (obj *git.Object, err error) {
  var objHash [git.HashLen]byte
  var decLen int
  
  decLen, err = hex.Decode(objHash[:], []byte(objHashStr))
  if err != nil || decLen != git.HashLen {
    err = errors.New("500 object hash decode error")
    return
  }
  obj, err = globpack.GetObject(objHash)
  return
}

func writeObject(objHashStr string, w http.ResponseWriter, req *http.Request, asGit bool) {
  obj, err := getObjectByHex(objHashStr); if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
  }
  
  if asGit {
    w.Header().Set("Content-Type", "application/x-git-object")
    w.WriteHeader(http.StatusOK)
    zlibWriter := zlib.NewWriter(w)
    fmt.Fprintf(zlibWriter, "%s %d\x00", obj.TypeString(),
      len(obj.Data))
    zlibWriter.Write(obj.Data)
    zlibWriter.Close()
  } else {
    w.Header().Set("Content-Type", "application/octet-stream")
    w.Header().Set("X-Git-Type", obj.TypeString())
    w.Header().Set("Content-Length", strconv.Itoa(len(obj.Data)))
    w.WriteHeader(http.StatusOK)
    w.Write(obj.Data)
  }
}

func handler(w http.ResponseWriter, req *http.Request) {
  sepLoc := strings.Index(req.URL.Path[1:], "/")
  requestType := req.URL.Path[0:sepLoc+1]
  switch {
  case requestType == "/object":
    handleObjectReq(w, req)
  case requestType == "/repo":
    handleRepoReq(w, req)
  case requestType == "/tree":
    handleTreeReq(w, req)
  default:
    http.Error(w, requestType, http.StatusInternalServerError)
    // http.NotFound(w, req)
  }
}

func handleObjectReq(w http.ResponseWriter, req *http.Request) {
  // Strip off "/object/"
  objHash := req.URL.Path[8:]
  writeObject(objHash, w, req, false)
}

func handleRepoReq(w http.ResponseWriter, req *http.Request) {
  gitPathSuffixIndex := strings.LastIndex(req.URL.Path, ".git")
  if gitPathSuffixIndex == -1 {
    http.NotFound(w, req)
    return
  }
  gitPathSuffixIndex += 4
  // Cut off the "/repo/" at the beginning and any extra path
  repoPath := req.URL.Path[6:gitPathSuffixIndex]
  // Most software will replace http:// in a URL with http:/, so double up that
  // slash for the repository URL.
  repoPath = strings.Replace(repoPath, ":/", "://", 1)
  // But if for some reason they made a request with the :// already present,
  // make sure we didn't mess things up.
  repoPath = strings.Replace(repoPath, ":///", "://", 1)
  pathRemainder := req.URL.Path[gitPathSuffixIndex:]
  switch {
    case pathRemainder == "/info/refs":
      // /info/refs is a request for a list of commit hashes and refs
      writeInfoRefs(repoPath, w, req)
    case (len(pathRemainder) == 50 && pathRemainder[0:9] == "/objects/" &&
      pathRemainder[11:12] == "/"):
      // /objects/5a/d095d6b595e6a9b15b5ffa659907229a9088ee is a request for the
      // 5ad095d6b595e6a9b15b5ffa659907229a9088ee object. We don't bother
      // trying to validate that the request was sent from the right repository,
      // because all repositories are assumed to be public, and may share
      // objects.
      objHash := pathRemainder[9:11] + pathRemainder[12:]
      writeObject(objHash, w, req, true)
    default:
      http.NotFound(w, req)
  }
}

func handleTreeReq(w http.ResponseWriter, req *http.Request) {
  // Strip off the object ID from "/tree/[object ID]"
  objHashStr := req.URL.Path[6:]
  
  obj, err := getObjectByHex(objHashStr); if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
  }
  
  if obj.Type != 1 {
    http.Error(w, "400 object is not a commit", http.StatusBadRequest)
    return
  }
  
  commit, err := obj.ReadCommit()
  
  seenTrees := make(map[[git.HashLen]byte]bool)
  
  recurseTreeReq([]byte{}, commit.Tree, w, seenTrees)
}

func recurseTreeReq(pathPrefix []byte, treeHash [git.HashLen]byte,
  w http.ResponseWriter, seenTrees map[[git.HashLen]byte]bool) {
  var treeObj *git.Object
  var treeEntries []git.TreeEntry
  var err error
  
  // Load the tree from disk
  treeObj, err = globpack.GetObject(treeHash)
  if err != nil {
    w.Write([]byte("Error looking up tree object: "+err.Error()))
    return
  }
  
  // Try to read the list of entries in the tree
  treeEntries, err = treeObj.ReadTree()
  if err != nil {
    w.Write([]byte("Error reading tree object: "+err.Error()))
    return
  }
  
  // Read each entry
  for _, treeEntry := range treeEntries {
    // Write this entry to the list
    fullPath := append(pathPrefix, treeEntry.Name...)
    w.Write(fullPath)
    w.Write([]byte("\n"))
    
    // If this was a directory, we may want to recurse
    if treeEntry.Mode == git.GitModeTree {
      // Have we seen this?
      _, exists := seenTrees[treeEntry.Hash]; if !exists {
        // No, say we've seen it now.
        seenTrees[treeEntry.Hash] = true
        
        // And recurse into it.
        recurseTreeReq(append(fullPath, byte('/')), treeEntry.Hash, w, seenTrees)
      }
    }
  }
}

func main() {
  runtime.GOMAXPROCS(runtime.NumCPU())
  var err error
  rSession, err = r.Connect(r.ConnectOpts{
    Address: "localhost:28015",
    MaxIdle: 100,
    MaxOpen: 100,
  })
  if err != nil {
    panic(err.Error)
  }
  
  http.HandleFunc("/", handler)
  fmt.Printf("Listening on port 3005.\n")
  http.ListenAndServe(":3005", nil)
}
