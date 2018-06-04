package main

import (
  "bytes"
  "compress/zlib"
  "database/sql"
  "encoding/hex"
  "encoding/json"
  "errors"
  "fmt"
  "github.com/aschmitz/gitglob/git"
  "net/http"
  "os"
  "strconv"
  "strings"
  "runtime"
  
  "github.com/aschmitz/gitglob/globpack"
  r "github.com/dancannon/gorethink"
  _ "github.com/lib/pq"
)

var rSession *r.Session
var dbConn *sql.DB

const (
  getRepoPathQuery = `SELECT url FROM repos WHERE id = $1;`
)

type GitglobConf struct {
    DbConnectionString string
}

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

func writeObjectByHex(objHashStr string, w http.ResponseWriter, asGit bool) {
  obj, err := getObjectByHex(objHashStr); if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
  }
  
  writeObject(obj, w, asGit)
}

func writeObject(obj *git.Object, w http.ResponseWriter, asGit bool) {
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

func writeTree(commitObj *git.Object, w http.ResponseWriter) {
  if commitObj.Type != git.GitTypeCommit {
    http.Error(w, "400 object is not a commit", http.StatusBadRequest)
    return
  }
  
  commit, err := commitObj.ReadCommit()
  if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
  }
  
  seenTrees := make(map[[git.HashLen]byte]bool)
  
  writeTreeRecurse([]byte{}, commit.Tree, w, seenTrees)
}

func writeTreeRecurse(pathPrefix []byte, treeHash [git.HashLen]byte,
  w http.ResponseWriter, seenTrees map[[git.HashLen]byte]bool) {
  var treeObj *git.Object
  var treeEntries []git.TreeEntry
  var err error
  
  // Load the tree from disk
  treeObj, err = globpack.GetObject(treeHash)
  if err != nil {
    http.Error(w, "500 error looking up tree object: "+err.Error(),
      http.StatusInternalServerError)
    return
  }
  
  // Try to read the list of entries in the tree
  treeEntries, err = treeObj.ReadTree()
  if err != nil {
    http.Error(w, "500 error reading tree object: "+err.Error(),
      http.StatusInternalServerError)
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
        writeTreeRecurse(append(fullPath, byte('/')), treeEntry.Hash, w, seenTrees)
      }
    }
  }
}

func writeRepoFile(commitObj *git.Object, pathParts []string, w http.ResponseWriter) {
  if commitObj.Type != git.GitTypeCommit {
    http.Error(w, "400 object is not a commit", http.StatusBadRequest)
    return
  }
  
  commit, err := commitObj.ReadCommit()
  if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
  }
  
  writeRepoFileRecurse(commit.Tree, pathParts, w)
}

func writeRepoFileRecurse(treeHash [git.HashLen]byte, pathParts []string, w http.ResponseWriter) {
  var treeObj *git.Object
  var treeEntries []git.TreeEntry
  var err error
  var obj *git.Object
  
  // Load the tree from disk
  treeObj, err = globpack.GetObject(treeHash)
  if err != nil {
    http.Error(w, "500 error looking up tree object: "+err.Error(),
      http.StatusInternalServerError)
    return
  }
  
  // Try to read the list of entries in the tree
  treeEntries, err = treeObj.ReadTree()
  if err != nil {
    http.Error(w, "500 error reading tree object: "+err.Error(),
      http.StatusInternalServerError)
    return
  }
  
  // Try to find the next directory we're looking for
  nextPathPart := pathParts[0]
  pathParts = pathParts[1:]
  for _, treeEntry := range treeEntries {
    if bytes.Equal(treeEntry.Name, []byte(nextPathPart)) {
      // This is what we're looking for. Are we trying to recurse further?
      if len(pathParts) > 0 {
        // Yes. Is this a tree?
        if treeEntry.Mode == git.GitModeTree {
          // Yes. Let's recurse.
          writeRepoFileRecurse(treeEntry.Hash, pathParts, w)
          return
        } else {
          // No. Throw an error.
          http.Error(w, "400 can't recurse into a non-tree",
            http.StatusBadRequest)
          return
        }
      } else {
        // No more recursion, we want to return this object.
        if (treeEntry.Mode == git.GitModeFile) ||
           (treeEntry.Mode == git.GitModeExecutable) {
          // We can just send the object itself.
          // First, find it.
          obj, err = globpack.GetObject(treeEntry.Hash)
          if err != nil {
            http.Error(w, err.Error(), http.StatusInternalServerError)
            return
          }
          
          writeObject(obj, w, false)
          return
        } else {
          // We don't (yet?) know how to display these.
          http.Error(w, "500 non-files not yet supported",
            http.StatusInternalServerError)
          return
        }
      }
    }
  }
  
  // We made it to this point without finding the name we're looking for. It's
  // not here.
  http.Error(w, "404 file not found", http.StatusNotFound)
  return
}

func handler(w http.ResponseWriter, req *http.Request) {
  sepLoc := strings.Index(req.URL.Path[1:], "/")
  requestType := req.URL.Path[0:sepLoc+1]
  switch {
  // /object/:objectid: Retrieve and return the raw data for an object
  case requestType == "/object":
    handleObjectReq(w, req)
  // /repo/:repopath: Provide a number of features relating to a repository
  case requestType == "/repo":
    handleRepoReq(w, req)
  // /tree/:commithash: Retrieve a tree given a commit hash
  case requestType == "/tree":
    handleTreeReq(w, req)
  // /file/:repoid/:filename(?ref=[ref]): retrieve a file
  case requestType == "/file":
    handleFileReq(w, req)
  default:
    http.Error(w, requestType, http.StatusInternalServerError)
    // http.NotFound(w, req)
  }
}

func handleObjectReq(w http.ResponseWriter, req *http.Request) {
  // Strip off "/object/"
  objHash := req.URL.Path[8:]
  writeObjectByHex(objHash, w, false)
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
    case pathRemainder[0:6] == "/tree/":
      // List all files in the commit referred to by the ref after /tree/.
      handleTreeRefReq(repoPath, pathRemainder[6:], w, req)
    case (len(pathRemainder) == 50 && pathRemainder[0:9] == "/objects/" &&
      pathRemainder[11:12] == "/"):
      // /objects/5a/d095d6b595e6a9b15b5ffa659907229a9088ee is a request for the
      // 5ad095d6b595e6a9b15b5ffa659907229a9088ee object. We don't bother
      // trying to validate that the request was sent from the right repository,
      // because all repositories are assumed to be public, and may share
      // objects.
      objHash := pathRemainder[9:11] + pathRemainder[12:]
      writeObjectByHex(objHash, w, true)
    default:
      http.NotFound(w, req)
  }
}

func handleTreeRefReq(repoPath string, refName string, w http.ResponseWriter, req *http.Request) {
  res, err := r.DB("gitglob").Table("refs_latest").Get(repoPath).Field("refs").Field(refName).Run(rSession)
  if err != nil {
    http.Error(w, "500 ref lookup error", http.StatusInternalServerError)
    return
  }
  
  var commithash [20]byte
  err = res.One(&commithash)
  
  switch {
  case err == r.ErrEmptyResult:
    http.NotFound(w, req)
  case err != nil:
    http.Error(w, "500 refs lookup error: reading",
      http.StatusInternalServerError)
  default:
    commitObj, err := globpack.GetObject(commithash)
    if err != nil {
      http.Error(w, "500 commit lookup error", http.StatusInternalServerError)
      return
    }
    writeTree(commitObj, w)
  }
}

func handleTreeReq(w http.ResponseWriter, req *http.Request) {
  // Strip off the object ID from "/tree/[object ID]"
  objHashStr := req.URL.Path[6:]
  
  commitObj, err := getObjectByHex(objHashStr); if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
  }
  
  writeTree(commitObj, w)
}

func handleFileReq(w http.ResponseWriter, req *http.Request) {
  // Retrieving a file from the path /file/:repoid/:filename(?ref=[ref])
  pathParts := strings.Split(req.URL.Path, "/")
  
  if len(pathParts) < 4 {
    http.Error(w, "400 invalid path", http.StatusBadRequest)
    return
  }
  
  // First two are "" and "file", get the repo ID.
  repoId, err := strconv.Atoi(pathParts[2])
  pathParts = pathParts[3:]
  if err != nil {
    http.Error(w, "400 error parsing repo ID", http.StatusBadRequest)
    return
  }
  
  // Get the repository information
  var repoPath string
  err = dbConn.QueryRow(getRepoPathQuery, repoId).Scan(&repoPath)
  if err != nil {
    http.Error(w, "500 error looking up repo", http.StatusInternalServerError)
    return
  }
  
  // Figure out which ref we're working with.
  refName := req.URL.Query().Get("ref")
  if refName == "" {
    // We default to the HEAD ref from the server. This does The Right Thing
    // for GitHub, and probably does for many other servers.
    refName = "HEAD"
  }
  
  // Look up the repository and this ref's hash.
  res, err := r.DB("gitglob").Table("refs_latest").Get(repoPath).Field("refs").Field(refName).Run(rSession)
  if err != nil {
    http.Error(w, "500 ref lookup error", http.StatusInternalServerError)
    return
  }
  
  var commithash [20]byte
  err = res.One(&commithash)
  
  switch {
  case err == r.ErrEmptyResult:
    http.NotFound(w, req)
  case err != nil:
    http.Error(w, "500 refs lookup error: reading",
      http.StatusInternalServerError)
  default:
    commitObj, err := globpack.GetObject(commithash)
    if err != nil {
      http.Error(w, "500 commit lookup error", http.StatusInternalServerError)
      return
    }
    
    writeRepoFile(commitObj, pathParts, w)
  }
}

func main() {
  runtime.GOMAXPROCS(runtime.NumCPU())
  var err error
  
  configFile, _ := os.Open("config.json")
  configDecoder := json.NewDecoder(configFile)
  config := GitglobConf{}
  err = configDecoder.Decode(&config)
  if err != nil {
    fmt.Println("Error reading configuration:", err)
    os.Exit(1)
  }
  
  dbConn, err = sql.Open("postgres", config.DbConnectionString)
  if err != nil {
    fmt.Println("Error connecting to DB:", err)
    os.Exit(1)
  }
  err = dbConn.Ping()
  if err != nil {
    fmt.Println("Error pinging DB:", err)
    os.Exit(1)
  }
  
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
