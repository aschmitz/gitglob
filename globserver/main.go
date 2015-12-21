package main

import (
  "compress/zlib"
  "encoding/hex"
  "fmt"
  "net/http"
  "strings"
  "runtime"
  
  "gitlab.lardbucket.org/aschmitz/gitglob/globpack"
  r "github.com/dancannon/gorethink"
)

const (
  hashLen = 20
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

func writeObject(objHashStr string, w http.ResponseWriter, req *http.Request) {
  var objHash [hashLen]byte
  decLen, err := hex.Decode(objHash[:], []byte(objHashStr))
  if err != nil || decLen != hashLen {
    http.Error(w, "500 object hash decode error",
      http.StatusInternalServerError)
    return
  }
  obj, err := globpack.GetObject(objHash); if err != nil {
    http.Error(w, "500 object retrieval error", http.StatusInternalServerError)
    return
  }
  
  zlibWriter := zlib.NewWriter(w)
  fmt.Fprintf(zlibWriter, "%s %d\x00", globpack.GetObjectTypeString(obj.Type),
    len(obj.Data))
  zlibWriter.Write(obj.Data)
  zlibWriter.Close()
}

func handler(w http.ResponseWriter, req *http.Request) {
  gitPathSuffixIndex := strings.LastIndex(req.URL.Path, ".git")
  if gitPathSuffixIndex == -1 {
    http.NotFound(w, req)
    return
  }
  gitPathSuffixIndex += 4
  repoPath := req.URL.Path[1:gitPathSuffixIndex]
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
      writeObject(objHash, w, req)
    default:
      http.NotFound(w, req)
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
  fmt.Printf("Listening on port 3001.\n")
  http.ListenAndServe(":3001", nil)
}
