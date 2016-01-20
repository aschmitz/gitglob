package main

import (
  "bytes"
  "database/sql"
  "encoding/binary"
  "encoding/hex"
  "encoding/json"
  "errors"
  "flag"
  "fmt"
  "io"
  "io/ioutil"
  "math/rand"
  "net"
  "net/http"
  "net/url"
  "os"
  "os/signal"
  "syscall"
  "runtime"
  "runtime/debug"
// "runtime/pprof"
  "sort"
  "strconv"
  "strings"
  "time"
  
  "github.com/aschmitz/gitglob/globpack"
  influxdb "github.com/influxdb/influxdb/client"
  _ "github.com/lib/pq"
  r "github.com/dancannon/gorethink"
)

var rSession *r.Session
var dbConn *sql.DB

const (
  hashLen = 20
  RefDiffTypeAbsolute = 1
  RefDiffTypeDelta = 2
  maxRefDepth = 100
  storagePath = "output"
  getNextRepoQuery = `UPDATE repos
    SET (queue_state, last_download_time) = (2, NOW())
    FROM (
      SELECT id, url, force_full FROM repos
        WHERE queue_state = 1 AND watching = True AND enabled = True
        ORDER BY queue_time ASC LIMIT 1 FOR UPDATE
    ) found
    WHERE repos.id = found.id
    RETURNING found.id, found.url, found.force_full;`
  
  // If the most recently queued update was before this download, then we're
  // done. If not, we need to download this repository again. Set the queue
  // state accordingly.
  markRepoUpdatedQuery = `UPDATE repos
    SET (queue_state, error_streak) = ((CASE
        WHEN queue_time < last_download_time THEN 0
        ELSE 1
      END), 0)
    WHERE id = $1;`
  
  queueRepoUpdateQuery = `UPDATE repos
    SET (queue_state, queue_time) = ((CASE
        WHEN queue_state = 0 THEN 1
        ELSE queue_state
      END), NOW())
    WHERE id = $1;`
  
  queueRepoUpdateForceFullQuery = `UPDATE repos
    SET (queue_state, queue_time, force_full) = ((CASE
        WHEN queue_state = 0 THEN 1
        ELSE queue_state
      END), NOW(), TRUE)
    WHERE id = $1;`
  
  saveRepoErrorQuery = `UPDATE repos
    SET (error_streak, last_error) = (error_streak + 1, $2)
    WHERE id = $1
    RETURNING error_streak;`
  
  disableRepoQuery = `UPDATE repos
    SET (enabled) = (FALSE)
    WHERE id = $1`
)

type GitglobConf struct {
    DbConnectionString string
}

type refDiffs struct {
  Type uint8
  From int64
  NewRefs map[string][]byte
  ChangedRefs map[string][]byte
  DeletedRefs []string
  NewHashes [][hashLen]byte
  OldHashes [][hashLen]byte
}

type commitFetchJob struct {
  Id string
  RepoAddress string
  ToRequest [][hashLen]byte
  BaseCommits [][hashLen]byte
  From int64
}

var ZeroLengthPackfile = errors.New("zero length packfile")

type unexpectedContentTypeError struct {
  errorName string
  resp *http.Response
  body []byte
  expected string
}
func (e unexpectedContentTypeError) Error() string {
  return "unexpected Content-Type: "+e.resp.Header["Content-Type"][0]+
    " (expected "+e.expected+"), status code "+strconv.Itoa(e.resp.StatusCode)+
    ", body:\n\n"+string(e.body[:])
}

type updateError struct {
  s string
  errorName string
  shouldRetry bool
}
func (e updateError) Error() string {
  return e.s
}

var influxdbClient *influxdb.Client

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

// Occasionally, we want to free memory we've taken up. This can lead to better
// behavior when running multiple copies of gitglob at the same time.
func forceGC() {
  runtime.GC()
  debug.FreeOSMemory()
}

func BuildPktLine(line []byte) ([]byte, error) {
  // Empty pkt-lines are reported as "0000".
  if line == nil {
    return []byte("0000"), nil
  }
  
  // Otherwise, four hex digits for the length, including the four hex digits
  //  themselves.
  if (len(line) + 4) > 0xffff {
    return nil, errors.New("pkt-line too long to send! Length: " +
      string(len(line)))
  }
  
  pktLen := make([]byte, 2)
  encodedLen := make([]byte, 4)
  binary.BigEndian.PutUint16(pktLen, uint16(len(line) + 4))
  hex.Encode(encodedLen, pktLen)
  
  return append(encodedLen, line...), nil
}

func ReadPktLineText(reader io.Reader) ([]byte, error) {
  return ReadPktLine(reader, true)
}

func ReadPktLineBin(reader io.Reader) ([]byte, error) {
  return ReadPktLine(reader, false)
}

func ReadPktLine(reader io.Reader, stripNewline bool) ([]byte, error) {
  // Read off four bytes: ASCII representation of the hex length of the line.
  lenBuf := make([]byte, 4)
  _, err := io.ReadFull(reader, lenBuf)
  if err != nil {
    return nil, errors.New("Couldn't read pkt-line length: " + err.Error())
  }
  
  // Decode those hex bytes to "real" bytes
  decodedLen := make([]byte, 2)
  _, err = hex.Decode(decodedLen, lenBuf)
  if err != nil {
    return nil, errors.New("Couldn't decode pkt-line length: " + err.Error())
  }
  
  // Finally, get the line length as an integer
  pktLen := binary.BigEndian.Uint16(decodedLen)
  
  // pkt-len lines of "0000" are special end markers.
  if pktLen == 0 {
    return nil, nil
  }
  
  // pkt-len lengths include themselves, so subtract four bytes.
  pktLen -= 4
  
  readLine := make([]byte, pktLen)
  bytesRead, err := io.ReadFull(reader, readLine); if err != nil {
    fmt.Println(bytesRead)
    fmt.Println(hex.Dump(readLine))
    panic(err)
    return nil, err
  }
  
  if stripNewline {
    // If the packet ends in a newline, cut it off.
    if readLine[pktLen-1] == 10 {
      readLine = readLine[0:pktLen-1]
    }
  }
  
  return readLine, err
}

func ReadUploadPackHeader(reader io.Reader) error {
  // Note that this technically doesn't follow the smart HTTP protocol:
  //   "Clients MUST validate the first five bytes of the response entity
  //    matches the regex "^[0-9a-f]{4}#".  If this test fails, clients
  //    MUST NOT continue."
  // However, hopefully this won't be an issue. At most, it will erroneously
  //  read 65k and *then* fail, not early. More likely, it will fail to parse
  //  the first four bytes as hex, and fail there, which will effectively match
  //  the described behavior.
  line, err := ReadPktLineText(reader); if err != nil {
    return errors.New("Unable to read initial service pkt-line: "+err.Error())
  }
  // "Clients MUST verify the first pkt-line is "# service=$servicename"."
  if string(line) != "# service=git-upload-pack" {
    return errors.New("Unexpected first response line: "+string(line))
  }
  line, err = ReadPktLineText(reader); if err != nil {
    return errors.New("Unable to read service-ending pkt-line: "+err.Error())
  }
  // "Servers MUST terminate the response with the magic "0000" end
  //  pkt-line marker."
  if line != nil {
    return errors.New("Unexpected non-empty pkt-line after service: "+
      string(line))
  }
  
  return nil
}

func readCapsLine(capString string) map[string]string {
  // Capabilities are space-separated keys, optionally key=value pairs.
  // We just store an empty string for the value if there is none.
  capabilities := map[string]string{};
  capStrings := strings.Split(capString, " ")
  for _, capString := range capStrings {
    capParts := strings.SplitN(capString, "=", 2)
    if len(capParts) == 1 {
      capabilities[capParts[0]] = ""
    } else {
      capabilities[capParts[0]] = capParts[1]
    }
  }
  
  return capabilities
}

func HaveCommit(commithash [hashLen]byte) (bool, globpack.GlobpackObjLoc, error) {
  objloc, err := globpack.LookupObjLocation(commithash); if err != nil {
    return false, globpack.GlobpackObjLoc{}, err
  }
  
  return objloc.Existed, *objloc, nil
}

type CommitSortObj struct {
  Filenum uint64
  Position uint64
  Commithash [hashLen]byte
}
type CommitSortList []CommitSortObj

func (cl CommitSortList) Len() int { return len(cl) }
func (cl CommitSortList) Less(i, j int) bool {
  // Did commit i probably come before commit j?
  filenumI := cl[i].Filenum
  filenumJ := cl[j].Filenum
  if filenumI < filenumJ {
    // Yes, probably.
    return true
  } else {
    // Did they come from the same file?
    if filenumI == filenumJ {
      // Yes, sort by the position in the file.
      return cl[i].Position < cl[j].Position
    } else {
      // No, so j was probably earlier.
      return false
    }
  }
}
func (cl CommitSortList) Swap(i, j int){ cl[i], cl[j] = cl[j], cl[i] }

func DownloadNewCommits(repoPath string, diffs refDiffs,
    forceFull bool) (string, error) {
  fmt.Println("Considering", len(diffs.NewHashes)+len(diffs.OldHashes),
    "commit(s)")
  
  haveHashMap := make(map[[hashLen]byte]globpack.GlobpackObjLoc)
  wantHashMap := make(map[[hashLen]byte]bool)
  var lastWanted [hashLen]byte
  
  if forceFull {
    // We want to get a full pack, so don't bother looking at old hashes.
    for _, hash := range diffs.NewHashes {
      wantHashMap[hash] = true
      lastWanted = hash
    }
  } else {
    // We should actually check which hashes we already have.
    for _, hash := range diffs.OldHashes {
      exists, objLoc, err := HaveCommit(hash); if err != nil {
        return "", err
      }
      
      if exists {
        haveHashMap[hash] = objLoc
      }
    }
    for _, hash := range diffs.NewHashes {
      _, ok := haveHashMap[hash]
      if !ok {
        // We either didn't check this object or we didn't have it. Check to see
        // if we do have it.
        exists, objLoc, err := HaveCommit(hash); if err != nil {
          return "", err
        }
        
        if exists {
          // We have the object already, so say we have it and don't request it.
          haveHashMap[hash] = objLoc
        } else {
          // We don't have the object and it was advertised, so request it.
          wantHashMap[hash] = true
          lastWanted = hash
        }
      } else {
        // We already know we have this object, so we don't have to do anything.
      }
    }
  }
  
  fmt.Println("Need", len(wantHashMap), "commit(s)")
  
  if len(wantHashMap) == 0 {
    return "", nil
  }
  
  delete(wantHashMap, lastWanted)
  
  // multi_ack_detailed might be useful, but we don't process it right now.
  // thin-pack is required to allow received packfiles to reference objects we
  // already have.
  // side-band-64k allows sending sideband packets of >1k at a time.
  request, err := BuildPktLine([]byte("want " +
    hex.EncodeToString(lastWanted[:]) +
    " thin-pack side-band-64k agent=gitglob/0.0.1\n"))
  if err != nil {
    return "", err
  }
  for commithash, _ := range wantHashMap {
    nextLine, err := BuildPktLine([]byte("want "+
      hex.EncodeToString(commithash[:])+"\n"))
    if err != nil {
      return "", err
    }
    request = append(request, nextLine...)
  }
  
  // Although not particularly documented in the smart-http docs, it seems to
  // be necessary to send an empty line *before* the "have" list.
  nextLine, err := BuildPktLine(nil)
  request = append(request, nextLine...)
  
  // Get a sorted list of hashes.
  haveList := make(CommitSortList, len(haveHashMap))
  haveListIndex := 0
  for commithash, objLoc := range haveHashMap {
    haveList[haveListIndex] = CommitSortObj{
      objLoc.Filenum,
      objLoc.Position,
      commithash,
    }
    haveListIndex++
  }
  sort.Sort(sort.Reverse(haveList))
  
  for _, cso := range haveList {
    nextLine, err := BuildPktLine([]byte("have "+
      hex.EncodeToString(cso.Commithash[:])+"\n")); if err != nil {
      return "", err
    }
    request = append(request, nextLine...)
  }
  
  nextLine, err = BuildPktLine([]byte("done\n"))
  request = append(request, nextLine...)
fmt.Println(string(request))
  
  // Actually request the packfile from the server.
  client := &http.Client{}
  req, err := http.NewRequest("POST", repoPath+
    "/git-upload-pack", bytes.NewReader(request))
  req.Header.Add("User-Agent", "gitglob/0.0.1")
  req.Header.Add("Pragma", "no-cache")
  req.Header.Add("Content-Type", "application/x-git-upload-pack-request")
  resp, err := client.Do(req)
  if err != nil {
    return "", updateError{
      s: "error connecting to git server for pack: "+err.Error(),
      errorName: "connect_pack",
      shouldRetry: true,
    }
  }
  defer resp.Body.Close()
  
  if resp.Header["Content-Type"][0] !=
      "application/x-git-upload-pack-result" {
    body, _ := ioutil.ReadAll(resp.Body)
    return "", unexpectedContentTypeError{
      errorName: "unexpected_contenttype",
      resp: resp,
      body: body,
      expected: "application/x-git-upload-pack-result",
    }
  }
  
  fmt.Println("Connected to", resp.Header["Server"][0])
  
  var line, fullPack []byte
  line, err = ReadPktLineBin(resp.Body)
  for ; line != nil; line, err = ReadPktLineBin(resp.Body) {
    if err != nil {
      fmt.Println("Error reading pack lines: "+err.Error())
      return "", err
    }
    
    switch line[0] {
    case 1:
      // Add this to the packfile in memory.
      fullPack = append(fullPack, line[1:]...)
    case 2:
      // stdout
      fmt.Println("Stdout from git server: "+string(line[1:]))
    case 3:
      // stderr
      fmt.Println("Error from git server: "+string(line[1:]))
    default:
      if bytes.HasPrefix(line, []byte("NAK")) ||
        bytes.HasPrefix(line, []byte("ACK ")) {
        continue
      }
      return "", errors.New("Unexpected sideband type "+string(line))
    }
  }
  fmt.Printf("Pack length: %d\n", len(fullPack))
  
  if len(fullPack) == 0 {
    // This usually means one of the objects we requested doesn't exist, but we
    // can't gracefully deal with that here.
    return "", ZeroLengthPackfile
  }
  
  err = globpack.VerifyPackfileChecksum(fullPack); if err != nil {
    // It's possible that we should retry, but we leave that up to the calling
    // function.
    return "", err
  }
  
  // Generate a filename based on the checksum of this file.
  filename := make([]byte, hashLen * 2)
  hex.Encode(filename, fullPack[len(fullPack)-hashLen:len(fullPack)])
  ioutil.WriteFile(storagePath+"/gitpack/"+string(filename)+".pack", fullPack,
    0644)
  
  return string(filename)+".pack", nil
}

func ReadUploadPackRefs(reader io.Reader) (map[string]string, map[string]string,
  error) {
  // Hande the first ref line separately, as it includes capabilities.
  line, err := ReadPktLineText(reader); if err != nil {
    return nil, nil, errors.New("Error reading first ref line: "+
      err.Error())
  }
  
  // Pull off the capabilities
  capParts := bytes.SplitN(line, []byte{0}, 2)
  line, capString := capParts[0], string(capParts[1])
  
  capabilities := readCapsLine(capString)
  
  // For reading refs, there are two things we want to do: we want to check
  //  commits to see if we don't have (and therefore, need) them, and we want
  //  to store a history of the existing refs for archival.
  refs := map[string]string{}; // refs[refName] = commithash
  for ; line != nil; line, err = ReadPktLineText(reader) {
    if err != nil {
      return nil, nil, errors.New("Error reading line: " + err.Error())
    }
    
    refParts := strings.SplitN(string(line), " ", 2)
    if len(refParts) != 2 {
      return nil, nil, errors.New("Unexpectedly short ref: "+string(line))
    }
    
    commit, refName := refParts[0], refParts[1]
    refs[refName] = commit
  }
  
  return capabilities, refs, nil
}

func hashArraySliceToSliceSlice(in [][hashLen]byte) [][]byte {
  out := make([][]byte, len(in))
  for index, objhash := range in {
    copiedhash := make([]byte, hashLen)
    copy(copiedhash, objhash[:])
    out[index] = copiedhash
  }
  return out
}

func hashSliceSliceToArraySlice(in []interface{}) [][hashLen]byte {
  out := make([][hashLen]byte, len(in))
  for index, objhash := range in {
    var copiedhash [hashLen]byte
    copy(copiedhash[:], objhash.([]byte))
    out[index] = copiedhash
  }
  return out
}

func sliceToHashArray(in []byte) [hashLen]byte {
  var out [hashLen]byte
  copy(out[:], in)
  return out
}

func doUpdateRepoRefs(repoId int, repoPath string, forceFull bool) error {
  timestamp := time.Now().UTC()
  client := &http.Client{}
  req, err := http.NewRequest("GET", repoPath+
    "/info/refs?service=git-upload-pack", nil)
  req.Header.Add("User-Agent", "gitglob/0.0.1")
  req.Header.Add("Pragma", "no-cache")
  resp, err := client.Do(req)
  if err != nil {
    err = updateError{
      s: "error connecting to git server for refs: "+err.Error(),
      errorName: "connect_refs",
      shouldRetry: true,
    }
    return handleUpdateError(err, repoId, repoPath)
  }
  defer resp.Body.Close()
  
  if resp.Header["Content-Type"][0] !=
      "application/x-git-upload-pack-advertisement" {
    body, _ := ioutil.ReadAll(resp.Body)
    return handleUpdateError(unexpectedContentTypeError{
      errorName: "unexpected_contenttype",
      resp: resp,
      body: body,
      expected: "application/x-git-upload-pack-advertisement",
    }, repoId, repoPath)
  }
  
  err = ReadUploadPackHeader(resp.Body); if err != nil {
    return err
  }
  
  // We ignore the remote capabilities at the moment. We shouldn't, but we do.
  // TODO: Check for the caps we need.
  _, hexRefs, err := ReadUploadPackRefs(resp.Body); if err != nil {
    return err
  }
  resp.Body.Close()
  
  // Get the commits as the byte versions, not the hex ones.
  refs := make(map[string][]byte)
  for refName, commithashHex := range hexRefs {
    commithash := make([]byte, 20)
    _, err := hex.Decode(commithash, []byte(commithashHex)); if err != nil {
      return err
    }
    refs[refName] = commithash
  }
  
  newRefDiffs, _, err := RecordRepoRefs(repoPath, timestamp, refs)
  if err != nil {
    return err
  }
  
  // If we have new commits to download, do that.
  if true {
    filename, err := DownloadNewCommits(repoPath, newRefDiffs, forceFull)
    if err == nil {
      // Queue processing of this packfile, if we wrote one. The packfile
      // filename may be blank if for some reason we already had all of these
      // commits.
      if filename != "" {
        _, err = r.DB("gitglob").Table("queued_packs").Insert(
          map[string]interface{} {
            "id": filename,
            "repo_path": repoPath,
            "repo_id": repoId,
            "queue_time": r.Now(),
          }, r.InsertOpts {
            Conflict: "update",
          }).RunWrite(rSession)
        if err != nil {
          return err
        }
fmt.Printf("Wrote file %s.\n", filename)
      }
      
      // Save the list of refs as the latest one.
      _, err = r.DB("gitglob").Table("refs_latest").Get(repoPath).Replace(
        map[string]interface{} {
          "id": repoPath,
          "refs": refs,
          "refStamp": timestamp.Unix(),
          "refFetches": r.Row.Field("refFetches").Default(0).Add(1),
        }).RunWrite(rSession)
      if err != nil {
        return err
      }
    } else {
      return handleUpdateError(err, repoId, repoPath)
    }
  }
  
  return nil
}

func handleUpdateError(upErr error, repoId int, repoPath string) error {
  shouldRetry := false
  shouldForceFull := false
  errorName := "unknown"
  switch upErr := upErr.(type) {
  case updateError:
    shouldRetry = upErr.shouldRetry
  case unexpectedContentTypeError:
    // curl -A "gitglob/0.0.1" -v -X GET \
    // https://github.com/aschmitz/404.git/info/refs?service=git-upload-pack
    switch upErr.resp.StatusCode {
    case 401:
      // This is saying we don't have permission to get the repository. That
      // could mean a DMCA request (in which case the body will have more
      // info), a deleted repository, a private repository, or a repository
      // that never existed.
      shouldRetry = false
    default:
      panic(upErr.Error())
    }
  case *net.OpError:
    switch upErr.Err.Error() {
    case syscall.ECONNRESET.Error():
      // We just had the connection unexpectedly reset. That *generally*
      // doesn't disqualify us from trying again, but we should make sure
      // that we don't just hammer sites that are sending connection resets
      // because of load or blocking, ideally.
      shouldRetry = true
      shouldForceFull = false
      errorName = "conn_reset"
    default:
      // An unknown network error, so we'll panic.
      panic(upErr.Error())
    }
  default:
    if upErr == ZeroLengthPackfile {
      // We didn't get any response from the git server on the other end: this
      // might happen because we requested an object that no longer exists, or
      // possibly(?) because we claimed to have an object that no longer
      // exists. Queue another update of this repo, not using any known
      // objects.
      
      // TODO: Determine whether this can happen when claiming to have unknown
      // objects. If it does, we'll definitely need to not claim objects from
      // branches that have been deleted.
      shouldRetry = true
      // shouldForceFull = true
      // Just basing our next pull on up-to-date refs should work
      shouldForceFull = false
      errorName = "empty_packfile"
    } else if upErr == globpack.BadPackfileChecksumError {
      // We received a packfile with a bad checksum. This probably happened for
      // one of two reasons:
      // 1: The connection dropped before we received the full packfile. This
      //    is a clear case where retrying is fine.
      // 2: The server failed when reading the packfile. We assume this happens
      //    when the repo is being updated, but it's difficult to tell.
      
      // A log from the second case:
      // Stdout from git server: fatal: unable to read \
      // 66b5a57133282e153bc4b3436c0f9a246edc121a
      // Error from git server: aborting due to possible repository \
      // corruption on the remote side.
      // Pack length: 103841792
      // panic: bad packfile checksum
      
      // In the second case, it's *possible* that this is not a transient
      // error, so perhaps we should have some backoff here and eventually
      // assume the repository has been corrupted. If this ever occurs, we'll
      // need to handle it with more grace than we do now.
      
      // For now, just assume an immediate retry of the exact same thing is
      // fine.
      shouldRetry = true
      shouldForceFull = false
      errorName = "bad_checksum"
    } else {
      panic(upErr.Error())
    }
  }
  
  // Note the error in our database
  var error_streak int
  err := dbConn.QueryRow(saveRepoErrorQuery, repoId, upErr.Error()).Scan(
    &error_streak)
  if err != nil {
    panic(err.Error())
  }
  
  // Note the error in InfluxDB
  influxWritePoint("errors", map[string]string{
    "class": "update",
    "name": errorName,
  }, map[string]interface{}{
    "value": 1,
  })
  
  if shouldRetry {
    // Queue the appropriate retry.
    if shouldForceFull {
      rows, err := dbConn.Query(queueRepoUpdateForceFullQuery, repoId)
      rows.Close()
      if err != nil {
        panic(err.Error())
      }
    } else {
      rows, err := dbConn.Query(queueRepoUpdateQuery, repoId)
      rows.Close()
      if err != nil {
        panic(err.Error())
      }
    }
    
    fmt.Println("Update error: '"+upErr.Error()+"', pausing a second.")
    time.Sleep(time.Second)
    
    return nil
  } else {
    // We're not going to bother retrying this repository in the future at
    // this point, so mark it as disabled.
    rows, err := dbConn.Query(disableRepoQuery, repoId)
    rows.Close()
    if err != nil {
      panic(err.Error())
    }
    
    return nil
  }
}

func readPackQueueLoop() {
    for {
    // Fetch the oldest queue entry and mark it as being in progress.
    res, err := r.DB("gitglob").Table("queued_packs").Filter(
      r.Row.Field("in_progress").Default(false).Eq(false)).
        OrderBy("queue_time").
        Limit(1).
        Update(r.Branch(r.Row.Field("in_progress").Default(false).Eq(false),
          map[string]interface{} {
            "in_progress": true,
            "start_time": r.Now(),
            },
          r.Error("Queue entry is taken.")), r.UpdateOpts {
            ReturnChanges: true,
          }).RunWrite(rSession)
    noWork := true
    if err != nil {
      fmt.Println("Error checking for work:", err.Error())
    } else if len(res.Changes) > 0 {
      noWork = false
    }
    
    if noWork {
      fmt.Println("No packs to load.\n")
      time.Sleep(time.Second)
    } else {
      packDetails := res.Changes[0].NewValue.(map[string]interface{})
      packFilename := packDetails["id"].(string)
      repoPath := packDetails["repo_path"].(string)
      repoId := int(packDetails["repo_id"].(float64))
      packFullPath := storagePath+"/gitpack/"+packFilename
      
fmt.Printf("Will read: %+v\n", packFullPath)
      
      // TODO: This should probably be an mmap instead.
      // https://github.com/edsrzf/mmap-go looks reasonable.
      packfileContents, err := ioutil.ReadFile(packFullPath); if err != nil {
        panic(err.Error())
      }
      err = globpack.LoadPackfile(packfileContents, repoPath); if err != nil {
        if err == globpack.CouldntResolveExternalDeltasError {
          rows, err := dbConn.Query(queueRepoUpdateForceFullQuery, repoId)
          rows.Close()
          if err != nil {
            panic(err.Error())
          }
        } else {
          panic(err.Error())
        }
      }
      forceGC()
      
      // Delete the pack from disk.
      err = os.Remove(packFullPath); if err != nil {
        panic(err.Error())
      }
      
      // Delete the pack from the list of packs that need to be read.
      res, err = r.DB("gitglob").Table("queued_packs").Get(packDetails["id"]).
        Delete().RunWrite(rSession)
      if err != nil {
        panic(err.Error())
      }
    }
  }
}

func readUpdateQueueLoop() {
  initRefpackWriting()
  var repoId int
  var forceFull bool
  var repoPath string
  
  for {
    // Fetch the oldest queue entry and mark it as being in progress.
    err := dbConn.QueryRow(getNextRepoQuery).Scan(&repoId, &repoPath,
      &forceFull)
    if err == sql.ErrNoRows {
      fmt.Println("No repos to check:", err.Error())
      time.Sleep(time.Second)
      continue
    } else if err != nil {
      panic(err.Error())
    }
    
fmt.Printf("Will update: %+v\n", repoPath)
    err = doUpdateRepoRefs(repoId, repoPath, forceFull); if err != nil {
      panic(err.Error())
    }
    forceGC()
    
    // Update the row: remove it if there isn't already a new update queued,
    //  or mark it not in progress if there is.
    rows, err := dbConn.Query(markRepoUpdatedQuery, repoId)
    rows.Close()
    if err != nil {
      panic(err.Error())
    }
  }
}

func main() {
  runtime.GOMAXPROCS(runtime.NumCPU())
  rand.Seed(time.Now().UTC().UnixNano())
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
  
  cleanupChan := make(chan os.Signal, 1)
  signal.Notify(cleanupChan, os.Interrupt)
  signal.Notify(cleanupChan, syscall.SIGTERM)
  go func() {
      <-cleanupChan
      globpack.CloseGlobpackWriter()
      CloseRefpackWriter()
      os.Exit(1)
  }()
  
  influxUrl, err := url.Parse("http://localhost:8086"); if err != nil {
    panic(err.Error())
  }
  influxdbClient, err = influxdb.NewClient(influxdb.Config{
      URL: *influxUrl,
  }); if err != nil {
    panic(err.Error())
  }
  
  var queue string
  var just_pack string
  
  flag.StringVar(&just_pack, "just_pack", "", "Just read one packfile.")
  flag.StringVar(&queue, "queue", "", "The queue to watch: 'update' or"+
    " 'pack'.")
  flag.Parse()
  
  // packfileContents, err := ioutil.ReadFile("output/gitpack/3517e339c11915790121fedb21f287b19a996c11.pack"); if err != nil {
  // packfileContents, err := ioutil.ReadFile("output/gitpack/2ed7c13416730ef1803402e48c351d324e98d1b3.pack"); if err != nil {
  //   panic(err.Error())
  // }
// go func() {
//   time.Sleep((3*60+30)*time.Second)
//   f, err := os.Create("readpack.mprof")
//   if err != nil {
//     panic(err.Error())
//   }
//   pprof.WriteHeapProfile(f)
//   f.Close()
// }()
  // err = globpack.LoadPackfile(packfileContents, "http://localhost/testpack.pack"); if err != nil {
  //   panic(err.Error())
  // }
  // return
  
  // if true {
  //   err = doUpdateRepoRefs("http://localhost:12345/foo/bar.git", true); if err != nil {
  //     fmt.Printf("%+v\n", err)
  //   }
  //   return
  // }
  
  if just_pack != "" {
    packfileContents, err := ioutil.ReadFile(just_pack); if err != nil {
      panic(err.Error())
    }
    err = globpack.LoadPackfile(packfileContents, "http://localhost/testpack.pack"); if err != nil {
      panic(err.Error())
    }
    return
  }
  
  switch queue {
  case "update":
    readUpdateQueueLoop()
  case "pack":
    readPackQueueLoop()
  default:
    flag.PrintDefaults()
  }
}
