package main

import (
  "bytes"
  "encoding/binary"
  "encoding/hex"
  "errors"
  "flag"
  "fmt"
  "io"
  "io/ioutil"
  "math/rand"
  "net/http"
  "net/url"
  "os"
  "os/signal"
  "syscall"
  "runtime"
// "runtime/pprof"
  "strings"
  "time"
  
  "gitlab.lardbucket.org/aschmitz/gitglob/globpack"
  influxdb "github.com/influxdb/influxdb/client"
  r "github.com/dancannon/gorethink"
)

var rSession *r.Session

const (
  hashLen = 20
  RefDiffTypeAbsolute = 1
  RefDiffTypeDelta = 2
  maxRefDepth = 100
  storagePath = "output"
)

type refDiffs struct {
  Type uint8
  From int64
  NewRefs map[string][]byte
  ChangedRefs map[string][]byte
  DeletedRefs []string
}

type commitFetchJob struct {
  Id string
  RepoAddress string
  ToRequest [][hashLen]byte
  BaseCommits [][hashLen]byte
  From int64
}

var ZeroLengthPackfile = errors.New("zero length packfile")

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
      panic(err.Error())
    }
  }
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

func HaveCommit(commithash [hashLen]byte) (bool, error) {
  objloc, err := globpack.LookupObjLocation(commithash); if err != nil {
    return false, err
  }
  
  return objloc.Existed, nil
}

func DownloadNewCommits(repoPath string, baseCommits [][hashLen]byte,
  newCommits [][hashLen]byte) (string, error) {
  fmt.Println("Considering", len(newCommits), "commit(s)")
  neededCommits := make([]string, len(newCommits))
  addedCommits := 0
  for _, commithash := range newCommits {
    exists, err := HaveCommit(commithash); if err != nil {
      return "", err
    }
    
    if (!exists) {
      neededCommits[addedCommits] = hex.EncodeToString(commithash[:])
      addedCommits += 1
    }
  }
  fmt.Println("Need", addedCommits, "commit(s)")
  
  if addedCommits == 0 {
    return "", nil
  }
  
  // multi_ack_detailed might be useful, but we don't process it right now.
  // thin-pack is required to allow received packfiles to reference objects we
  // already have.
  // side-band-64k allows sending sideband packets of >1k at a time.
  request, err := BuildPktLine([]byte("want " + neededCommits[0] +
    " thin-pack side-band-64k agent=gitglob/0.0.1\n"))
  if err != nil {
    return "", err
  }
  for _, commithash := range neededCommits[1:addedCommits] {
    nextLine, err := BuildPktLine([]byte("want "+commithash+"\n"))
    if err != nil {
      return "", err
    }
    request = append(request, nextLine...)
  }
  
  // Although not particularly documented in the smart-http docs, it seems to
  // be necessary to send an empty line *before* the "have" list.
  nextLine, err := BuildPktLine(nil)
  request = append(request, nextLine...)
  
  for _, commithash := range baseCommits {
    nextLine, err := BuildPktLine([]byte("have "+
      hex.EncodeToString(commithash[:])+"\n")); if err != nil {
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
    panic("failed to connect: " + err.Error())
  }
  
  if resp.Header["Content-Type"][0] !=
      "application/x-git-upload-pack-result" {
    panic("Unexpected Content-Type: "+resp.Header["Content-Type"][0])
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

func doUpdateRepoRefs(repoPath string, forceFull bool) error {
  timestamp := time.Now().UTC()
  client := &http.Client{}
  req, err := http.NewRequest("GET", repoPath+
    "/info/refs?service=git-upload-pack", nil)
  req.Header.Add("User-Agent", "gitglob/0.0.1")
  req.Header.Add("Pragma", "no-cache")
  resp, err := client.Do(req)
  if err != nil {
    return errors.New("failed to connect: " + err.Error())
  }
  defer resp.Body.Close()
  
  if resp.Header["Content-Type"][0] !=
      "application/x-git-upload-pack-advertisement" {
    return errors.New("Unexpected Content-Type: "+
      resp.Header["Content-Type"][0])
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
  
  newRefDiffs, oldRefs, err := RecordRepoRefs(repoPath, timestamp, refs)
  if err != nil {
    return err
  }
  
  // Figure out which commit hashes are new, and which are old.
  var newHashes [][hashLen]byte
  var oldHashes [][hashLen]byte
  newHashMap := make(map[[hashLen]byte]bool)
  oldHashMap := make(map[[hashLen]byte]bool)
  
  // Do we need to download everything anyway?
  // (This could happen when we failed to fetch an external delta base in an
  // earlier update)
  if forceFull == true {
    for _, hash := range refs {
      newHashMap[sliceToHashArray(hash)] = true
    }
  } else {
    // Start with the old ones, as we know we have them.
    for _, hash := range oldRefs {
      oldHashMap[sliceToHashArray(hash)] = true
    }
    for hash, _ := range oldHashMap {
      oldHashes = append(oldHashes, hash)
    }
    // Then see which ones are newly referenced.
    for _, hash := range newRefDiffs.NewRefs {
      newHashMap[sliceToHashArray(hash)] = true
    }
    for _, hash := range newRefDiffs.ChangedRefs {
      newHashMap[sliceToHashArray(hash)] = true
    }
  }
  for hash, _ := range newHashMap {
    // Call the hash new only if we didn't just say we have it.
    // This could happen if, for example, a new tag were created for the current
    // master HEAD commit.
    _, ok := oldHashMap[hash]
    if !ok {
      newHashes = append(newHashes, hash)
    }
  }
  
  fmt.Printf("%d new hashes to fetch, %d old hashes to reference. Based on %d\n",
    len(newHashes), len(oldHashes), newRefDiffs.From)
  
  fmt.Printf("%d base commits, %d commits to request from %s\n",
    len(oldHashes), len(newHashes), repoPath)
  
  // If we have new commits to download, do that.
  if len(newHashes) > 0 {
    filename, err := DownloadNewCommits(repoPath, oldHashes, newHashes)
    if err == nil {
      // Queue processing of this packfile, if we wrote one. The packfile
      // filename may be blank if for some reason we already had all of these
      // commits.
      if filename != "" {
        _, err = r.Db("gitglob").Table("queued_packs").Insert(
          map[string]interface{} {
            "filename": filename,
            "repo_path": repoPath,
            "queue_time": r.Now(),
          }).RunWrite(rSession)
        if err != nil {
          return err
        }
fmt.Printf("Wrote file %s.\n", filename)
      }
      
      // Save the list of refs as the latest one.
      _, err = r.Db("gitglob").Table("refs_latest").Get(repoPath).Replace(
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
      if err == ZeroLengthPackfile {
        // We didn't get any response from the git server on the other end: this
        // might happen because we requested an object that no longer exists, or
        // possibly(?) because we claimed to have an object that no longer
        // exists. Queue another update of this repo, not using any known
        // objects.
        
        // TODO: Determine whether this can happen when claiming to have unknown
        // objects. If it does, we'll definitely need to not claim objects from
        // branches that have been deleted.
        _, err := r.Db("gitglob").Table("queued_updates").Get(repoPath).
          Replace(func(row r.Term) interface{} {
            return r.Branch(row.Eq(nil), map[string]interface{} {
              "id": repoPath,
              "queue_time": r.Now(),
              "oldest_to_grab": r.Now(),
              "force_full": true,
            }, row.Merge(map[string]interface{} {
              "oldest_to_grab": row.Field("oldest_to_grab").Default(r.Now()),
              "force_full": true,
            }))
          }).
        RunWrite(rSession); if err != nil {
          panic(err.Error())
        }
      } else {
        panic(err.Error())
      }
    }
  }
  
  return nil
}

func readPackQueueLoop() {
    for {
    // Fetch the oldest queue entry and mark it as being in progress.
    res, err := r.Db("gitglob").Table("queued_packs").Filter(
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
    if err != nil {
      panic(err.Error())
    }
    if len(res.Changes) != 1 {
      fmt.Println("No packs to load.\n")
      time.Sleep(time.Second)
    } else {
      packDetails := res.Changes[0].NewValue.(map[string]interface{})
      packFilename := packDetails["filename"].(string)
      repoPath := packDetails["repo_path"].(string)
      packFullPath := storagePath+"/gitpack/"+packFilename
      
fmt.Printf("Will read: %+v\n", packFullPath)
      
      // TODO: This should probably be an mmap instead.
      // https://github.com/edsrzf/mmap-go looks reasonable.
      packfileContents, err := ioutil.ReadFile(packFullPath); if err != nil {
        panic(err.Error())
      }
      err = globpack.LoadPackfile(packfileContents, repoPath); if err != nil {
        if err == globpack.CouldntResolveExternalDeltasError {
          _, err := r.Db("gitglob").Table("queued_updates").Get(repoPath).
            Replace(func(row r.Term) interface{} {
              return r.Branch(row.Eq(nil), map[string]interface{} {
                "id": repoPath,
                "queue_time": r.Now(),
                "oldest_to_grab": r.Now(),
                "force_full": true,
              }, row.Merge(map[string]interface{} {
                "oldest_to_grab": row.Field("oldest_to_grab").Default(r.Now()),
                "force_full": true,
              }))
            }).
          RunWrite(rSession); if err != nil {
            panic(err.Error())
          }
        } else {
          panic(err.Error())
        }
      }
      
      // Delete the pack from disk.
      err = os.Remove(packFullPath); if err != nil {
        panic(err.Error())
      }
      
      // Delete the pack from the list of packs that need to be read.
      res, err = r.Db("gitglob").Table("queued_packs").Get(packDetails["id"]).
        Delete().RunWrite(rSession)
      if err != nil {
        panic(err.Error())
      }
    }
  }
}

func readUpdateQueueLoop() {
  initRefpackWriting()
  
  for {
    // Fetch the oldest queue entry and mark it as being in progress.
    res, err := r.Db("gitglob").Table("queued_updates").Filter(
      r.Row.Field("in_progress").Default(false).Eq(false)).
        OrderBy("queue_time").
        Limit(1).
        Replace(r.Branch(r.Row.Field("in_progress").Default(false).Eq(false),
          r.Row.Without("oldest_to_grab").Merge(map[string]interface{} {
            "in_progress": true,
            "start_time": r.Now(),
            }),
          r.Error("Queue entry is taken.")), r.ReplaceOpts {
            ReturnChanges: true,
          }).RunWrite(rSession)
    if err != nil {
      panic(err.Error())
    }
    if len(res.Changes) != 1 {
      fmt.Println("No repos to check.\n")
      time.Sleep(time.Second)
    } else {
      repoDetails := res.Changes[0].NewValue.(map[string]interface{})
      repoPath := repoDetails["id"].(string)
      
      forceFull := false
      if forceVal, ok := repoDetails["force_full"].(bool); ok && forceVal {
        forceFull = true
      }
      
fmt.Printf("Will update: %+v\n", repoPath)
      err = doUpdateRepoRefs(repoPath, forceFull); if err != nil {
        panic(err.Error())
      }
      
      // Update the row: remove it if there isn't already a new update queued,
      //  or mark it not in progress if there is.
      res, err = r.Db("gitglob").Table("queued_updates").Get(repoPath).Replace(
        func(row r.Term) interface{} {
          return r.Branch(row.Field("oldest_to_grab").Default(nil).Eq(nil),
          nil,
          row.Without("in_progress", "start_time").Merge(
            map[string]interface{} {
              "queue_time": row.Field("oldest_to_grab"),
            }))
        }).RunWrite(rSession)
      if err != nil {
        panic(err.Error())
      }
    }
  }
}

func main() {
  runtime.GOMAXPROCS(runtime.NumCPU())
  rand.Seed(time.Now().UTC().UnixNano())
  var err error
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
  
  // To queue an update (JS):
  // var path = 'https://github.com/aschmitz/nepenthes.git';
  // r.db('gitglob').table('queued_updates').get(path).replace(function(row) {
  //   return r.branch(row.eq(null), {
  //     id: path, queue_time: r.now(), oldest_to_grab: r.now()
  //   }, row.merge({
  //     oldest_to_grab: row('oldest_to_grab').default(r.now())
  //   }));
  // })
  
  var queue string
  flag.StringVar(&queue, "queue", "update", "The queue to watch: 'update' or"+
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
  
  switch queue {
  case "update":
    readUpdateQueueLoop()
  case "pack":
    readPackQueueLoop()
  default:
    flag.PrintDefaults()
  }
}
