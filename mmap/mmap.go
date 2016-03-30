/*
Copyright (c) 2015 The Go Authors. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

   * Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
   * Redistributions in binary form must reproduce the above
copyright notice, this list of conditions and the following disclaimer
in the documentation and/or other materials provided with the
distribution.
   * Neither the name of Google Inc. nor the names of its
contributors may be used to endorse or promote products derived from
this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

// Borrowed from https://github.com/golang/exp/blob/master/mmap/mmap_linux.go
// at commit eb7c1fa4d21cad64754b3d847ab08550c4c68529

package mmap

import (
  "fmt"
  "os"
  "runtime"
  "syscall"
)

type MmappedFile struct {
  Data []byte
}

// Close closes the reader.
func (r *MmappedFile) Close() error {
  if r.Data == nil {
    return nil
  }
  data := r.Data
  r.Data = nil
  runtime.SetFinalizer(r, nil)
  return syscall.Munmap(data)
}

// AdviseSequential uses madvise to tell the system of upcoming sequential reads
func (r *MmappedFile) AdviseSequential() error {
  return syscall.Madvise(r.Data, syscall.MADV_SEQUENTIAL)
}

// open memory-maps the named file for reading.
func Open(filename string) (*MmappedFile, error) {
  f, err := os.Open(filename)
  if err != nil {
    return nil, err
  }
  defer f.Close()
  fi, err := f.Stat()
  if err != nil {
    return nil, err
  }
  
  size := fi.Size()
  if size == 0 {
    return &MmappedFile{}, nil
  }
  if size < 0 {
    return nil, fmt.Errorf("mmap: file %q has negative size", filename)
  }
  if size != int64(int(size)) {
    return nil, fmt.Errorf("mmap: file %q is too large", filename)
  }

  data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
  if err != nil {
    return nil, err
  }
  r := &MmappedFile{data}
  runtime.SetFinalizer(r, (*MmappedFile).Close)
  return r, nil
}
