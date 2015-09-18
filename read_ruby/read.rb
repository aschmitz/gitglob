# encoding: BINARY
require 'rubygems'
require 'hexdump'
require 'zlib'
require 'digest'
require 'rethinkdb'
include RethinkDB::Shortcuts

conn = r.connect(host: "localhost", port: 28015)

Encoding.default_external = 'BINARY'
STATIC_HEADER = "gpak\x00\x0d\x0a\xa5"
VERSION_NUMBER = "\x00\x00\x00\x01"
EMPTY_TIMESTAMP = "\xff" * 8
EMPTY_CHECKSUM = "\x00" * 32
TYPE_DELTA = 0x08
TYPE_COMPRESSED = 0x10
TYPE_INVALID_MASK = 0xff ^ TYPE_COMPRESSED ^ TYPE_DELTA ^ 0x07

if ARGV.length < 1 || ARGV.length > 2
  puts "Usage: read.rb globpack [hash to output]"
  exit
end
pack = File.open(ARGV[0])
globpack_name = File.basename(ARGV[0])
marked_object = [ARGV[1]].pack('H40')

# Read and verify header (sans checksum, we'll do that later)
header = pack.read(52)
if header.length != 52
  raise 'Unexpectedly short globpack/header'
end
if header[0..7] != STATIC_HEADER
  raise 'Bad globpack static header.'
end
if header[8..11] != VERSION_NUMBER
  raise 'Unknown globpack version.'
end
expected_length = header[12..19].unpack('Q>').first
if File.size(pack) != expected_length
  raise "Incorrect globpack length. (File said #{expected_length}, really was "+
    File.size(pack)+')'
end
expected_checksum = header[20..51]
pos = 52

# Initialize checksum. Note that the checksum is computed with itself and the
# size being masked (each in a different way)
checksum_header = header.dup
checksum_header[12..19] = EMPTY_TIMESTAMP
checksum_header[20..51] = EMPTY_CHECKSUM
checksum_digest = Digest::SHA256.new
checksum_digest.update checksum_header

deltas = 0
compressed = 0
objects = {}
while not pack.eof?
  objpos = pos
  hash_data = pack.read(20)
  checksum_digest.update hash_data
  
  type = pack.read(1)
  checksum_digest.update type
  type = type.unpack('C').first
  pos += 21
  
  if type & TYPE_INVALID_MASK > 0
    raise "Unexpected type: #{type}"
  end
  if type & TYPE_DELTA > 0
    # This is a delta, read a base object too.
    base = pack.read(20)
    checksum_digest.update(base)
    pos += 20
    deltas += 1
  end
  if type & TYPE_COMPRESSED > 0
    compressed += 1
  end
  # if type & 0x07 == 0x01
  #   puts "Commit: #{hash}"
  # end
  
  # Now read the stored data length
  continue = true
  offset = 0
  length = 0
  while continue
    byte = pack.read(1)
    checksum_digest.update(byte)
    byte = byte.unpack('C').first
    continue = byte & 0x80 == 0x80
    length += (byte & 0x7f) << offset
    offset += 7
    pos += 1
  end
  
  objects[hash_data] = objpos
  
  compressed_data = pack.read(length)
  checksum_digest.update(compressed_data)
  pos += length
  
  if hash_data == marked_object
    puts "Marked object (#{hash_data.unpack('H40')[0]}):"
    puts "  Type: 0x#{type.chr.unpack('H2')[0]}"
    if type & TYPE_COMPRESSED
      puts "Compressed data:"
      compressed_data.hexdump
      decompressed_data = Zlib::Inflate.new(-15).inflate(compressed_data)
    else
      decompressed_data = compressed_data
    end
    puts "Decompressed data:"
    decompressed_data.hexdump
  end
end
pack.close

computed_checksum = checksum_digest.digest
if expected_checksum != computed_checksum
  raise 'Checksum mismatch'
end

puts "#{objects.length} objects, #{deltas} deltas, #{compressed} compressed"

to_insert = objects.map{|hash, loc| {
  id: r.binary(hash),
  loc: loc,
  file: globpack_name
}}
res = r.db('gitglob_2').table('objects').insert(to_insert,
  conflict: 'update').run(conn)
if res['errors'] > 0
  raise "RethinkDB Error(s): #{res['errors']} errors. First error:\n"+
    res['first_error']
end
