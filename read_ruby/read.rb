# encoding: BINARY
require 'rubygems'
require 'hexdump'
require 'zlib'

Encoding.default_external = 'BINARY'

marked_object = '78afc6473f30ba4ee28b4f04655cbb6b67456607'
pack = File.open('file.globpack')

header = pack.read(52)
pos = 52

deltas = 0
compressed = 0
objects = {}
while not pack.eof?
  objpos = pos
  hash = pack.read(20).unpack('H40')[0]
  type = pack.readbyte
  pos += 21
  if type & 0xe0 > 0
    raise "Unexpected type: #{type}"
  end
  if type & 0x08 > 0
    # This is a delta, read a base object too.
    base = pack.read(20)
    pos += 20
    deltas += 1
  end
  if type & 0x10 > 0
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
    byte = pack.readbyte
    continue = byte & 0x80 == 0x80
    length += (byte & 0x7f) << offset
    offset += 7
    pos += 1
  end
  
  # puts "hash: #{hash}, type: #{type}, length: #{length}, loc: #{objpos}"
  objects[hash] = objpos
  
  compressed_data = pack.read(length)
  pos += length
  
  if hash == marked_object
    puts "Marked object (#{hash}):"
    puts "  Type: 0x#{type.chr.unpack('H2')[0]}"
    puts "Compressed data:"
    compressed_data.hexdump
    decompressed_data = Zlib::Inflate.new(-15).inflate(compressed_data)
    puts "Decompressed data:"
    decompressed_data.hexdump
  end
end
pack.close

puts "#{objects.length} objects, #{deltas} deltas, #{compressed} compressed"
