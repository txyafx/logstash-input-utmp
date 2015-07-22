# encoding: utf-8
require "filewatch/helper"
require "filewatch/buftok"
require "filewatch/watch"
require "filewatch/tail"
require "logstash/inputs/base"
require "logstash/namespace"
require "pathname"
require "socket" # for Socket.gethostname

class FixBlockTail < FileWatch::Tail
  def setblocksize(blocksize)
    @blocksize = blocksize
  end

  private
    def _read_file(path, &block)
      @buffers[path] ||= FileWatch::BufferedTokenizer.new(@opts[:delimiter])
      delimiter_byte_size = @opts[:delimiter].bytesize
      changed = false
      loop do
        begin
          data = @files[path].sysread(@blocksize)
          changed = true
          #@buffers[path].extract(data).each do |line|
          #  yield(path, line)
          #  @sincedb[@statcache[path]] += (line.bytesize + delimiter_byte_size)
          #end
          yield(path, data)
          @sincedb[@statcache[path]] += @blocksize
        rescue Errno::EWOULDBLOCK, Errno::EINTR, EOFError
          break
        end
      end

      if changed
        now = Time.now.to_i
        delta = now - @sincedb_last_write
        if delta >= @opts[:sincedb_write_interval]
          @logger.debug? && @logger.debug("writing sincedb (delta since last write = #{delta})")
          _sincedb_write
          @sincedb_last_write = now
        end
      end
    end # def _read_file
end
