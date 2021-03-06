# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"

require "pathname"
require "socket" # for Socket.gethostname
require_relative "binarytail"

# Stream events from files.
#
# By default, each event is assumed to be one line. If you would like
# to join multiple log lines into one event, you'll want to use the
# multiline codec.
#
# Files are followed in a manner similar to `tail -0F`. File rotation
# is detected and handled by this input.
class LogStash::Inputs::Utmp < LogStash::Inputs::Base
  config_name "utmp"

  # TODO(sissel): This should switch to use the `line` codec by default
  # once file following
  default :codec, "plain"

  # The path(s) to the file(s) to use as an input.
  # You can use globs here, such as `/var/log/*.log`
  # Paths must be absolute and cannot be relative.
  #
  # You may also configure multiple paths. See an example
  # on the <<array,Logstash configuration page>>.
  config :path, :validate => :array, :required => true

  # Exclusions (matched against the filename, not full path). Globs
  # are valid here, too. For example, if you have
  # [source,ruby]
  #     path => "/var/log/*"
  #
  # You might want to exclude gzipped files:
  # [source,ruby]
  #     exclude => "*.gz"
  config :exclude, :validate => :array

  # How often (in seconds) we stat files to see if they have been modified.
  # Increasing this interval will decrease the number of system calls we make,
  # but increase the time to detect new log lines.
  config :stat_interval, :validate => :number, :default => 1

  # How often (in seconds) we expand globs to discover new files to watch.
  config :discover_interval, :validate => :number, :default => 15

  # Path of the sincedb database file (keeps track of the current
  # position of monitored log files) that will be written to disk.
  # The default will write sincedb files to some path matching `$HOME/.sincedb*`
  # NOTE: it must be a file path and not a directory path
  config :sincedb_path, :validate => :string

  # How often (in seconds) to write a since database with the current position of
  # monitored log files.
  config :sincedb_write_interval, :validate => :number, :default => 15

  # Choose where Logstash starts initially reading files: at the beginning or
  # at the end. The default behavior treats files like live streams and thus
  # starts at the end. If you have old data you want to import, set this
  # to 'beginning'
  #
  # This option only modifies "first contact" situations where a file is new
  # and not seen before. If a file has already been seen before, this option
  # has no effect.
  config :start_position, :validate => [ "beginning", "end"], :default => "end"

  # set the new line delimiter, defaults to "\n"
  config :delimiter, :validate => :string, :default => "\n"

  #set block size
  config :struct_size, :validate => :number, :default => 384
  #set format
  config :struct_format, :validate => :string, :default =>""

  public
  def register
    require "addressable/uri"
    require "filewatch/tail"
    require "digest/md5"
    @logger.info("Registering utmp input", :path => @path)
    @host = Socket.gethostname.force_encoding(Encoding::UTF_8)

    @tail_config = {
      :exclude => @exclude,
      :stat_interval => @stat_interval,
      :discover_interval => @discover_interval,
      :sincedb_write_interval => @sincedb_write_interval,
      :delimiter => @delimiter,
      :logger => @logger,
    }

    @path.each do |path|
      if Pathname.new(path).relative?
        raise ArgumentError.new("File paths must be absolute, relative path specified: #{path}")
      end
    end

    if @sincedb_path.nil?
      if ENV["SINCEDB_DIR"].nil? && ENV["HOME"].nil?
        @logger.error("No SINCEDB_DIR or HOME environment variable set, I don't know where " \
                      "to keep track of the files I'm watching. Either set " \
                      "HOME or SINCEDB_DIR in your environment, or set sincedb_path in " \
                      "in your Logstash config for the file input with " \
                      "path '#{@path.inspect}'")
        raise # TODO(sissel): HOW DO I FAIL PROPERLY YO
      end

      #pick SINCEDB_DIR if available, otherwise use HOME
      sincedb_dir = ENV["SINCEDB_DIR"] || ENV["HOME"]

      # Join by ',' to make it easy for folks to know their own sincedb
      # generated path (vs, say, inspecting the @path array)
      @sincedb_path = File.join(sincedb_dir, ".sincedb_" + Digest::MD5.hexdigest(@path.join(",")))

      # Migrate any old .sincedb to the new file (this is for version <=1.1.1 compatibility)
      old_sincedb = File.join(sincedb_dir, ".sincedb")
      if File.exists?(old_sincedb)
        @logger.info("Renaming old ~/.sincedb to new one", :old => old_sincedb,
                     :new => @sincedb_path)
        File.rename(old_sincedb, @sincedb_path)
      end

      @logger.info("No sincedb_path set, generating one based on the file path",
                   :sincedb_path => @sincedb_path, :path => @path)
    end

    if File.directory?(@sincedb_path)
      raise ArgumentError.new("The \"sincedb_path\" argument must point to a file, received a directory: \"#{@sincedb_path}\"")
    end

    @tail_config[:sincedb_path] = @sincedb_path

    if @start_position == "beginning"
      @tail_config[:start_new_files_at] = :beginning
    end
  end # def register

  public
  def run(queue)
    #@tail = FileWatch::Tail.new(@tail_config)
    @tail = FixBlockTail.new(@tail_config)
    @tail.logger = @logger
    struct_size = @struct_size
    @tail.setblocksize(struct_size)
    @path.each { |path| @tail.tail(path) }

    #residualstr=""
    #residualnum=0
    @tail.subscribe do |path, line|
      @logger.debug? && @logger.debug("Received line", :path => path, :textsize => line.size, :text => line)
#      textsize = line.size
#      if textsize + residualnum < struct_size then
#        residualstr += line
#        residualnum += textsize
#        next
#      else
#        struct_bin = residualnum + line[0, struct_size-residualnum]
#        struct = struct_bin.unpack("s2Ia32a4a32a256s2iI2I4a20")
#        #event["struct"] = struct.join("|")
#        line = line[struct_size-residualnum,textsize]
#        struct_num = (textsize - struct_size + residualnum)/struct_size
#        residualnum = (textsize - struct_size + residualnum)%struct_size
#        i=1
#        while i<=struct_num do
#          struct = line[i*struct_size,i*struct_size+struct_size].unpack("s2Ia32a4a32a256s2iI2I4a20").join("|")         
#          
#          i += 1
#        end
#        
#      end
      #@codec.decode(line) do |event|
      #line do |event|
      event = LogStash::Event.new
      event["[@metadata][path]"] = path
      event["host"] = @host if !event.include?("host")
      event["path"] = path if !event.include?("path")
      #event["struct"] = line.unpack("s2Ia32a4a32a256s2iI2I4a20").join("|")
      struct = line.unpack(@struct_format);
      event["message"] = struct.join("|")
#        event["ut_type"] = struct[0].to_s;
#        event["ut_pid"] = struct[2].to_s;
#        event["ut_line"] = struct[3];
#        event["ut_id"] = struct[4];
#        event["ut_user"] = struct[5];
#        event["ut_host"] = struct[6];
#        event["e_termination"] = struct[7].to_s;
#        event["e_exit"] = struct[8].to_s;
#        event["ut_session"] = struct[9].to_s;
#        event["tv_sec"] = struct[10].to_s;
#        event["tv_usec"] = struct[11].to_s;
#        event["ut_ip"] = struct[12].to_s+'.'+struct[13].to_s+'.'+struct[14].to_s+'.'+struct[15].to_s
        
        decorate(event)
        queue << event
      #end
    end
    finished
  end # def run

  public
  def teardown
    if @tail
      @tail.sincedb_write
      @tail.quit
      @tail = nil
    end
  end # def teardown
end # class LogStash::Inputs::File
