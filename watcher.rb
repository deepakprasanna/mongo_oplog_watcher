require "singleton"
require "mongo"
include Mongo 

class MongoSandBoxOplog
  include Singleton
  attr_accessor :tail
  def initialize
    @connection = Mongo::Connection.new("localhost", 27017)
    @db = @connection.db "local"
    @collection = @db["oplog.$main"]
    @tail = Cursor.new(@collection, :tailable => true, :order => [['$natural', 1]])
  end
end

class MongoReplSet
  include Singleton
  attr_accessor :cursor
  def initialize
    @connection = Mongo::ReplSetConnection.new(["localhost:27018","localhost:27019","localhost:27020"]) 
    @db = @connection.db "vimana-sandbox-dup"
    @cursor = @db["utilization.metrics.cycledowntime"]
  end

  def insert(oplog_record)
    return unless oplog_record["op"] == "i"
    return unless oplog_record["ns"] == "vimana-sandbox.utilization.metrics.cycledowntime" 
    begin 
      id = @cursor.insert(oplog_record["o"])
      puts "Im listening the oplog and I just inserted #{id} into the replica."
    rescue Mongo::ConnectionFailure
      puts "Oops, He is dead Jim."
    end 
  end
end

while true
  tail = MongoSandBoxOplog.instance.tail
  MongoReplSet.instance.insert(tail.next) if tail.has_next?
end  
