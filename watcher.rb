require "singleton"
require "mongo"
require "pp"
include Mongo 



class MongoSandBoxOplog
  include Singleton
  attr_accessor :tail
  def initialize
    @connection = Mongo::Connection.new("localhost", 27018)
    @db = @connection.db "local"
    @collection = @db["oplog.$main"]
    @tail = Cursor.new(@collection, :tailable => true, :order => [['$natural', 1]])
  end
end

class MongoUtilties
  include Singleton
  attr_accessor :cursor
  def initialize
    @connection = Mongo::ReplSetConnection.new(["localhost:27018","localhost:27019","localhost:27020"]) 
    @db = @connection.db "vimana-sandbox-dup"
    @cursor = @db["utilization.metrics.cycledowntime"]
  end
end


def tailer(_next)
  return unless _next["op"] == "i"
  return unless _next["ns"] == "vimana-sandbox.utilization.metrics.cycledowntime" 
  pp "inserting #{_next['o']["_id"]} from collection #{_next["ns"]} into utilities"
  MongoUtilties.instance.cursor.insert(_next['o'])
end

while true
  tail = MongoSandBoxOplog.instance.tail
  tailer(tail.next) if tail.has_next?
end  


