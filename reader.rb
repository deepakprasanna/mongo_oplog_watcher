require "mongo"
require "singleton"


class MongoReplSet
  include Singleton
  attr_accessor :cursor
  def initialize
    @connection = Mongo::ReplSetConnection.new(["localhost:27018","localhost:27019","localhost:27020"],
                                               :read => :secondary) 
    @db = @connection.db "vimana-sandbox-dup"
    @cursor = @db["utilization.metrics.cycledowntime"]
  end

  def find_one
    begin 
      puts @cursor.find_one
    rescue Mongo::ConnectionFailure
      puts "Oops, He is dead Jim."
    end 
  end
end

while true
  count ||= 0
  sleep 0.2
  MongoReplSet.instance.find_one
  puts count += 1
end
