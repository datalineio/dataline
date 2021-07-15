require_relative './airbyte_protocol.rb'
require_relative './airbyte_logger.rb'

require_relative './mongodb_stream.rb'
require_relative './mongodb_reader.rb'
require_relative './mongodb_state.rb'

class MongodbSource
  def spec
    spec = JSON.parse(File.read(__dir__ + '/spec.json'))

    message =  AirbyteMessage.from_dynamic!({
      'type' => Type::Spec,
      'spec' => spec,
    })

    puts message.to_json
  end

  def check(config:)
    @config = JSON.parse(File.read(config))

    result = begin
               client.collections.first.find.limit(1).first
               {'status' => Status::Succeeded}
             rescue Exception => e
               AirbyteLogger.log(e.backtrace.join("\n"), Level::Fatal)
               {'status' => Status::Failed, 'message' => 'Authentication failed.'}
             end

    message =  AirbyteMessage.from_dynamic!({
      'type' => Type::ConnectionStatus,
      'connectionStatus' => result,
    })

    puts message.to_json
  end

  def discover(config:)
    @config = JSON.parse(File.read(config))

    streams = client.collections.map do |collection|
      AirbyteLogger.log("Discovering stream #{collection.name}")
      MongodbStream.new(collection: collection).discover
    end

    catalog = AirbyteCatalog.from_dynamic!({
      'streams' => streams,
    })

    puts AirbyteMessage.from_dynamic!({
      'type' => Type::Catalog,
      'catalog' => catalog.to_dynamic
    }).to_json
  end

  def read(config:, catalog:, state: nil)
    @config = JSON.parse(File.read(config))
    @catalog = JSON.parse(File.read(catalog))
    @state = MongodbState.new(state_file: state)

    MongodbReader.new(client: client, catalog: @catalog, state: @state).read
  end

  def method_missing(m, *args, &block)
    AirbyteLogger.log("There's no method called #{m}", Level::Fatal)
  end

  private

  def client
    @client ||= begin
                  uri = "mongodb+srv://#{@config['user']}:#{@config['password']}@#{@config['host']}/#{@config['database']}?authSource=#{@config['auth_source']}"
                  if !@config.fetch(:"replica_set", "").strip.empty?
                    uri += "&replicaSet=#{@config['replica_set']}&ssl=true"
                  elsif ["true", true].include?(@config.fetch("ssl", false))
                    uri += "&ssl=true"
                  end
                  @client = Mongo::Client.new(uri)
                  @client.logger.formatter = AirbyteLogger.logger_formatter
                  @client
                end
  end
end
