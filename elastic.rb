
require 'rubygems' if RUBY_VERSION < '1.9.0'

require 'date'
require 'time'
require 'json'
require 'net/http'
require 'timeout'

module Sensu::Extension

  class Elastic < Handler

    def name
      'elastic'
    end

    def description
      'output events to Elasticserach'
    end

    def definition
      {
        type: 'extension',
        name: name,
        mutator: 'ruby_hash'
      }
    end

    def options
      return @options if @options
      @options = {
        timeout: 5,
        endpoint: 'http://localhost:9200',
        type: 'sensu',
        index: 'sensu-%Y.%m.%d',
        flush_interval: 2
      }
      if @settings[:elastic].is_a?(Hash)
        @options.merge!(@settings[:elastic])
      end
      @options
    end

    def post_init
      @flush_timer = EM::PeriodicTimer.new(options[:flush_interval]) do
        flush
      end
      @data = []
      @uri = URI.parse("#{options[:endpoint]}/_bulk")
    end

    def flush()
      return if @data.length == 0
      begin
        timeout options[:timeout] do
          req = Net::HTTP::Post.new(@uri)
          req.body = @data.join("\n")
          begin
            res = Net::HTTP.start(@uri.hostname, @uri.port) do |http|
              http.request(req)
            end
          rescue SocketError => e
            @logger.error("elastic: error #{e.message}")
          end
          if not res.is_a?(Net::HTTPSuccess)
            @logger.info("elastic: sent failed #{res.body}")
          end
        end
      rescue Timeout::Error
        @logger.error("elastic: timed out")
      end
      @data = []
    end

    def run(event)
      client = event[:client]
      check = event[:check]
      begin
        event_data = JSON.load(check[:output])
      rescue
        @logger.error("elastic: failed to parse #{check[:output]}")
      end
      if event_data != nil
        metadata = {
          'index' => {
            '_id' => nil,
            '_index' => Time.new.strftime(options[:index]),
            '_type' => options[:type],
            '_routing' => nil
          }
        }
        tags = client[:tags] || []
        tags = tags.concat(check[:tags] || [])
        source = {
          '@version' => '1',
          '@timestamp' => Time.at(check[:issued]).iso8601,
          'sensuclient' => client[:name],
          'tags' => tags,
          'type' => options[:type]
        }
        source = event_data.merge(source)
        @data << JSON.dump(metadata)
        @data << JSON.dump(source)
        yield 'elastic: handler finished', 0
      end
    end

  end
end

