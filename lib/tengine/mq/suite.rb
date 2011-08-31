require 'tengine/mq'

class Tengine::Mq::Suite

  attr_reader :config

  def initialize(config)
    @config = config.symbolize_keys
  end

  def connection
    @connection ||= AMQP.connect(config[:connection].symbolize_keys)
  end

  def channel
    @channel ||= AMQP::Channel.new(connection)
  end

  def exchange
    c = config[:exchange].symbolize_keys
    exchange_type = c.delete(:type)
    exchange_name = c.delete(:name)
    @exchange ||= AMQP::Exchange.new(channel, exchange_type, exchange_name, c)
  end

  def queue
    c = config[:queue].symbolize_keys
    queue_name = c.delete(:name)
    @queue ||= AMQP::Queue.new(channel, queue_name, c)
    @queue.bind(exchange)
    @queue
  end

end
