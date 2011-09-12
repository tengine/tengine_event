# -*- coding: utf-8 -*-
require 'tengine/mq'

require 'active_support/core_ext/hash/keys'
require 'active_support/memoizable'
require 'amqp'

class Tengine::Mq::Suite
  # memoize については http://wota.jp/ac/?date=20081025#p11 などを参照してください
  extend ActiveSupport::Memoizable

  attr_reader :config

  def initialize(config = {})
    c = (config || {}).symbolize_keys
    @config = [:connection, :exchange, :queue].inject({}) do |d, key|
      d[key] = DEFAULT_CONFIG[key].merge((c[key] || {}).symbolize_keys)
      d
    end
  end

  DEFAULT_CONFIG= {
    :connection => {
      :user => 'guest',
      :pass => 'guest',
      :vhost => '/',
      # :timeout => nil,
      :logging => false,
      :insist => false,
      :host => 'localhost',
      :port => 5672,
    }.freeze,

    :exchange => {
      :name => "tengine_event_exchange",
      :type => 'direct',
      :passive => false,
      :durable => true,
      :auto_delete => false,
      :internal => false,
      :nowait => true,
    },

    :queue => {
      :name => "tengine_event_queue",
      :passive => false,
      :durable => true,
      :auto_delete => false,
      :exclusive => false,
      :nowait => true,
      :subscribe => {
        :ack => true,
        :nowait => true,
        :confirm => nil,
      },
    }
  }

  def connection
    AMQP.connect(config[:connection])
  end

  def channel
    AMQP::Channel.new(connection, :prefetch => 1)
  end

  def exchange
    c = config[:exchange]
    exchange_type = c.delete(:type)
    exchange_name = c.delete(:name)
    AMQP::Exchange.new(channel, exchange_type, exchange_name, c)
  end

  def queue
    c = config[:queue]
    queue_name = c.delete(:name)
    queue = AMQP::Queue.new(channel, queue_name, c)
    queue.bind(exchange)
    queue
  end
  memoize :connection, :channel, :exchange, :queue

end
