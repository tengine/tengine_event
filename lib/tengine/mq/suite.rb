# -*- coding: utf-8 -*-
require 'tengine/mq'

require 'active_support/core_ext/hash/keys'
require 'active_support/memoizable'
require 'amqp'

class Tengine::Mq::Suite
  # memoize については http://wota.jp/ac/?date=20081025#p11 などを参照してください
  extend ActiveSupport::Memoizable

  attr_reader :config
  attr_reader :auto_reconnect_delay

  def initialize(config = {})
    c = (config || {}).symbolize_keys
    @config = [:sender, :connection, :exchange, :queue].inject({}) do |d, key|
      d[key] = DEFAULT_CONFIG[key].merge((c[key] || {}).symbolize_keys)
      d
    end
    @auto_reconnect_delay = @config[:connection].delete(:auto_reconnect_delay)
    # 一度も、AMQP.connectを実行する前に、connection に関する例外が発生すると、
    # 再接続などのハンドリングができないので、初期化時に connection への接続までを行います。
    connection
  end

  DEFAULT_CONFIG= {
    :sender => {
      :keep_connection => false,
      :retry_interval => 1,  # in seconds
      :retry_count => 30,
    }.freeze,

    :connection => {
      :user => 'guest',
      :pass => 'guest',
      :vhost => '/',
      # :timeout => nil,
      :logging => false,
      :insist => false,
      :host => 'localhost',
      :port => 5672,
      :auto_reconnect_delay => 1, # in seconds
    }.freeze,

    :exchange => {
      :name => "tengine_event_exchange",
      :type => 'direct',
      :passive => false,
      :durable => true,
      :auto_delete => false,
      :internal => false,
      :nowait => true,
      :publish => {
        :persistent => true,
      },
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

  def connection(&block)
    result = AMQP.connect(config[:connection], &block)
    unless auto_reconnect_delay.nil?
      result.on_tcp_connection_loss do |conn, settings|
        conn.reconnect(false, auto_reconnect_delay.to_i)
      end
      result.after_recovery do |conn, settings|
        reset_channel
      end
    end
    result
  end

  def channel
    options = {
      :prefetch => 1,
      :auto_recovery => !auto_reconnect_delay.nil?,
    }
    AMQP::Channel.new(connection, options)
  end

  def exchange
    c = config[:exchange].dup
    c.delete(:publish)
    exchange_type = c.delete(:type)
    exchange_name = c.delete(:name)
    AMQP::Exchange.new(channel, exchange_type, exchange_name, c)
  end

  def queue
    c = config[:queue].dup
    queue_name = c.delete(:name)
    queue = AMQP::Queue.new(channel, queue_name, c)
    queue.bind(exchange)
    queue
  end
  memoize :connection, :channel, :exchange, :queue

  def reset_channel
    channel(true)
    exchange(true)
    queue(true)
  end


end
