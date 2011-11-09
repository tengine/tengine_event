# -*- coding: utf-8 -*-
require 'tengine/mq'

require 'active_support/core_ext/hash/keys'
require 'amqp'
require 'amqp/extensions/rabbitmq'

module Enumerable
  def each_next_tick
    raise ArgumentError, "no block given" unless block_given?
    self.reverse.inject(->{}) do |block, obj|
      lambda do
        EM.next_tick do
          yield obj
          block.call
        end
      end
    end.call
  end
end

# MQと到達保証について by @shyouhei on 2nd Nov., 2011.
#
# 端的に言ってAMQPプロトコルにはパケットの到達保証が、ありません。
#
# したがってAMQP::Exchangeを普通に使うだけでは、fireしたイベントがどこまで確実に
# 届くかが保証されません。
#
# この問題に対してRabbitMQは、それ単体では全体の到達保証をしませんが、以下の追加
# 手段を提供してくれています。
#
# * RabbitMQ自体の実装の努力により、RabbitMQのサーバにデータが到達して以降は、
#   RabbitMQが到達性を保証してくれます
#
# * AMQPプロトコルを勝手に拡張していて、RabbitMQのサーバにパケットが届いたことを
#   ackしてくれるようにできます
#
# したがってAMQPブローカーにRabbitMQを使っている限りは、MQサーバにパケットが到着
# したことを、クライアント側でackを読みながら確認することで、全体としての到達保
# 証が可能になるわけです。
#
# Tengine::Event::Sender#fireを実行すると、イベントを送信して、このackを確認す
# る部分までを自動的に行います。したがって所謂fire-and-forgetの動作が達成されて
# います。ただし、以下のように制限があります
#
# * AMQP gemの制約上、おおむね非同期的に動作します。つまり、fireは送信を予約する
#   だけで、実のところ送信が終わるのは(再送等で)fireが終了してからだいぶ先の話に
#   なります。
#
# * あるときだれかが EM.stop すると、それ以上はackを読めなくなり、再送信ができな
#   くなります。
#
# * そうは言ってもEM.stopできないとプロセスが終了できないので、stop可能かどうか
#   を調査できるようにしました(新API)。
#
#   * fireメソッドの戻り値のTengine::Eventに新メソッド #transmitted? が追加になっ
#     ていますので個別のイベントの送信が終わったかどうかはこれで確認できます。
#
#   * senderが送信中のイベント一覧は sender.pending_events で入手できます
#
#   * もうsenderが送り終わったらそのままEM.stopしてよい場合(だいたいそうだと思
#     いますが)のために、 sender.stop_after_transmission があります
#
#   APIは今後も使い勝手のために追加する可能性があります

class Tengine::Mq::Suite

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
    @tx_pending_events = nil
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

  def connection(force = false, &block)
    if @connection.nil? || force
      @connection = AMQP.connect(config[:connection], &block)
      unless auto_reconnect_delay.nil?
        @connection.on_tcp_connection_loss do |conn, settings|
          conn.reconnect(false, auto_reconnect_delay.to_i)
        end
        @connection.after_recovery do |conn, settings|
          reset_channel
        end
        @connection.on_closed do
          @connection = nil
          reset_channel
        end
      end
    end
    @connection
  end

  def channel(force = false)
    if @channel.nil? || force
      options = {
        :prefetch => 1,
        :auto_recovery => !auto_reconnect_delay.nil?,
      }
      @channel = AMQP::Channel.new(connection, options)
    end
    @channel
  end

  def exchange(force = false)
    if @exchange.nil? || force
      c = config[:exchange].dup
      c.delete(:publish)
      exchange_type = c.delete(:type)
      exchange_name = c.delete(:name)
      @exchange = AMQP::Exchange.new(channel, exchange_type, exchange_name, c)
    end
    @exchange
  end

  def queue(force = false)
    if @queue.nil? || force
      c = config[:queue].dup
      queue_name = c.delete(:name)
      @queue = AMQP::Queue.new(channel, queue_name, c)
      @queue.bind(exchange)
    end
    @queue
  end

  def reset_channel
    @channel = nil
    @exchange = nil
    @queue = nil
    @tx_pending_events = nil
    @publisher_confirmation_initiated = false
  end

  def fire sender, event, opts, retryc, block # :nodoc:
    ensure_publisher_confirmation do
      begin
        exchange.publish event.to_json, @config[:exchange][:publish]
      rescue => e
        # exchange.publish はたとえば RuntimeError を raise したりするようだ
        if retryc >= opts[:retry_count]
          raise ::Tengine::Event::Sender::RetryError.new(event, retryc, e)
        else
          EM.add_timer opts[:retry_interval] do
            fire sender, event, opts, retryc + 1, block
          end
        end
      else
        if @tx_pending_events
          @tag += 1
          e = @@tx_pending_event.new @tag, sender, event, opts, retryc, block
          @tx_pending_events.push e
          @@all_pending_events[event] += 1
        else
          EM.next_tick do
            block.call if block
            unless opts[:keep_connection]
              EM.next_tick do
                connection.disconnect do
                  EM.stop
                end
              end
            end
          end
        end
      end
    end
  end

  def pending_events_for sender # :nodoc:
    (@tx_pending_events||[]).select {|i| i.sender == sender }.map {|i| i.event }
  end

  def self.pending? event # :nodoc:
    @@all_pending_events.has_key? event
  end

  def wait_for_connection &block
    defer1 = proc do
      Tengine::Core.stdout_logger.info "waiting for MQ to be set up..." if defined? Tengine::Core
      sleep 0.1 until connection.connected?
    end
    defer2 = proc do |a|
      channel; setup_confirmation # initiate
      sleep 0.1 until instance_variable_get("@publisher_confirmation_initiated")
    end
    EM.defer defer1, lambda {|a| EM.defer defer2, block }
  end

  private

  def ensure_publisher_confirmation
    if instance_variable_get("@publisher_confirmation_initiated")
      yield
    else
      wait_for_connection do
        yield
      end
    end
  end

  @@tx_pending_event = Struct.new :tag, :sender, :event, :opts, :retry, :block
  @@all_pending_events = Hash.new do |h, k| h.store k, 0 end

  def setup_confirmation
    return if @publisher_confirmation_initiated
    raise "It seems the message queue is absent.  Consider using Suite#wait_for_connection" unless connection.connected?

    cap = connection.server_capabilities
    if cap and cap["publisher_confirms"] then
      unless channel.uses_publisher_confirmations?
        channel.confirm_select do
          @tx_pending_events = Array.new
          @tag = 1
          @publisher_confirmation_initiated = true
          channel.on_ack do |ack|
            unless @tx_pending_events.empty?
              f = false
              n = ack.delivery_tag
              ok = []
              ng = []
              if ack.multiple
                ok, @tx_pending_events = @tx_pending_events.partition {|i| i.tag <= n }
              else
                ng, @tx_pending_events = @tx_pending_events.partition {|i| i.tag < n }
                if @tx_pending_events.empty?
                  # the packet in quesion is lost?
                else
                  e = @tx_pending_events.shift
                  ok = [e]
                end
              end
              f = !ng.empty?
              ng.each_next_tick {|e| e.sender.fire e.event, e.opts, e.retry + 1, &e.block }
              ok.each_next_tick {|e| e.block.call if e.block }
              ok.each do |e|
                x = @@all_pending_events[e.event] - 1
                if x <= 0
                  @@all_pending_events.delete e.event
                else
                  @@all_pending_events[e.event] = x
                end
              end
              f = ok.inject(f) {|r, e| r | e.opts[:keep_connection] }
              if f == false and @tx_pending_events.empty?
                @connection.disconnect do
                  EM.stop
                end
              end
            end
          end
        end
      end
    else
      @publisher_confirmation_initiated = true # initiated, but not available
      if !$__tengine_mq_suite_warning_shown__
        $__tengine_mq_suite_warning_shown__ = true
        Tengine.logger.warn <<-end

The  message  queue  broker   you  are  connecting   lacks  Publisher [BEWARE!]
Confirmation capability,  so we cannot make  sure your events  are in [BEWARE!]
fact reaching  to one  of the Tengine  cores.  We strongly  recommend [BEWARE!]
you to use a relatively recent version of RabbitMQ.                   [BEWARE!]

        end
      end
    end
  end
end
