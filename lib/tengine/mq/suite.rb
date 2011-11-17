# -*- coding: utf-8 -*-
require 'tengine/mq'

require 'active_support/core_ext/hash/keys'
require 'amqp'
require 'amqp/extensions/rabbitmq'

module Enumerable
  def each_next_tick
    raise ArgumentError, "no block given" unless block_given?
    self.reverse_each.inject(lambda{}) do |block, obj|
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
    @mutex = Mutex.new
    @publisher_confirmation_status = :disconnected
    @tx_pending_events = Array.new
    @retrying_events = Hash.new
    @any_pending_events = Hash.new
    @hooks = Hash.new do |h, k| h.store k, Array.new end

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

  # mq.connection.on_error { ... } 等は複数回指定すると前回のコールバッ
  # クを上書きしてしまうのでNG、こちらをご使用ください
  def add_hook name, &block
    @mutex.synchronize do
      @hooks[name] << block
    end
  end

  def connection(force = false, &block)
    if @connection.nil? || force
      @connection = setup_connection block
    end
    @connection
  end

  def channel(force = false)
    if @channel.nil? || force
      @channel = setup_channel
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

  def fire sender, event, opts, retryc, block # :nodoc:
    ev = @@pending_event.new 0, sender, event, opts, retryc, block
    @mutex.synchronize do
      @any_pending_events[ev] = true
      fire_internal ev
    end
  end

  def pending_events_for sender # :nodoc:
    @mutex.synchronize do
      @any_pending_events.keys.select {|i| i.sender == sender }.map {|i| i.event }
    end
  end

  def self.pending? event # :nodoc:
    @@all_pending_events.has_key? event
  end

  def stop
    EM.defer(lambda{ sleep 0.1 until @mutex.synchronize { @any_pending_events.empty? } },
             lambda{|a| connection.disconnect { if block_given? then yield else EM.stop end }})
  end

  #######
  private
  #######

  def setup_connection block
    mids = %w[
      before_recovery
      on_closed
      on_connection_interruption
      on_error
      on_possible_authentication_failure
      after_recovery
      on_tcp_connection_failure
      on_tcp_connection_loss
    ].map(&:intern)

    callbacks = mids.inject({}) {|r, mid|
      r.update mid => lambda {|*argv|
        @hooks[:"everything"].each {|proc| proc.call(mid, argv) }
        @hooks[:"connection.#{mid}"].each {|proc| proc.call(*argv) }
      }
    }

    config2 = callbacks.merge(config[:connection]) {|k, v1, v2| v2 }

    AMQP.connect(config2, &block).tap do |connection|
      @mutex.synchronize { mids.each {|i| @hooks[:"connection.#{i}"].clear } };

      unless auto_reconnect_delay.nil?
        add_hook :'connection.on_tcp_connection_loss' do |conn, settings|
          revoke_retry_timers
          conn.reconnect(false, auto_reconnect_delay.to_i)
        end
        add_hook :'connection.after_recovery' do |conn, settings|
          reinvoke_retry_timers
          @channel = nil
          @exchange = nil
          @queue = nil
        end
        add_hook :'connection.on_closed' do |conn, settings|
          revoke_retry_timers
          @connection = nil
          @channel = nil
          @exchange = nil
          @queue = nil
        end
      end

      callbacks.each_pair do |k, v|
        begin
          begin
            connection.send k, &v
          rescue RSpec::Mocks::MockExpectationError
            # @connectoinはmock objectかも...
          end
        rescue NameError
          # RSpecはrequireされていないかも...
        end
      end

      begin
        RSpec
      rescue NameError
        # rspec ではないとき(テスト以外)はundefしておく
        # 本当はテスト時もundefしたいが...
        klass = class << connection; self; end
        klass.send :undef_method, *mids
      end
    end
  end

  def setup_channel
    options = {
      :prefetch => 1,
      :auto_recovery => !auto_reconnect_delay.nil?,
    }
    AMQP::Channel.new(connection, options).tap do |channel|
      begin
        begin
          channel.on_error do |*argv|
            @mutex.synchronize do
              @hooks[:'channel.on_error'].each do |proc|
                proc.call(*argv)
              end
            end
          end
        rescue RSpec::Mocks::MockExpectationError
          # @connectoinはmock objectかも...
        end
      rescue NameError
        # RSpecはrequireされていないかも...
      end
    end
  end

  ####

  @@all_pending_events = Hash.new do |h, k| h.store k, 0 end
  @@pending_event = Struct.new :tag, :sender, :event, :opts, :retry, :block

  def release_event ev
    exchange.publish ev.event.to_json, @config[:exchange][:publish]
  end

  def event_release_failed ev, ex
    # exchange.publish はたとえば RuntimeError を raise したりするようだ
    if ev.retry < ev.opts[:retry_count]
      idx = EM.add_timer ev.opts[:retry_interval] do
        EM.next_tick do
          @mutex.synchronize do
            ev.retry += 1
            @retrying_events.rehash
            @any_pending_events.rehash
            fire_internal ev
          end
        end
      end
      @retrying_events[ev] = [idx, Time.now]
    else
      # inside mutex lock
      @retrying_events.delete ev
      @any_pending_events.delete ev
      # 送信失敗かつコネクション維持しないということはここで停止すべき
      stop unless ev.opts[:keep_connection]
    end
  end

  def event_released ev
    case @publisher_confirmation_status
    when :unsupported then
      EM.next_tick do
        @mutex.synchronize do
          @retrying_events.delete ev
          @any_pending_events.delete ev
          ev.block.call if ev.block
          stop unless ev.opts[:keep_connection]
        end
      end
    when :established then
      @mutex.synchronize do
        @tag += 1
        ev.tag = @tag
        @tx_pending_events.push ev
        @@all_pending_events[ev.event] += 1
        @retrying_events.rehash
        @any_pending_events.rehash
      end
    else
      raise "timing bug.  contact @shyouhei with this info: #{@publisher_confirmation_status}"
    end
  end

  def fire_internal ev
    # inside mutex lock
    defer_until_proper_handshake do
      begin
        release_event ev
      rescue => ex
        event_release_failed ev, ex
      else
        event_released ev
      end
    end
  end

  #####

  def revoke_retry_timers
    @mutex.synchronize do
      if @publisher_confirmation_initiated != :disconnected
        @publisher_confirmation_initiated = :disconnected
        @retrying_events.each_value do |(idx, *)|
          EM.stop_timer idx if idx
        end
        # all pending events are hereby considered lost
        @tx_pending_events.each do |e|
          @retrying_events[e] = [nil, Time.at(0)]
        end
      end
    end
  end

  def reinvoke_retry_timers
    #inside mutex lock
    @retrying_events.each_pair.to_a.each_next_tick do |i, (j, k)|
      u = (k + (i.opts[:retry_interval] || 0)) - Time.now
      if u < 0
        # retry interval passed, just send it again
        fire_internal i
      else
        # need to re-add timer
        EM.add_timer u do i.fire end
      end
    end
    @retrying_events.clear
  end

  def consume_publisher_confirmation ack
    @mutex.synchronize do
      f = @tx_pending_events.empty?
      n = ack.delivery_tag
      ok = []
      ng = []
      if ack.multiple
        ok, @tx_pending_events = @tx_pending_events.partition {|i| i.tag <= n }
      else
        ng, @tx_pending_events = @tx_pending_events.partition {|i| i.tag < n }
        if @tx_pending_events.empty?
          # the packet in quesion is lost?
        elsif e = @tx_pending_events.shift
          ok = [e]
        end
      end
      f ||= !ng.empty?
      ng.each_next_tick {|e| e.sender.fire e.event, e.opts, e.retry + 1, &e.block }
      ok.each_next_tick {|e| e.block.call if e.block }
      ok.each do |e|
        @any_pending_events.delete e
        @retrying_events.delete e
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

  def setup_publisher_confirmation
    @mutex.synchronize do
      reinvoke_retry_timers
      channel.on_ack do |ack| consume_publisher_confirmation ack end
      @tag = 1
      @publisher_confirmation_status = :established
    end
  end

  def initiate_publisher_confirmation
    @mutex.synchronize do
      case @publisher_confirmation_status
      when :established, :unsupported
        return true
      when :handshaking
        # in progress
        return false
      else
        cap = connection.server_capabilities
        if cap and cap["publisher_confirms"] then
          if channel.uses_publisher_confirmations?
            # already
          else
            @publisher_confirmation_status = :handshaking
            channel.confirm_select do
              setup_publisher_confirmation
            end
          end
          return false
        else
          @publisher_confirmation_status = :unsupported # initiated, but not available
          if !$__tengine_mq_suite_warning_shown__
            $__tengine_mq_suite_warning_shown__ = true
            Tengine.logger.warn <<-end

The  message  queue  broker   you  are  connecting   lacks  Publisher [BEWARE!]
Confirmation capability,  so we cannot make  sure your events  are in [BEWARE!]
fact reaching  to one  of the Tengine  cores.  We strongly  recommend [BEWARE!]
you to use a relatively recent version of RabbitMQ.                   [BEWARE!]

            end
          end
          return true
        end
      end
    end
  end

  def defer_until_proper_handshake
    # inside of mutex lock
    case @publisher_confirmation_status when :established, :unsupported
      EM.next_tick do yield end
    else
      raise ArgumentError, "no block given" unless block_given?
      Tengine::Core.stdout_logger.info "waiting for MQ to be set up..." if defined? Tengine::Core

      #              vvv -- note this lane
      d4 = lambda do |a| yield                                           end
      d3 = lambda do     sleep 0.1 until initiate_publisher_confirmation end
      d2 = lambda do |a| EM.defer d3, d4                                 end
      d1 = lambda do     sleep 0.1 until connection.connected?           end
      d0 = lambda do     EM.defer d1, d2                                 end

      d0.call
    end
  end
end
