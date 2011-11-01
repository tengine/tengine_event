# -*- coding: utf-8 -*-
require 'tengine/event'

require 'active_support/core_ext/array/extract_options'

class Tengine::Event::Sender

  class RetryError < StandardError
    attr_reader :event, :retry_count, :source
    def initialize(event, retry_count, source = nil)
      @event, @retry_count = event, retry_count
      @source = source.is_a?(RetryError) ? source.source : source
    end
    def message
      result = "failed %d time(s) to send event %s." % [retry_count, event]
      if @source
        result << "  The last source exception was #{@source.inspect}"
      end
      result
    end
    alias_method :to_s, :message
  end

  attr_reader :mq_suite
  attr_reader :logger
  attr_accessor :default_keep_connection

  def initialize(*args)
    options = args.extract_options!
    config_or_mq_suite = args.first
    case config_or_mq_suite
    when Tengine::Mq::Suite then
      @mq_suite = config_or_mq_suite
    when nil then
      @mq_suite = Tengine::Mq::Suite.new(options)
    end
    @default_keep_connection = (@mq_suite.config[:sender] || {})[:keep_connection]
    @logger = options[:logger] || Tengine::NullLogger.new
  end

  @@retry_counts = Hash.new
  @@finalizer = lambda do |id| @@retry_counts.delete id end

  def __instrument # test backdoor
    @@retry_counts.size
  end

  # publish an event message to AMQP exchange
  # @param [String/Tengine::Event] event_or_event_type_name
  # @param [Hash] options the options for attributes
  # @option options [String] :key attriute key
  # @option options [String] :source_name source_name
  # @option options [Time] :occurred_at occurred_at
  # @option options [Integer] :level level
  # @option options [Symbol] :level_key level_key
  # @option options [String] :sender_name sender_name
  # @option options [Hash] :properties properties
  # @option options [Hash] :keep_connection
  # @option options [Hash] :retry_interval
  # @option options [Hash] :retry_count
  # @return [Tengine::Event]
  def fire(event_or_event_type_name, options = {}, &block)
    @logger.info("fire(#{event_or_event_type_name.inspect}, #{options}) called")
    opts ||= (options || {}).dup
    keep_connection ||= (opts.delete(:keep_connection) || default_keep_connection)
    sender_retry_interval ||= (opts.delete(:retry_interval) || mq_suite.config[:sender][:retry_interval]).to_i
    sender_retry_count ||= (opts.delete(:retry_count) || mq_suite.config[:sender][:retry_count]).to_i
    event =
      case event_or_event_type_name
      when Tengine::Event then event_or_event_type_name
      else
        Tengine::Event.new(opts.update(
          :event_type_name => event_or_event_type_name.to_s))
      end
    ObjectSpace.define_finalizer event, @@finalizer
    send_event_with_retry(event, keep_connection, sender_retry_interval, sender_retry_count, &block)
    event
    @logger.info("fire(#{event_or_event_type_name.inspect}, #{options}) complete")
  rescue Exception => e
    @logger.warn("fire(#{event_or_event_type_name.inspect}, #{options}) raised [#{e.class.name}] #{e.message}")
    raise e
  end

  private
  def send_event_with_retry(event, keep_connection, sender_retry_interval, sender_retry_count, &block)
    @logger.debug("send_event_with_retry(#{event.inspect}) called")
    s, x, n = false, false, @@retry_counts[event.object_id] || 0
    raise RetryError.new(event, n) if n > sender_retry_count
    @@retry_counts[event.object_id] = n + 1
    # ここで渡される block としては、以下のように mq の connection クローズ と eventmachine の停止が考えられる
    # block = Proc.new(mq.connection.disconnect { EM.stop })
    mq_suite.exchange.publish(event.to_json, mq_suite.config[:exchange][:publish]) do
      # 送信に成功した場合にのみ、このブロックは実行される
      s = true
      @@retry_counts.delete event.object_id # eager deletion
      block.yield if block_given?
      unless keep_connection
        @logger.warn("now disconnecting mq_suite.connection and EM.stop by #{self.inspect}")
        mq_suite.connection.disconnect { EM.stop }
      end
    end
  rescue => e
    @logger.warn("send_event_with_retry(#{event.inspect}) raised [#{e.class.name}] #{e.message}")
    # amqpは送信に失敗しても例外を raise せず、 publish に渡されたブロックが実行されないだけ。
    # 他の失敗による例外は、この rescue でリトライされる
    if n >= sender_retry_count
      # 送信に失敗しているのに自動的に切断してはいけません
      # mq_suite.connection.disconnect { EM.stop } unless keep_connection
      x = true
      @@retry_counts.delete event.object_id # eager deletion
      raise RetryError.new(event, n, e)
    end
  ensure
    if not s and not x and n <= sender_retry_count
      EM.add_timer(sender_retry_interval) do
        send_event_with_retry(event, keep_connection, sender_retry_interval, sender_retry_count, &block)
      end
    end
  end
end
