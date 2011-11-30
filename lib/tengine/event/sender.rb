# -*- coding: utf-8 -*-
require 'tengine/event'

require 'active_support/core_ext/array/extract_options'

class Tengine::Event::Sender

  # 現在不使用。やがて消します。
  RetryError = Class.new StandardError

  attr_reader :mq_suite
  attr_accessor :default_keep_connection
  attr_accessor :stop_after_transmission

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
    @stop_after_transmission = !@default_keep_connection
  end

  def stop(&block)
    @mq_suite.stop(&block)
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
  def fire(event_or_event_type_name, options = {}, retry_count = 0, &block)
    Tengine.logger.info("fire(#{event_or_event_type_name.inspect}, #{options}, #{retry_count}) called")
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
    @mq_suite.fire self, event, { :keep_connection => keep_connection, :retry_interval => sender_retry_interval, :retry_count => sender_retry_count }, retry_count, block
    event
    Tengine.logger.info("fire(#{event_or_event_type_name.inspect}, #{options}) complete")
  rescue Exception => e
    Tengine.logger.warn("fire(#{event_or_event_type_name.inspect}, #{options}) raised [#{e.class.name}] #{e.message}")
    raise e
  end

  def pending_events
    @mq_suite.pending_events_for self
  end

  # fireの中で勝手に待つようにしましたので、今後不要です。
  # 使っている箇所はやがて消していきましょう。
  def wait_for_connection
    yield
  end
end
