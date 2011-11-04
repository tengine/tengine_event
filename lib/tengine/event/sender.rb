# -*- coding: utf-8 -*-
require 'tengine/event'

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
  attr_accessor :default_keep_connection
  attr_accessor :stop_after_transmission

  def initialize(config_or_mq_suite = nil)
    case config_or_mq_suite
    when Tengine::Mq::Suite then
      @mq_suite = config_or_mq_suite
    when nil, Hash then
      @mq_suite = Tengine::Mq::Suite.new(config_or_mq_suite)
    end
    @default_keep_connection = (@mq_suite.config[:sender] || {})[:keep_connection]
    @stop_after_transmission = !@default_keep_connection
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
  end

  def pending_events
    @mq_suite.pending_events_for self
  end

  def wait_for_connection
    mq_suite.wait_for_connection do
      yield
    end
  end
end
