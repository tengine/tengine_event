# -*- coding: utf-8 -*-
require 'tengine/event'

class Tengine::Event::Sender

  attr_reader :mq_suite
  def initialize(config_or_mq_suite = nil)
    case config_or_mq_suite
    when Tengine::Mq::Suite then
      @mq_suite = config_or_mq_suite
    when nil, Hash then
      @mq_suite = Tengine::Mq::Suite.new(config_or_mq_suite)
    end
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
    opts ||= (options || {}).dup
    keep_connection ||= (opts.delete(:keep_connection) || mq_suite.config[:sender][:keep_connection])
    sender_retry_interval ||= (opts.delete(:retry_interval) || mq_suite.config[:sender][:retry_interval]).to_i
    sender_retry_count ||= (opts.delete(:retry_count) || mq_suite.config[:sender][:retry_count]).to_i
      event =
        case event_or_event_type_name
        when Tengine::Event then event_or_event_type_name
        else
          Tengine::Event.new(opts.update(
            :event_type_name => event_or_event_type_name.to_s))
        end
    send_event_with_retry(event, keep_connection, sender_retry_interval, sender_retry_count, &block)
    event
  end

  private
  def send_event_with_retry(event, keep_connection, sender_retry_interval, sender_retry_count, &block)
    @retrying_count ||= 0
    begin
      # ここで渡される block としては、以下のように mq の connection クローズ と eventmachine の停止が考えられる
      # block = Proc.new(mq.connection.disconnect { EM.stop })
      mq_suite.exchange.publish(event.to_json, mq_suite.config[:exchange][:publish]) do
        block.yield if block_given?
        @success_published = true
        mq_suite.connection.disconnect { EM.stop } unless keep_connection
      end
      EM.add_timer(sender_retry_interval) {
        if @retrying_count >= sender_retry_count
          raise "sender retry error"
        end
        @retrying_count += 1
        unless @success_published
          # リトライ
          send_event_with_retry(event, keep_connection, sender_retry_interval, sender_retry_count, &block)
        end
      }
    rescue => e
      # amqp の例外以外は、この rescue でリトライされる
      if @retrying_count >= sender_retry_count
        # mq_suite.connection.disconnect { EM.stop } unless keep_connection
        raise e
      else
        @retrying_count += 1
        sleep(sender_retry_interval)
        retry
      end
    end
  end

end
