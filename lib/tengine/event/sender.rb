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
  # @return [Tengine::Event]
  def fire(event_or_event_type_name, options = {}, &block)
    event =
      case event_or_event_type_name
      when Tengine::Event then event_or_event_type_name
      else
        Tengine::Event.new((options || {}).update(
          :event_type_name => event_or_event_type_name.to_s))
      end
    # ここで渡される block としては、以下のように mq の connection クローズ と eventmachine の停止が考えられる
    # mq.connection.disconnect { EM.stop }
    mq_suite.exchange.publish(event.to_json, &block)
    event
  end

end
