# -*- coding: utf-8 -*-
require 'tengine'

require 'active_support/core_ext/object/blank'
require 'active_support/json'
require 'uuid'

# Serializable Class of object to send to an MQ or to receive from MQ.
class Tengine::Event

  class << self
    def config; @config ||= {}; end
    def config=(v); @config = v; end
    def mq_suite; @mq_suite ||= Tengine::Mq::Suite.new(config); end

    def uuid_gen
      # uuidtools と uuid のどちらが良いかは以下のサイトを参照して uuid を使うようにしました。
      # http://d.hatena.ne.jp/kiwamu/20090205/1233826235
      @uuid_gen ||= ::UUID.new
    end

    # publish an event message to AMQP exchange
    # @param [String] event_type_name event_type_name
    # @param [Hash] options the options for attributes
    # @option options [String] :key attriute key
    # @option options [String] :source_name source_name
    # @option options [Time] :occurred_at occurred_at
    # @option options [Hash] :properties properties
    # @return [Tengine::Event]
    def fire(event_type_name, options = {})
      event = self.new((options || {}).update(:event_type_name => event_type_name))
      mq_suite.exchange.publish(event.to_json)
      event
    end
  end


  # constructor
  # @param [Hash] attrs the options for attributes
  # @option attrs [String] :key attriute key
  # @option attrs [String] :event_type_name event_type_name
  # @option attrs [String] :source_name source_name
  # @option attrs [Time] :occurred_at occurred_at
  # @option attrs [Hash] :properties properties
  # @return [Tengine::Event]
  def initialize(attrs = nil)
    if attrs
      raise ArgumentError, "attrs must be a Hash but was #{attrs.inspect}" unless attrs.is_a?(Hash)
      attrs.each do |key, value|
        send("#{key}=", value)
      end
    end
    @key ||= self.class.uuid_gen.generate # Stringを返す
    @notification_level ||= NOTIFICATION_LEVELS_INV[:info]
  end

  # @attribute
  # キー。インスタンス生成時に同じ意味のイベントには同じキーが割り振られます。
  attr_accessor :key

  # @attribute
  # イベント種別名。
  attr_reader :event_type_name
  def event_type_name=(v); @event_type_name = v.nil? ? nil : v.to_s; end

  # @attribute
  # イベントの発生源の識別名。
  attr_reader :source_name
  def source_name=(v); @source_name = v.nil? ? nil : v.to_s; end

  # @attribute
  # イベントの発生日時。
  attr_accessor :occurred_at
  def occurred_at=(v)
    if v
      raise ArgumentError, "occurred_at must be a Time but was #{v.inspect}" unless v.is_a?(Time)
      @occurred_at = v.utc
    else
      @occurred_at = nil
    end
  end

  # from notification_level to notification_level_key
  NOTIFICATION_LEVELS = {
      0 => :gr_heartbeat,
      1 => :debug,
      2 => :info,
      3 => :warn,
      4 => :error,
      5 => :fatal,
  }.freeze

  # from notification_level_key to notification_level
  NOTIFICATION_LEVELS_INV = NOTIFICATION_LEVELS.invert.freeze

  # @attribute
  # イベントの通知レベル
  attr_accessor :notification_level

  # @attribute
  # イベントの通知レベルキー
  # :gr_heartbeat/:debug/:info/:warn/:error/:fatal
  def notification_level_key; NOTIFICATION_LEVELS[notification_level];end
  def notification_level_key=(v); self.notification_level = NOTIFICATION_LEVELS_INV[v.to_sym]; end


  # @attribute
  # プロパティ。他の属性だけでは表現できない諸属性を格納するHashです。
  def properties
    @properties ||= {}
  end

  def properties=(hash)
    if hash
      @properties = (hash || {}).inject({}){|d, (k,v)| d[k.to_s] = v; d}
    else
      @properties = nil
    end
  end

  ATTRIBUTE_NAMES = [:key, :event_type_name, :source_name, :occurred_at, :properties].freeze

  # @return [Hash] attributes of this object
  def attributes
    ATTRIBUTE_NAMES.inject({}) do |d, attr_name|
      v = send(attr_name)
      d[attr_name] = v unless v.blank?
      d
    end
  end


end
