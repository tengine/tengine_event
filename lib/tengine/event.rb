# -*- coding: utf-8 -*-
require 'tengine'

require 'active_support/core_ext/object/blank'
require 'uuid'

# Serializable Class of object to send to an MQ or to receive from MQ.
class Tengine::Event

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
      raise ArgumentError, "attrs must be a Hash but #{attrs.inspect}" unless attrs.is_a?(Hash)
      attrs.each do |key, value|
        send("#{key}=", value)
      end
    end
    # uuidtools と uuid のどちらが良いかは以下のサイトを参照して uuid を使うようにしました。
    # http://d.hatena.ne.jp/kiwamu/20090205/1233826235
    @@uuid_gen ||= ::UUID.new
    @key ||= @@uuid_gen.generate # Stringを返す
    @properties ||= {}
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

  # @attribute
  # プロパティ。他の属性だけでは表現できない諸属性を格納するHashです。
  attr_reader :properties
  def properties=(hash)
    @properties = (hash || {}).inject({}){|d, (k,v)| d[k.to_s] = v; d}
  end

end
