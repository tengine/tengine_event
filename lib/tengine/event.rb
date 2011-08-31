# -*- coding: utf-8 -*-
require 'tengine'

require 'active_support/core_ext/object/blank'
require 'uuid'

class Tengine::Event
  def initialize
    # uuidtools と uuid のどちらが良いかは以下のサイトを参照して uuid を使うようにしました。
    # http://d.hatena.ne.jp/kiwamu/20090205/1233826235
    @@uuid_gen ||= ::UUID.new
    @key = @@uuid_gen.generate # Stringを返す
    @properties = {}
  end

  # @attribute
  # キー。インスタンス生成時に同じ意味のイベントには同じキーが割り振られます。
  attr_accessor :key

  # @attribute
  # イベント種別名。
  attr_accessor :event_type_name

  # @attribute
  # イベントの発生源の識別名。
  attr_accessor :source_name

  # @attribute
  # イベントの発生日時。
  attr_accessor :occurred_at

  # @attribute
  # プロパティ。他の属性だけでは表現できない諸属性を格納するHashです。
  attr_accessor :properties
end
