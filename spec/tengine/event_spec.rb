# -*- coding: utf-8 -*-
require File.expand_path(File.dirname(__FILE__) + '/../spec_helper')

require 'time'

describe "Tengine::Event" do
  describe :new_object do
    subject{ Tengine::Event.new }
    it{ subject.should be_a(Tengine::Event) }
    its(:key){ should =~ /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/ }
    its(:event_type_name){ should be_nil }
    its(:source_name){ should be_nil }
    its(:occurred_at){ should be_nil }
    its(:notification_level){ should == 2}
    its(:notification_level_key){ should == :info}
    its(:properties){ should be_a(Hash) }
    its(:properties){ should be_empty }
    it {
      attrs = subject.attributes
      attrs.should be_a(Hash)
      attrs.delete(:key).should_not be_nil
      attrs.should == {}
    }
    it {
      hash = JSON.parse(subject.to_json)
      hash.should be_a(Hash)
      hash.delete('key').should =~ /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/
      hash.should == {
        "notification_level"=>2
      }
    }
  end

  describe :new_object_with_attrs do
    subject{ Tengine::Event.new(
        :event_type_name => :foo,
        :key => "hoge",
        'source_name' => "server1",
        :occurred_at => Time.utc(2011,8,11,12,0),
        :notification_level_key => 'error',
        :properties => {:bar => "ABC", :baz => 999}
        )}
    it{ subject.should be_a(Tengine::Event) }
    its(:key){ should == "hoge" }
    its(:event_type_name){ should == "foo" }
    its(:source_name){ should == "server1" }
    its(:occurred_at){ should == Time.utc(2011,8,11,12,0) }
    its(:notification_level){ should == 4}
    its(:notification_level_key){ should == :error}
    its(:properties){ should == {'bar' => "ABC", 'baz' => 999}}
    it {
      attrs = subject.attributes
      attrs.should == {
        :event_type_name => 'foo',
        :key => "hoge",
        :source_name => "server1",
        :occurred_at => Time.utc(2011,8,11,12,0),
        :properties => {'bar' => "ABC", 'baz' => 999}
      }
    }
    it {
      hash = JSON.parse(subject.to_json)
      hash.should be_a(Hash)
      hash.should == {
        "notification_level"=>4,
        'event_type_name' => 'foo',
        'key' => "hoge",
        'source_name' => "server1",
        'occurred_at' => "2011-08-11T12:00:00Z", # Timeオブジェクトは文字列に変換されます
        'properties' => {'bar' => "ABC", 'baz' => 999}
      }
    }
  end

  describe 'occurred_at with local_time should convert to UTC' do
    subject{ Tengine::Event.new(
        :occurred_at => Time.parse("2011-08-31 12:00:00 +0900"),
        :key => 'hoge'
        )}
    it{ subject.should be_a(Tengine::Event) }
    its(:occurred_at){ should == Time.utc(2011,8,31,3,0) }
    its(:occurred_at){ should be_utc }
    it {
      hash = JSON.parse(subject.to_json)
      hash.should be_a(Hash)
      hash.should == {
        "notification_level"=>2,
        'key' => "hoge",
        'occurred_at' => "2011-08-31T03:00:00Z", # Timeオブジェクトは文字列に変換されます
      }
    }
  end

  it 'attrs_for_new must be Hash' do
    expect{
      Tengine::Event.new("{foo: 1, bar: 2}")
    }.to raise_error(ArgumentError, /attrs must be a Hash but was/)
  end


  it 'occurred_at must be Time' do
    expect{
      Tengine::Event.new(:occurred_at => "2011-08-31 12:00:00 +0900")
    }.to raise_error(ArgumentError, /occurred_at must be a Time but was/)
  end

  describe :fire do
    before do
      Tengine::Event.config = {
        :connection => {"foo" => "aaa"},
        :exchange => {'name' => "exchange1", 'type' => 'direct', 'durable' => true},
        :queue => {'name' => "queue1", 'durable' => true},
      }
      @mock_connection = mock(:connection)
      @mock_channel = mock(:channel)
      @mock_exchange = mock(:exchange)
      AMQP.should_receive(:connect).with({:foo => "aaa"}).and_return(@mock_connection)
      AMQP::Channel.should_receive(:new).with(@mock_connection).and_return(@mock_channel)
      AMQP::Exchange.should_receive(:new).with(@mock_channel, "direct", "exchange1", :durable => true).and_return(@mock_exchange)
    end

    it "JSON形式にserializeしてexchangeにpublishする" do
      expected_event = Tengine::Event.new(:event_type_name => :foo, :key => "uniq_key")
      @mock_exchange.should_receive(:publish).with(expected_event.to_json)
      Tengine::Event.fire(:foo, :key => "uniq_key")
    end
  end


  describe :notification_level do
    {
      0 => :gr_heartbeat,
      1 => :debug,
      2 => :info,
      3 => :warn,
      4 => :error,
      5 => :fatal,
    }.each do |level, level_key|
      context "set by notification_level" do
        subject{ Tengine::Event.new(:notification_level => level) }
        its(:notification_level_key){ should == level_key}
      end
      context "set Symbol by notification_level_key" do
        subject{ Tengine::Event.new(:notification_level_key => level_key.to_sym) }
        its(:notification_level){ should == level}
        its(:notification_level_key){ should == level_key.to_sym}
      end
      context "set String by notification_level_key" do
        subject{ Tengine::Event.new(:notification_level_key => level_key.to_s) }
        its(:notification_level){ should == level}
        its(:notification_level_key){ should == level_key.to_sym}
      end
    end
  end

end
