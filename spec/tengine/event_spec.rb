# -*- coding: utf-8 -*-
require File.expand_path(File.dirname(__FILE__) + '/../spec_helper')

require 'amqp'
require 'time'

describe "Tengine::Event" do
  expected_host_name = "this_server1"

  before do
    Tengine::Event.stub!(:host_name).and_return(expected_host_name)
  end

  describe :new_object do
    context "without config" do
      before do
        Tengine::Event.config = {}
      end

      it{ Tengine::Event.default_source_name.should == expected_host_name }
      it{ Tengine::Event.default_sender_name.should == expected_host_name }
      it{ Tengine::Event.default_level.should == 2 }

      subject{ Tengine::Event.new }
      it{ subject.should be_a(Tengine::Event) }
      its(:key){ should =~ /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/ }
      its(:event_type_name){ should be_nil }
      its(:source_name){ should == expected_host_name}
      its(:occurred_at){ should be_nil }
      its(:level){ should == 2}
      its(:level_key){ should == :info}
      its(:sender_name){ should == expected_host_name }
      its(:properties){ should be_a(Hash) }
      its(:properties){ should be_empty }
      it {
        attrs = subject.attributes
        attrs.should be_a(Hash)
        attrs.delete(:key).should_not be_nil
        attrs.should == {
          :level=>2,
          :source_name => expected_host_name,
          :sender_name => expected_host_name,
        }
      }
      it {
        hash = JSON.parse(subject.to_json)
        hash.should be_a(Hash)
        hash.delete('key').should =~ /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/
        hash.should == {
          "level" => 2,
          'source_name' => expected_host_name,
          'sender_name' => expected_host_name,
        }
      }
    end

    context "with config" do
      before do
        Tengine::Event.config = {
          :default_source_name => "event_source1",
          :default_sender_name => "sender1",
          :default_level_key => :warn
        }
      end
      after do
        Tengine::Event.config = {}
      end

      it{ Tengine::Event.default_source_name.should == "event_source1" }
      it{ Tengine::Event.default_sender_name.should == "sender1" }
      it{ Tengine::Event.default_level.should == 3 }

      subject{ Tengine::Event.new }
      it{ subject.should be_a(Tengine::Event) }
      its(:key){ should =~ /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/ }
      its(:event_type_name){ should be_nil }
      its(:source_name){ should == "event_source1"}
      its(:occurred_at){ should be_nil }
      its(:level){ should == 3}
      its(:level_key){ should == :warn}
      its(:sender_name){ should == "sender1" }
      its(:properties){ should be_a(Hash) }
      its(:properties){ should be_empty }
      it {
        attrs = subject.attributes
        attrs.should be_a(Hash)
        attrs.delete(:key).should_not be_nil
        attrs.should == {
          :level=>3,
          :source_name => "event_source1",
          :sender_name => "sender1",
        }
      }
      it {
        hash = JSON.parse(subject.to_json)
        hash.should be_a(Hash)
        hash.delete('key').should =~ /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/
        hash.should == {
          'level'=>3,
          'source_name' => "event_source1",
          'sender_name' => "sender1",
        }
      }
    end

  end

  describe :new_object_with_attrs do
    subject{ Tengine::Event.new(
        :event_type_name => :foo,
        :key => "hoge",
        'source_name' => "server1",
        :occurred_at => Time.utc(2011,8,11,12,0),
        :level_key => 'error',
        'sender_name' => "server2",
        :properties => {:bar => "ABC", :baz => 999}
        )}
    it{ subject.should be_a(Tengine::Event) }
    its(:key){ should == "hoge" }
    its(:event_type_name){ should == "foo" }
    its(:source_name){ should == "server1" }
    its(:occurred_at){ should == Time.utc(2011,8,11,12,0) }
    its(:level){ should == 4}
    its(:level_key){ should == :error}
    its(:sender_name){ should == "server2" }
    its(:properties){ should == {'bar' => "ABC", 'baz' => 999}}
    it {
      attrs = subject.attributes
      attrs.should == {
        :event_type_name => 'foo',
        :key => "hoge",
        :source_name => "server1",
        :occurred_at => Time.utc(2011,8,11,12,0),
        :level => 4,
        :sender_name => "server2",
        :properties => {'bar' => "ABC", 'baz' => 999}
      }
    }
    it {
      hash = JSON.parse(subject.to_json)
      hash.should be_a(Hash)
      hash.should == {
        "level"=>4,
        'event_type_name' => 'foo',
        'key' => "hoge",
        'source_name' => "server1",
        'occurred_at' => "2011-08-11T12:00:00Z", # Timeオブジェクトは文字列に変換されます
        'sender_name' => "server2",
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
        "level"=>2,
        'key' => "hoge",
        'occurred_at' => "2011-08-31T03:00:00Z", # Timeオブジェクトは文字列に変換されます
        'source_name' => "this_server1",
        'sender_name' => "this_server1",
      }
    }
  end

  it 'attrs_for_new must be Hash' do
    expect{
      Tengine::Event.new("{foo: 1, bar: 2}")
    }.to raise_error(ArgumentError, /attrs must be a Hash but was/)
  end


  describe :occurred_at do
    context "valid String" do
      subject{ Tengine::Event.new(:occurred_at => "2011-08-31 12:00:00 +0900") }
      its(:occurred_at){ should be_a(Time); should == Time.utc(2011, 8, 31, 3)}
    end
  end
  it 'occurred_at must be Time' do
    expect{
      Tengine::Event.new(:occurred_at => "invalid time string")
    }.to raise_error(ArgumentError, /no time information/)
  end

  describe :level do
    {
      0 => :gr_heartbeat,
      1 => :debug,
      2 => :info,
      3 => :warn,
      4 => :error,
      5 => :fatal,
    }.each do |level, level_key|
      context "set by level" do
        subject{ Tengine::Event.new(:level => level) }
        its(:level_key){ should == level_key}
      end
      context "set Symbol by level_key" do
        subject{ Tengine::Event.new(:level_key => level_key.to_sym) }
        its(:level){ should == level}
        its(:level_key){ should == level_key.to_sym}
      end
      context "set String by level_key" do
        subject{ Tengine::Event.new(:level_key => level_key.to_s) }
        its(:level){ should == level}
        its(:level_key){ should == level_key.to_sym}
      end
    end
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
      AMQP::Channel.should_receive(:new).with(@mock_connection, :prefetch => 1).and_return(@mock_channel)
      AMQP::Exchange.should_receive(:new).with(@mock_channel, "direct", "exchange1", :durable => true).and_return(@mock_exchange)
    end

    it "JSON形式にserializeしてexchangeにpublishする" do
      expected_event = Tengine::Event.new(:event_type_name => :foo, :key => "uniq_key")
      @mock_exchange.should_receive(:publish).with(expected_event.to_json)
      Tengine::Event.fire(:foo, :key => "uniq_key")
    end
  end


  describe :parse do
    context "can parse valid json object" do
      subject do
        source = Tengine::Event.new(
          :event_type_name => :foo,
          :key => "hoge",
          'source_name' => "server1",
          :occurred_at => Time.utc(2011,8,11,12,0),
          :level_key => 'error',
          'sender_name' => "server2",
          :properties => {:bar => "ABC", :baz => 999}
          )
        Tengine::Event.parse(source.to_json)
      end
      its(:key){ should == "hoge" }
      its(:event_type_name){ should == "foo" }
      its(:source_name){ should == "server1" }
      its(:occurred_at){ should == Time.utc(2011,8,11,12,0) }
      its(:level){ should == 4}
      its(:level_key){ should == :error}
      its(:sender_name){ should == "server2" }
      its(:properties){ should == {'bar' => "ABC", 'baz' => 999}}
    end

    it "raise ArgumentError for invalid attribute name" do
      expect{
        Tengine::Event.parse({'name' => :foo}.to_json)
      }.to raise_error(NoMethodError)
    end

  end

end
