# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
require File.expand_path(File.dirname(__FILE__) + '/../../spec_helper')

require_relative '../../../lib/tengine/mq/suite'
require 'amqp'

describe Enumerable do
  describe "#each_next_tick" do
    it "eachと同じ順にiterateする" do
      str = ""
      EM.run do
        [1, 2, 3, 4].each_next_tick do |i|
          str << i.to_s
        end
        EM.add_timer 0.1 do EM.stop end
      end
      str.should == "1234"
    end

    it "next_tickでやる" do
      str = ""
      EM.run do
        [1, 2, 3, 4].each_next_tick do |i|
          str << i.to_s
        end
        str.should == ""
        EM.add_timer 0.1 do EM.stop end
      end
    end
  end
end

describe "Tengine::Mq::Suite" do

  context "normal usage" do
    before do
      @config = {
        :connection => {"auto_reconnect_delay" => 3},
        :exchange => {'name' => "exchange1", 'type' => 'direct', 'durable' => true, 'publish' => {'persistent' => true}},
        :queue => {'name' => "queue1", 'durable' => true},
      }
      @mock_connection = mock(:connection)
      @mock_channel = mock(:channel)
      @mock_exchange = mock(:exchange)
      @mock_queue = mock(:queue)
    end

    subject{ Tengine::Mq::Suite.new(@config) }

    it "'s exchange must be AMQP::Exchange" do
      # connection
      AMQP.should_receive(:connect).with({:user=>"guest", :pass=>"guest", :vhost=>"/",
          :logging=>false, :insist=>false, :host=>"localhost", :port=>5672}).and_return(@mock_connection)
      @mock_connection.should_receive(:on_tcp_connection_loss)
      @mock_connection.should_receive(:after_recovery)
      @mock_connection.should_receive(:on_closed)
      @mock_connection.stub(:connected?).and_return(true)
      @mock_connection.stub(:server_capabilities).and_return(nil)
      # channel
      AMQP::Channel.should_receive(:new).with(@mock_connection, :prefetch => 1, :auto_recovery => true).and_return(@mock_channel)
      # exchange
      AMQP::Exchange.should_receive(:new).with(@mock_channel, "direct", "exchange1",
        :passive=>false, :durable=>true, :auto_delete=>false, :internal=>false, :nowait=>true).and_return(@mock_exchange)
      subject.exchange.should == @mock_exchange
    end

    it "'s queue must be AMQP::Queue" do
      # connection
      AMQP.should_receive(:connect).with({:user=>"guest", :pass=>"guest", :vhost=>"/",
          :logging=>false, :insist=>false, :host=>"localhost", :port=>5672}).and_return(@mock_connection)
      @mock_connection.should_receive(:on_tcp_connection_loss)
      @mock_connection.should_receive(:after_recovery)
      @mock_connection.should_receive(:on_closed)
      @mock_connection.stub(:connected?).and_return(true)
      @mock_connection.stub(:server_capabilities).and_return(nil)
      # channel
      AMQP::Channel.should_receive(:new).with(@mock_connection, :prefetch => 1, :auto_recovery => true).and_return(@mock_channel)
      # exchange
      AMQP::Exchange.should_receive(:new).with(@mock_channel, "direct", "exchange1",
        :passive=>false, :durable=>true, :auto_delete=>false, :internal=>false, :nowait=>true).and_return(@mock_exchange)
      # queue
      AMQP::Queue.should_receive(:new).with(@mock_channel, "queue1",
        :passive=>false, :durable=>true, :auto_delete=>false, :exclusive=>false, :nowait=>true,
        :subscribe=>{:ack=>true, :nowait=>true, :confirm=>nil}).and_return(@mock_queue)
      @mock_queue.should_receive(:bind).with(@mock_exchange)
      subject.queue.should == @mock_queue
    end

    it "'s connection should reconnect on_tcp_connection_loss" do
      settings = {
        :host => "localhost", :port => 5672, :user => "guest", :pass => "guest", :vhost => "/",
        :timeout => nil, :logging => false, :ssl => false, :broker => nil, :frame_max => 131072, :insist => false
      }
      # connection
      AMQP.should_receive(:connect).with({:user=>"guest", :pass=>"guest", :vhost=>"/",
          :logging=>false, :insist=>false, :host=>"localhost", :port=>5672}).and_return(@mock_connection)
      @mock_connection.should_receive(:on_tcp_connection_loss).and_yield(@mock_connection, settings)
      @mock_connection.should_receive(:reconnect).with(false, 3)
      @mock_connection.should_receive(:after_recovery)
      @mock_connection.should_receive(:on_closed)
      subject.connection
    end

    it "'s connection should reconnect on_tcp_connection_loss, then after_recovery called" do
      settings = {
        :host => "localhost", :port => 5672, :user => "guest", :pass => "guest", :vhost => "/",
        :timeout => nil, :logging => false, :ssl => false, :broker => nil, :frame_max => 131072, :insist => false
      }
      # connection
      AMQP.should_receive(:connect).with({:user=>"guest", :pass=>"guest", :vhost=>"/",
          :logging=>false, :insist=>false, :host=>"localhost", :port=>5672}).twice.and_return(@mock_connection)
      @mock_connection.should_receive(:on_tcp_connection_loss).twice.and_yield(@mock_connection, settings)
      @mock_connection.should_receive(:reconnect).twice.with(false, 3)
      @mock_connection.should_receive(:on_closed).twice
      @mock_connection.should_receive(:after_recovery).twice # .and_yield(@mock_connection, settings)
      # subject.should_receive(:reset_channel) # stack level too deep になってしまうので、コメントアウトしてます
      subject.connection(true)
    end

    it "'s reset_channel call channel, exchange, queue with true to clear memoized objects" do
      AMQP.should_receive(:connect).with({:user=>"guest", :pass=>"guest", :vhost=>"/",
          :logging=>false, :insist=>false, :host=>"localhost", :port=>5672}).and_return(@mock_connection)
      @mock_connection.should_receive(:on_tcp_connection_loss)
      @mock_connection.should_receive(:after_recovery)
      @mock_connection.should_receive(:on_closed)
      subject.reset_channel
    end

  end

end
