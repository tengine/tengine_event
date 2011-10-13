# -*- coding: utf-8 -*-
p# -*- coding: utf-8 -*-
require File.expand_path(File.dirname(__FILE__) + '/../../spec_helper')

require 'amqp'

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
      @mock_connection.should_receive(:after_recovery).twice # .and_yield(@mock_connection, settings)
      # subject.should_receive(:reset_channel) # stack level too deep になってしまうので、コメントアウトしてます
      subject.connection(true)
    end

    it "'s reset_channel call channel, exchange, queue with true to clear memoized objects" do
      AMQP.should_receive(:connect).with({:user=>"guest", :pass=>"guest", :vhost=>"/",
          :logging=>false, :insist=>false, :host=>"localhost", :port=>5672}).and_return(@mock_connection)
      @mock_connection.should_receive(:on_tcp_connection_loss)
      @mock_connection.should_receive(:after_recovery)
      subject.should_receive(:channel).with(true)
      subject.should_receive(:exchange).with(true)
      subject.should_receive(:queue).with(true)
      subject.reset_channel
    end

  end

end
