# -*- coding: utf-8 -*-
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
      AMQP.should_receive(:connect).with({:user=>"guest", :pass=>"guest", :vhost=>"/",
          :logging=>false, :insist=>false, :host=>"localhost", :port=>5672}).and_return(@mock_connection)
      @mock_connection.should_receive(:on_tcp_connection_loss)
      @mock_connection.should_receive(:after_recovery)
      AMQP::Channel.should_receive(:new).with(@mock_connection, :prefetch => 1, :auto_recovery => true).and_return(@mock_channel)
      AMQP::Exchange.should_receive(:new).with(@mock_channel, "direct", "exchange1",
        :passive=>false, :durable=>true, :auto_delete=>false, :internal=>false, :nowait=>true).and_return(@mock_exchange)
      subject.exchange.should == @mock_exchange
    end

    it "'s queue must be AMQP::Queue" do
      AMQP.should_receive(:connect).with({:user=>"guest", :pass=>"guest", :vhost=>"/",
          :logging=>false, :insist=>false, :host=>"localhost", :port=>5672}).and_return(@mock_connection)
      @mock_connection.should_receive(:on_tcp_connection_loss)
      @mock_connection.should_receive(:after_recovery)
      AMQP::Channel.should_receive(:new).with(@mock_connection, :prefetch => 1, :auto_recovery => true).and_return(@mock_channel)
      AMQP::Exchange.should_receive(:new).with(@mock_channel, "direct", "exchange1",
        :passive=>false, :durable=>true, :auto_delete=>false, :internal=>false, :nowait=>true).and_return(@mock_exchange)
      AMQP::Queue.should_receive(:new).with(@mock_channel, "queue1",
        :passive=>false, :durable=>true, :auto_delete=>false, :exclusive=>false, :nowait=>true,
        :subscribe=>{:ack=>true, :nowait=>true, :confirm=>nil}).and_return(@mock_queue)
      @mock_queue.should_receive(:bind).with(@mock_exchange)
      subject.queue.should == @mock_queue
    end

    it "'s connection should reconnect on_tcp_connection_loss" do
      AMQP.should_receive(:connect).with({:user=>"guest", :pass=>"guest", :vhost=>"/",
          :logging=>false, :insist=>false, :host=>"localhost", :port=>5672}).and_return(@mock_connection)
      @mock_connection.should_receive(:on_tcp_connection_loss).and_yield(@mock_connection, @config[:connection])
      @mock_connection.should_receive(:reconnect).with(false, 3)
      @mock_connection.should_receive(:after_recovery).and_yield(@mock_connection, @config[:connection])
      subject.should_receive(:channel).and_return(@mock_channel)
      subject.should_receive(:exchange).and_return(@mock_queue)
      subject.should_receive(:queue).times.and_return(@mock_queue)
      subject.connection
    end

  end

end
