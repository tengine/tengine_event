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
      AMQP.should_receive(:connect).with(an_instance_of(Hash)).and_return(@mock_connection)
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
      AMQP.should_receive(:connect).with(an_instance_of(Hash)).and_return(@mock_connection)
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
      AMQP.should_receive(:connect).with(an_instance_of(Hash)).and_return(@mock_connection)
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
      AMQP.should_receive(:connect).with(an_instance_of(Hash)).twice.and_return(@mock_connection)
      @mock_connection.should_receive(:on_tcp_connection_loss).twice.and_yield(@mock_connection, settings)
      @mock_connection.should_receive(:reconnect).twice.with(false, 3)
      @mock_connection.should_receive(:on_closed).twice
      @mock_connection.should_receive(:after_recovery).twice # .and_yield(@mock_connection, settings)
      # subject.should_receive(:reset_channel) # stack level too deep になってしまうので、コメントアウトしてます
      subject.connection(:force)
    end

  end

  context "add_hook" do
    before :all do
      require 'socket'
      @n = rand(32768)
      @t = nil
      @e = StandardError.new
      begin
        @t = Thread.start do
          begin
            Socket.tcp_server_loop(nil, @n) do |sock, addr|
              Thread.start(sock, addr) do |s, a|
                Thread.stop # stops forever
              end
            end
          rescue @e
            # end of server
          end
        end
        @t.abort_on_exception = true
        sleep 0.5 # このくらいあればlistenできるでしょ
      rescue
        @n = rand(32768)
        retry
      end
    end

    after :all do
      @t.raise @e
    end

    before do
      pending "this spec needs 1.9.2" if RUBY_VERSION < "1.9.2"
    end

    it "hookを追加する" do
      yielded = false
      EM.run do
        mq = Tengine::Mq::Suite.new(:connection => { :host => "localhost", :port => @n })
        mq.add_hook(:"connection.on_closed") do
          yielded = true
        end
        mq.stop
      end
      yielded.should be_true
    end

    it "hookを保持する" do
      mq = nil

      # 1st time
      yielded = 0
      EM.run do
        mq = Tengine::Mq::Suite.new(:connection => { :host => "localhost", :port => @n })
        mq.add_hook(:"connection.on_closed") do
          yielded += 1
        end
        mq.stop
      end

      # 2nd time
      yielded = 0
      EM.run do
        mq.stop
      end
      yielded.should == 1
    end

    it "hookを保持する #2" do
      mq = nil

      # 1st time
      yielded = 0
      EM.run do
        mq = Tengine::Mq::Suite.new(:connection => { :host => "localhost", :port => @n })
        mq.add_hook(:"channel.on_error") do
          yielded += 1
        end
        mq.channel.exec_callback_once_yielding_self(:error, "channel close reason object")
        mq.stop
      end

      # 2nd time
      yielded = 0
      EM.run do
        mq.channel.exec_callback_once_yielding_self(:error, "channel close reason object")
        mq.stop
      end
      yielded.should == 1
    end
  end
end
