require File.expand_path(File.dirname(__FILE__) + '/../../spec_helper')

require 'amqp'

describe "Tengine::Mq::Suite" do

  context "normal usage" do
    before do
      @config = {
        :connection => {"foo" => "aaa"},
        :exchange => {'name' => "exchange1", 'type' => 'direct', 'durable' => true},
        :queue => {'name' => "queue1", 'durable' => true},
      }
      @mock_connection = mock(:connection)
      @mock_channel = mock(:channel)
      @mock_exchange = mock(:exchange)
      @mock_queue = mock(:queue)
    end

    subject{ Tengine::Mq::Suite.new(@config) }

    it "'s exchange must be AMQP::Exchange" do
      AMQP.should_receive(:connect).with({:foo => "aaa"}).and_return(@mock_connection)
      AMQP::Channel.should_receive(:new).with(@mock_connection, :prefetch => 1).and_return(@mock_channel)
      AMQP::Exchange.should_receive(:new).with(@mock_channel, "direct", "exchange1", :durable => true).and_return(@mock_exchange)
      subject.exchange.should == @mock_exchange
    end

    it "'s queue must be AMQP::Queue" do
      AMQP.should_receive(:connect).with({:foo => "aaa"}).and_return(@mock_connection)
      AMQP::Channel.should_receive(:new).with(@mock_connection, :prefetch => 1).and_return(@mock_channel)
      AMQP::Exchange.should_receive(:new).with(@mock_channel, "direct", "exchange1", :durable => true).and_return(@mock_exchange)
      AMQP::Queue.should_receive(:new).with(@mock_channel, "queue1", :durable => true).and_return(@mock_queue)
      @mock_queue.should_receive(:bind).with(@mock_exchange)
      subject.queue.should == @mock_queue
    end

  end

end
