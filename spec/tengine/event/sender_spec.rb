# -*- coding: utf-8 -*-
require File.expand_path(File.dirname(__FILE__) + '/../../spec_helper')

describe "Tengine::Event::Sender" do
  describe :initialize do
    context "mq_suite without config" do
      subject{ Tengine::Event::Sender.new.mq_suite }
      it{ subject.config[:connection].should_not be_empty }
      it{ subject.config[:exchange].should_not be_empty }
      it{ subject.config[:exchange][:name].should == "tengine_event_exchange" }
      it{ subject.config[:queue].should_not be_empty }
      it{ subject.config.keys.length.should == 3 }
    end

    context "mq_suite with config" do
      subject{ Tengine::Event::Sender.new(:exchange => {:name => "another_exhange"}).mq_suite }
      it{ subject.config[:exchange][:name].should == "another_exhange" }
    end
  end

  describe :fire do
    before do
      @sender = Tengine::Event::Sender.new(:exchange => {:name => "exchange1"})
      @mock_connection = mock(:connection)
      @mock_channel = mock(:channel)
      @mock_exchange = mock(:exchange)
      AMQP.should_receive(:connect).with({:user=>"guest", :pass=>"guest", :vhost=>"/",
          :logging=>false, :insist=>false, :host=>"localhost", :port=>5672}).and_return(@mock_connection)
      @mock_connection.should_receive(:on_tcp_connection_loss)
      AMQP::Channel.should_receive(:new).with(@mock_connection, :prefetch => 1, :auto_recovery => true).and_return(@mock_channel)
      AMQP::Exchange.should_receive(:new).with(@mock_channel, "direct", "exchange1",
        :passive=>false, :durable=>true, :auto_delete=>false, :internal=>false, :nowait=>true).and_return(@mock_exchange)
    end

    it "JSON形式にserializeしてexchangeにpublishする" do
      expected_event = Tengine::Event.new(:event_type_name => :foo, :key => "uniq_key")
      @mock_exchange.should_receive(:publish).with(expected_event.to_json)
      @sender.fire(:foo, :key => "uniq_key")
    end

    it "Tengine::Eventオブジェクトを直接指定することも可能" do
      expected_event = Tengine::Event.new(:event_type_name => :foo, :key => "uniq_key")
      @mock_exchange.should_receive(:publish).with(expected_event.to_json)
      @sender.fire(expected_event)
    end

  end

end
