# -*- coding: utf-8 -*-
require File.expand_path(File.dirname(__FILE__) + '/../../spec_helper')
require 'amqp'

describe "Tengine::Event::Sender" do
  describe :initialize do
    before do
      # connection
      @mock_connection = mock(:connection)
      AMQP.should_receive(:connect).with({:user=>"guest", :pass=>"guest", :vhost=>"/",
          :logging=>false, :insist=>false, :host=>"localhost", :port=>5672}).and_return(@mock_connection)
      @mock_connection.should_receive(:on_tcp_connection_loss)
      @mock_connection.should_receive(:after_recovery)
    end

    context "mq_suite without config" do
      subject{ Tengine::Event::Sender.new.mq_suite }
      it{ subject.config[:sender].should_not be_empty }
      it{ subject.config[:connection].should_not be_empty }
      it{ subject.config[:exchange].should_not be_empty }
      it{ subject.config[:exchange][:name].should == "tengine_event_exchange" }
      it{ subject.config[:exchange][:publish].should == {:persistent => true} }
      it{ subject.config[:queue].should_not be_empty }
      it{ subject.config.keys.length.should == 4 }
    end

    context "mq_suite with config" do
      subject{ Tengine::Event::Sender.new(:exchange => {:name => "another_exhange"}).mq_suite }
      it{ subject.config[:exchange][:name].should == "another_exhange" }
    end
  end

  describe :fire do
    before do
      @mock_connection = mock(:connection)
      @mock_channel = mock(:channel)
      @mock_exchange = mock(:exchange)

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

      @sender = Tengine::Event::Sender.new(:exchange => {:name => "exchange1"})
    end

    it "JSON形式にserializeしてexchangeにpublishする" do
      expected_event = Tengine::Event.new(:event_type_name => :foo, :key => "uniq_key")
      @mock_exchange.should_receive(:publish).with(expected_event.to_json, :persistent => true)
      EM.should_receive(:add_timer).with(1)
      @sender.fire(:foo, :key => "uniq_key")
    end

    it "Tengine::Eventオブジェクトを直接指定することも可能" do
      expected_event = Tengine::Event.new(:event_type_name => :foo, :key => "uniq_key")
      @mock_exchange.should_receive(:publish).with(expected_event.to_json, :persistent => true)
      EM.should_receive(:add_timer).with(1)
      @sender.fire(expected_event)
    end
  end

  describe "fire_error" do
    context "AMQP::TCPConnectionFailed 以外のエラー" do
      before do
        @mock_connection = mock(:connection)
        @mock_channel = mock(:channel)
        @mock_exchange = mock(:exchange)

        # connection
        AMQP.should_receive(:connect).with({:user=>"guest", :pass=>"guest", :vhost=>"/",
            :logging=>false, :insist=>false, :host=>"localhost", :port=>5672}).and_return(@mock_connection)
        @mock_connection.should_receive(:on_tcp_connection_loss)
        @mock_connection.should_receive(:after_recovery)
        # channel
        AMQP::Channel.should_receive(:new).with(@mock_connection, {:prefetch => 1, :auto_recovery => true}).and_return(@mock_channel)
        # exchange
        AMQP::Exchange.should_receive(:new).with(@mock_channel, "direct", "exchange1",
          :passive=>false, :durable=>true, :auto_delete=>false, :internal=>false, :nowait=>true).and_return(@mock_exchange)

        # テスト実行時に1秒×30回、掛かるのは困るので、default値を変更しています。
        @sender = Tengine::Event::Sender.new(:exchange => {:name => "exchange1"}, :sender => {:retry_interval => 0})
      end

      it "エラーが発生しても設定のリトライが行われる" do
        expected_event = Tengine::Event.new(:event_type_name => :foo, :key => "uniq_key")
        lambda {
          # 正規のfireとリトライのfireなので、リトライ回数+1
          @mock_exchange.should_receive(:publish).with(expected_event.to_json, {:persistent=>true}).exactly(31).times.and_raise('error')
          @sender.fire(expected_event)
        }.should raise_error
      end

      it "エラーが発生してもオプションで指定したリトライ回数分のリトライが行われる" do
        expected_event = Tengine::Event.new(:event_type_name => :foo, :key => "uniq_key")
        lambda {
          # 正規のfireとリトライのfireなので、リトライ回数+1
          @mock_exchange.should_receive(:publish).with(expected_event.to_json, {:persistent=>true}).exactly(2).times.and_raise('error')
          @sender.fire(expected_event, :retry_count => 1)
        }.should raise_error
      end

      it "エラーが発生してもオプションで指定したリトライ間隔でリトライが行われる" do
        expected_event = Tengine::Event.new(:event_type_name => :foo, :key => "uniq_key")
        lambda {
          # 正規のfireとリトライのfireなので、リトライ回数+1
          @mock_exchange.should_receive(:publish).with(expected_event.to_json, {:persistent=>true}).exactly(4).times.and_raise('error')
          @sender.fire(expected_event, :retry_count => 3, :retry_interval => 0)
        }.should raise_error
      end
    end
  end

end
