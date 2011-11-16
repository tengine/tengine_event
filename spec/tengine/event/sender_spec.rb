# -*- coding: utf-8 -*-
require File.expand_path(File.dirname(__FILE__) + '/../../spec_helper')
require 'amqp'

describe "Tengine::Event::Sender" do
  describe :initialize do
    before do
      # connection
      @mock_connection = mock(:connection)
      AMQP.should_receive(:connect).with(an_instance_of(Hash)).and_return(@mock_connection)
      @mock_connection.should_receive(:on_tcp_connection_loss)
      @mock_connection.should_receive(:after_recovery)
      @mock_connection.should_receive(:on_closed)
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

    context "with mq_suite" do
      before{ @mq_suite = Tengine::Mq::Suite.new }
      it{ Tengine::Event::Sender.new( @mq_suite ) }
    end

    context "with mq_suite and options" do
      before{ @mq_suite = Tengine::Mq::Suite.new }
      it{ Tengine::Event::Sender.new( @mq_suite, :logger => mock(:logger) ) }
    end

  end

  describe :fire do
    before do
      @mock_connection = mock(:connection)
      @mock_channel = mock(:channel)
      @mock_exchange = mock(:exchange)

      # connection
      AMQP.should_receive(:connect).with(an_instance_of(Hash)).and_return(@mock_connection)
      @mock_connection.should_receive(:on_tcp_connection_loss)
      @mock_connection.should_receive(:after_recovery)
      @mock_connection.should_receive(:on_closed)
      @mock_connection.stub(:connected?).and_return(true)
      @mock_connection.stub(:disconnect).and_yield
      @mock_connection.stub(:server_capabilities).and_return(nil)
      # channel
      AMQP::Channel.should_receive(:new).with(@mock_connection, :prefetch => 1, :auto_recovery => true).and_return(@mock_channel)
      @mock_channel.stub(:publisher_index).and_return(nil)
      # exchange
      AMQP::Exchange.should_receive(:new).with(@mock_channel, "direct", "exchange1",
        :passive=>false, :durable=>true, :auto_delete=>false, :internal=>false, :nowait=>true).and_return(@mock_exchange)
    end

    context "正常系" do
      before do
        @sender = Tengine::Event::Sender.new(:exchange => {:name => "exchange1"})
        @sender.mq_suite.stub(:ensure_publisher_confirmation).and_yield
      end

      it "JSON形式にserializeしてexchangeにpublishする" do
        occurred_at = Time.now
        expected_event = Tengine::Event.new(:event_type_name => :foo, :key => "uniq_key", :occurred_at => occurred_at)
        @mock_exchange.should_receive(:publish).with(expected_event.to_json, :persistent => true)
        EM.run { @sender.fire(:foo, :key => "uniq_key", :occurred_at => occurred_at) }
      end

      it "Tengine::Eventオブジェクトを直接指定することも可能" do
        expected_event = Tengine::Event.new(:event_type_name => :foo, :key => "uniq_key")
        @mock_exchange.should_receive(:publish).with(expected_event.to_json, :persistent => true)
        EM.run { @sender.fire(expected_event) }
      end

      context "publish後に特定の処理を行う" do
        it "カスタム処理" do
          expected_event = Tengine::Event.new(:event_type_name => :foo, :key => "uniq_key")
          @mock_exchange.should_receive(:publish).with(expected_event.to_json, :persistent => true)
          block_called = false
          EM.run {
            @sender.fire(expected_event){ block_called = true }
          }
          block_called.should == true
        end

        it "自動で切断せずに、接続を維持する" do
          # mock connection ではテストが難しい
          expected_event = Tengine::Event.new(:event_type_name => :foo, :key => "uniq_key")
          @mock_exchange.should_receive(:publish).with(expected_event.to_json, :persistent => true)
          block_called = false
          EM.run {
            @sender.default_keep_connection = true
            @sender.fire(expected_event){ block_called = true }
            EM.add_timer(1) {
              @sender.mq_suite.connection.disconnect { EM.stop }
            }
          }
          block_called.should == true
        end
      end
    end

    context "AMQP::TCPConnectionFailed 以外のエラー" do
      before do
        # テスト実行時に1秒×30回、掛かるのは困るので、default値を変更しています。
        @sender = Tengine::Event::Sender.new(:exchange => {:name => "exchange1"}, :sender => {:retry_interval => 0})
      end
      it "メッセージ送信ができなくてpublishに渡したブロックが呼び出されず、インターバルが過ぎて、EM.add_timeに渡したブロックが呼び出された場合" do
        expected_event = Tengine::Event.new(:event_type_name => :foo, :key => "uniq_key")
        @mock_exchange.should_receive(:publish).with(expected_event.to_json, :persistent => true).exactly(31).times.and_raise(StandardError)
        EM.stub(:add_timer).with(0).exactly(31).times.and_yield
        EM.stub(:add_timer).with(0, an_instance_of(Proc)) {|k, v| v.call }
        block_called = false
        EM.run {
          @sender.fire(expected_event){ block_called = true }
          @sender.stop
        }
        block_called.should == false
      end

      it "エラーが発生しても設定のリトライが行われる" do
        expected_event = Tengine::Event.new(:event_type_name => :foo, :key => "uniq_key")
        EM.run {
          # 正規のfireとリトライのfireなので、リトライ回数+1
          @mock_exchange.should_receive(:publish).with(expected_event.to_json, {:persistent=>true}).exactly(31).times.and_raise('error')
          @sender.fire(expected_event)
          @sender.stop
        }
      end

      it "エラーが発生してもオプションで指定したリトライ回数分のリトライが行われる" do
        expected_event = Tengine::Event.new(:event_type_name => :foo, :key => "uniq_key")
        EM.run {
          # 正規のfireとリトライのfireなので、リトライ回数+1
          @mock_exchange.should_receive(:publish).with(expected_event.to_json, {:persistent=>true}).exactly(2).times.and_raise('error')
          @sender.fire(expected_event, :retry_count => 1)
          @sender.stop
        }
      end

      it "エラーが発生してもオプションで指定したリトライ間隔でリトライが行われる" do
        expected_event = Tengine::Event.new(:event_type_name => :foo, :key => "uniq_key")
        EM.run {
          # 正規のfireとリトライのfireなので、リトライ回数+1
          @mock_exchange.should_receive(:publish).with(expected_event.to_json, {:persistent=>true}).exactly(4).times.and_raise('error')
          @sender.fire(expected_event, :retry_count => 3, :retry_interval => 0)
          @sender.stop
        }
      end

      it "ちょうどretry_count回めのリトライして成功の場合は例外にならない" do
        expected_event = Tengine::Event.new(:event_type_name => :foo, :key => "uniq_key")
        x = false
        @mock_exchange.stub(:publish).with(expected_event.to_json, {:persistent=>true}) do
          if x = !x
            raise "foo"
          else
            next "foo"
          end
        end
        block_called = false
        EM.run {
          @sender.fire(expected_event, :retry_count => 1) { block_called = true }
          @sender.stop
        }
        block_called.should be_true
      end
    end

    context "入り乱れたfireにおけるretryの回数" do
      subject { Tengine::Event::Sender.new(:exchange => {:name => "exchange1"}, :sender => {:retry_interval => 0}) }
      it "https://www.pivotaltracker.com/story/show/20236589" do
        n1 = 0
        n2 = 0
        ev1 = Tengine::Event.new(:event_type_name => :foo, :key => "uniq_key")
        ev2 = Tengine::Event.new(:event_type_name => :foo, :key => "another_uniq_key")
        @mock_exchange.stub(:publish).with(ev1.to_json, {:persistent=>true}) do
          n1 += 1
          raise "ev1"
        end
        @mock_exchange.stub(:publish).with(ev2.to_json, {:persistent=>true}) do
          n2 += 1
          raise "ev2"
        end
        EM.run do
          subject.fire(ev1, :keep_connection => true)
          subject.fire(ev2, :keep_connection => true)
          subject.stop
        end
        if n1 == 31
          n2.should <= 31
          n2.should >= 2
        elsif n2 == 31
          n1.should <= 31
          n1.should >= 2
        else
          raise "neither n1(#{n1}) nor n2(#{n2})"
        end
      end

      it "無限にメモリを消費しない" do
        n = 256 # 1024 # 4096
        @mock_exchange.stub(:publish).with(an_instance_of(String), {:persistent=>true}).and_raise('error')
        EM.run do
          n.times do
            EM.next_tick do
              ev = Tengine::Event.new(:event_type_name => :foo, :key => "uniq_key")
              subject.fire(ev, :keep_connection => true, :retry_cont => 3)
            end
          end
          EM.next_tick do
            subject.stop
          end
        end
        GC.start
        subject.pending_events.size.should < n
      end
    end
  end

  describe :stop do
    subject { Tengine::Event::Sender.new(:exchange => {:name => "exchange1"}) }

    it "EMのイベントループを抜ける" do
      EM.run do
        sender.stop
      end
      # ここに到達すればOK
    end

    it "ペンディングのイベントが送信されるまではEMのイベントループにとどまる" do
      event = Tengine::Event.new(:event_type_name => :foo, :key => "uniq_key")
      x = false
      @mock_exchange.stub(:publish).with(expected_event.to_json, :persistent=>true) do
        if x = !x
          raise "foo"
        else
          next "foo"
        end
      end
      block_called = false
      EM.run {
        @sender.fire(expected_event, :retry_count => 1) { block_called = true }
        @sender.stop
      }
      block_called.should be_true
    end
  end
end
