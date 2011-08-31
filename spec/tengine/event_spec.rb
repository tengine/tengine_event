require File.expand_path(File.dirname(__FILE__) + '/../spec_helper')

require 'time'

describe "Tengine::Event" do
  describe :new_object do
    subject{ Tengine::Event.new }
    it{ subject.should be_a(Tengine::Event) }
    its(:key){ should_not be_blank }
    its(:event_type_name){ should be_nil }
    its(:source_name){ should be_nil }
    its(:occurred_at){ should be_nil }
    its(:properties){ should be_a(Hash) }
    its(:properties){ should be_empty }
    it {
      attrs = subject.attributes
      attrs.should be_a(Hash)
      attrs.delete(:key).should_not be_nil
      attrs.should == {}
    }
  end

  describe :new_object_with_attrs do
    subject{ Tengine::Event.new(
        :event_type_name => :foo,
        :key => "hoge",
        'source_name' => "server1",
        :occurred_at => Time.utc(2011,8,11,12,0),
        :properties => {:bar => "ABC", :baz => 999}
        )}
    it{ subject.should be_a(Tengine::Event) }
    its(:key){ should == "hoge" }
    its(:event_type_name){ should == "foo" }
    its(:source_name){ should == "server1" }
    its(:occurred_at){ should == Time.utc(2011,8,11,12,0) }
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
  end

  describe :local_time_occurred_at do
    subject{ Tengine::Event.new(
        :occurred_at => Time.parse("2011-08-31 12:00:00 +0900")
        )}
    it{ subject.should be_a(Tengine::Event) }
    its(:occurred_at){ should == Time.utc(2011,8,31,3,0) }
    its(:occurred_at){ should be_utc }
  end

  it :attrs_for_new_must_be_Hash do
    expect{
      Tengine::Event.new("{foo: 1, bar: 2}")
    }.to raise_error(ArgumentError, /attrs must be a Hash but was/)
  end


  it :occurred_at_must_be_Time do
    expect{
      Tengine::Event.new(:occurred_at => "2011-08-31 12:00:00 +0900")
    }.to raise_error(ArgumentError, /occurred_at must be a Time but was/)
  end


end
