require File.expand_path(File.dirname(__FILE__) + '/../spec_helper')

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
  end

  describe :new_object_with_attrs do
    subject{ Tengine::Event.new(
        :event_type_name => :foo,
        :key => "hoge",
        :source_name => "server1",
        :occurred_at => Time.utc(2011,8,11,12,0),
        :properties => {:bar => "ABC", :baz => 999}
        )}
    it{ subject.should be_a(Tengine::Event) }
    its(:key){ should == "hoge" }
    its(:event_type_name){ should == "foo" }
    its(:source_name){ should == "server1" }
    its(:occurred_at){ should == Time.utc(2011,8,11,12,0) }
    its(:properties){ should == {'bar' => "ABC", 'baz' => 999}}
  end

end
