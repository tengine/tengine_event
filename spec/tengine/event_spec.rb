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
    its(:occurred_at){ should_not be_empty }
  end
end
