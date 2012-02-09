# Generated by jeweler
# DO NOT EDIT THIS FILE DIRECTLY
# Instead, edit Jeweler::Tasks in Rakefile, and run 'rake gemspec'
# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name = "tengine_event"
  s.version = "0.4.6"

  s.required_rubygems_version = Gem::Requirement.new(">= 0") if s.respond_to? :required_rubygems_version=
  s.authors = ["taigou", "totty", "g-morita", "shyouhei", "akm"]
  s.date = "2012-02-09"
  s.description = "Tengine Event API to access the queue"
  s.email = "tengine@nautilus-technologies.com"
  s.executables = ["tengine_fire", "tengine_event_sucks"]
  s.extra_rdoc_files = [
    "README.rdoc"
  ]
  s.files = [
    ".document",
    ".rspec",
    "Gemfile",
    "Gemfile.lock",
    "README.rdoc",
    "Rakefile",
    "VERSION",
    "bin/tengine_event_sucks",
    "bin/tengine_fire",
    "lib/tengine/event.rb",
    "lib/tengine/event/model_notifiable.rb",
    "lib/tengine/event/sender.rb",
    "lib/tengine/mq.rb",
    "lib/tengine/mq/suite.rb",
    "lib/tengine/null_logger.rb",
    "lib/tengine_event.rb",
    "spec/.gitignore",
    "spec/mq_config.yml.example",
    "spec/spec_helper.rb",
    "spec/tengine/event/model_notifiable_spec.rb",
    "spec/tengine/event/sender_spec.rb",
    "spec/tengine/event_spec.rb",
    "spec/tengine/mq/connect_actually_spec.rb",
    "spec/tengine/mq/suite_spec.rb",
    "spec/tengine/null_logger_spec.rb",
    "spec/tengine_spec.rb",
    "tengine_event.gemspec"
  ]
  s.homepage = "https://github.com/tengine/tengine_event"
  s.licenses = ["MPL/LGPL"]
  s.require_paths = ["lib"]
  s.rubygems_version = "1.8.12"
  s.summary = "Tengine Event API to access the queue"

  if s.respond_to? :specification_version then
    s.specification_version = 3

    if Gem::Version.new(Gem::VERSION) >= Gem::Version.new('1.2.0') then
      s.add_runtime_dependency(%q<activesupport>, [">= 3.0.0"])
      s.add_runtime_dependency(%q<tengine_support>, [">= 0.3.24"])
      s.add_runtime_dependency(%q<uuid>, ["~> 2.3.4"])
      s.add_runtime_dependency(%q<amqp>, ["~> 0.8.0"])
      s.add_development_dependency(%q<rspec>, ["~> 2.6.0"])
      s.add_development_dependency(%q<yard>, ["~> 0.7.2"])
      s.add_development_dependency(%q<bundler>, ["~> 1.0.18"])
      s.add_development_dependency(%q<jeweler>, ["~> 1.6.4"])
      s.add_development_dependency(%q<simplecov>, ["~> 0.5.3"])
      s.add_development_dependency(%q<ZenTest>, ["~> 4.6.2"])
      s.add_development_dependency(%q<ci_reporter>, ["~> 1.6.5"])
    else
      s.add_dependency(%q<activesupport>, [">= 3.0.0"])
      s.add_dependency(%q<tengine_support>, [">= 0.3.24"])
      s.add_dependency(%q<uuid>, ["~> 2.3.4"])
      s.add_dependency(%q<amqp>, ["~> 0.8.0"])
      s.add_dependency(%q<rspec>, ["~> 2.6.0"])
      s.add_dependency(%q<yard>, ["~> 0.7.2"])
      s.add_dependency(%q<bundler>, ["~> 1.0.18"])
      s.add_dependency(%q<jeweler>, ["~> 1.6.4"])
      s.add_dependency(%q<simplecov>, ["~> 0.5.3"])
      s.add_dependency(%q<ZenTest>, ["~> 4.6.2"])
      s.add_dependency(%q<ci_reporter>, ["~> 1.6.5"])
    end
  else
    s.add_dependency(%q<activesupport>, [">= 3.0.0"])
    s.add_dependency(%q<tengine_support>, [">= 0.3.24"])
    s.add_dependency(%q<uuid>, ["~> 2.3.4"])
    s.add_dependency(%q<amqp>, ["~> 0.8.0"])
    s.add_dependency(%q<rspec>, ["~> 2.6.0"])
    s.add_dependency(%q<yard>, ["~> 0.7.2"])
    s.add_dependency(%q<bundler>, ["~> 1.0.18"])
    s.add_dependency(%q<jeweler>, ["~> 1.6.4"])
    s.add_dependency(%q<simplecov>, ["~> 0.5.3"])
    s.add_dependency(%q<ZenTest>, ["~> 4.6.2"])
    s.add_dependency(%q<ci_reporter>, ["~> 1.6.5"])
  end
end

