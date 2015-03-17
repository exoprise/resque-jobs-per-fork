# -*- encoding: utf-8 -*-

Gem::Specification.new do |s|
  s.name          = "resque-jobs-per-fork"
  s.homepage      = "http://github.com/samgranieri/resque-jobs-per-fork"
  s.summary       = "Have your resque workers process more that one job"
  s.description   = "When your resque jobs are frequent and fast, the overhead of forking and running your after_fork might get too big."
  s.require_path  = "lib"
  s.authors       = ["Sam Granieri", "Mick Staugaard"]
  s.email         = ["sam@samgranieri.com"]
  s.version       = "1.15.1"
  s.platform      = Gem::Platform::RUBY
  s.files         = Dir.glob("{lib,test}/**/*") + %w(LICENSE README.rdoc Rakefile)

  #removing dependency so it doesn't drag in real resque
  #s.add_dependency("resque", "~> 1.15")
end

