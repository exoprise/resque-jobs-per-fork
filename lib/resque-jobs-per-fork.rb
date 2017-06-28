require 'resque'
require 'resque/worker'

module ResqueJobsPerFork

  module WorkerExtension
  
    def perform(job)
      raise "You need to set JOBS_PER_FORK on the command line" unless (jobs_per_fork = ENV['JOBS_PER_FORK'])
      jobs_per_fork = jobs_per_fork.to_i
      sleeps_allowed = ENV['JOBS_PER_FORK_SLEEPS'].to_i
      sleeps_taken = 0
      # INTERVAL is already used by mongo-resque
      sleep_interval = Float(ENV['INTERVAL'] || 5.0)
      run_hook :before_perform_jobs_per_fork, self
      jobs_performed ||= 0
      while jobs_performed < jobs_per_fork do
        break if @shutdown
        if jobs_performed == 0
          super
        elsif another_job = reserve
          sleeps_taken = 0
          super(another_job)
        elsif sleeps_taken < sleeps_allowed
          sleeps_taken += 1
          sleep(sleep_interval)
          next
        else
          break # to prevent looping/hammering Redis with LPOPs
        end
        jobs_performed += 1
      end
      jobs_performed = nil
      run_hook :after_perform_jobs_per_fork, self
    end
  end

  def shutdown
    super
    quit_child if @child
  end

  # signals the forked child to exit after the current job
  def quit_child
    return unless @child
    log! "sending QUIT to child at #{@child}"
    Process.kill("QUIT", @child) rescue nil
  end
  
end

module Resque

  # the `before_perform_jobs_per_fork` hook will run in the child perform
  # right before the child perform starts
  #
  # Call with a block to set the hook.
  # Call with no arguments to return the hook.
  def self.before_perform_jobs_per_fork(&block)
    block ? (@before_perform_jobs_per_fork = block) : @before_perform_jobs_per_fork
  end

  # Set the before_perform_jobs_per_fork proc.
  def self.before_perform_jobs_per_fork=(before_perform_jobs_per_fork)
    @before_perform_jobs_per_fork = before_perform_jobs_per_fork
  end

  # the `after_perform_jobs_per_fork` hook will run in the child perform
  # right before the child perform terminates
  #
  # Call with a block to set the hook.
  # Call with no arguments to return the hook.
  def self.after_perform_jobs_per_fork(&block)
    block ? (@after_perform_jobs_per_fork = block) : @after_perform_jobs_per_fork
  end

  # Set the after_perform_jobs_per_fork proc.
  def self.after_perform_jobs_per_fork=(after_perform_jobs_per_fork)
    @after_perform_jobs_per_fork = after_perform_jobs_per_fork
  end

  Worker.prepend ResqueJobsPerFork::WorkerExtension
  
  # class Worker
  #
  #   def perform_with_jobs_per_fork(job)
  #     raise "You need to set JOBS_PER_FORK on the command line" unless (jobs_per_fork = ENV['JOBS_PER_FORK'])
  #     jobs_per_fork = jobs_per_fork.to_i
  #     sleeps_allowed = ENV['JOBS_PER_FORK_SLEEPS'].to_i
  #     sleeps_taken = 0
  #     # INTERVAL is already used by mongo-resque
  #     sleep_interval = Float(ENV['INTERVAL'] || 5.0)
  #     run_hook :before_perform_jobs_per_fork, self
  #     jobs_performed ||= 0
  #     while jobs_performed < jobs_per_fork do
  #       break if @shutdown
  #       if jobs_performed == 0
  #         perform_without_jobs_per_fork(job)
  #       elsif another_job = reserve
  #         sleeps_taken = 0
  #         perform_without_jobs_per_fork(another_job)
  #       elsif sleeps_taken < sleeps_allowed
  #         sleeps_taken += 1
  #         sleep(sleep_interval)
  #         next
  #       else
  #         break # to prevent looping/hammering Redis with LPOPs
  #       end
  #       jobs_performed += 1
  #     end
  #     jobs_performed = nil
  #     run_hook :after_perform_jobs_per_fork, self
  #   end
  #   alias_method :perform_without_jobs_per_fork, :perform
  #   alias_method :perform, :perform_with_jobs_per_fork
  # end
end
