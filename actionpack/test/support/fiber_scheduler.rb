# frozen_string_literal: true

# A minimal fiber scheduler for tests, cut down from ruby/ruby's
# test/fiber/scheduler.rb.
class FiberScheduler
  def initialize
    @fiber = Fiber.current
    @readable = {}
    @writable = {}
    @waiting = {}
    @blocking = {}.compare_by_identity
    @ready = []
    @lock = Thread::Mutex.new
    @urgent = IO.pipe
  end

  def run
    while @readable.any? || @writable.any? || @waiting.any? || @blocking.any? || @ready.any?
      run_once
    end
  end

  def scheduler_close
    run
  ensure
    @urgent.each(&:close)
  end

  def fiber(&block)
    fiber = Fiber.new(blocking: false, &block)
    unblock(:fiber, Fiber.current) unless Fiber.current == @fiber
    fiber.transfer
    fiber
  end

  def block(_blocker, timeout = nil)
    fiber = Fiber.current

    if timeout
      @waiting[fiber] = current_time + timeout
    else
      @blocking[fiber] = true
    end

    @fiber.transfer
  ensure
    @waiting.delete(fiber)
    @blocking.delete(fiber)
  end

  def unblock(_blocker, fiber)
    @lock.synchronize { @ready << fiber }
    @urgent.last.write_nonblock(".")
  end

  def fiber_interrupt(fiber, _exception)
    unblock(nil, fiber)
  end

  def kernel_sleep(duration = nil)
    block(:sleep, duration)
    true
  end

  def io_wait(io, events, timeout)
    fiber = Fiber.current
    @readable[io] = fiber unless (events & IO::READABLE).zero?
    @writable[io] = fiber unless (events & IO::WRITABLE).zero?
    @waiting[fiber] = current_time + timeout if timeout

    @fiber.transfer
  ensure
    @readable.delete(io)
    @writable.delete(io)
    @waiting.delete(fiber)
  end

  private
    def run_once
      readable, writable = IO.select(@readable.keys + [@urgent.first], @writable.keys, [], next_timeout)

      selected = {}
      readable&.each do |io|
        if fiber = @readable.delete(io)
          selected[fiber] = IO::READABLE
        elsif io == @urgent.first
          @urgent.first.read_nonblock(1024)
        end
      end
      writable&.each do |io|
        if fiber = @writable.delete(io)
          selected[fiber] = selected.fetch(fiber, 0) | IO::WRITABLE
        end
      end
      selected.each { |fiber, events| fiber.transfer(events) }

      time = current_time
      @waiting.select { |_, timeout| timeout <= time }.each_key do |fiber|
        @waiting.delete(fiber)
        fiber.transfer if fiber.alive?
      end

      ready = @lock.synchronize { @ready.slice!(0..) }
      ready.each { |fiber| fiber.transfer if fiber.alive? }
    end

    def next_timeout
      if timeout = @waiting.values.min
        [timeout - current_time, 0].max
      end
    end

    def current_time
      Process.clock_gettime(Process::CLOCK_MONOTONIC)
    end
end
