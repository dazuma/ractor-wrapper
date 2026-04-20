# frozen_string_literal: true

require "helper"

# Stand-in for FiberReturnMessage / FiberExceptionMessage that exposes a
# fiber_id without the rest of the protocol baggage.
FiberResume = ::Struct.new(:fiber_id, :payload, keyword_init: true)

describe ::Ractor::Wrapper::Server::Dispatcher do
  include TimeoutHelper

  let(:dispatcher) { ::Ractor::Wrapper::Server::Dispatcher.new(2) }

  describe "shared queue" do
    it "delivers an enqueued call to a worker that accepts calls" do
      dispatcher.enqueue_call(:msg)
      kind, msg = dispatcher.dequeue(0, accept_calls: true)
      assert_equal(:call, kind)
      assert_equal(:msg, msg)
    end

    it "blocks a worker when the shared queue is empty" do
      thread = ::Thread.new { dispatcher.dequeue(0, accept_calls: true) }
      sleep 0.1
      assert(thread.alive?, "worker should be blocked on empty dispatcher")
      dispatcher.enqueue_call(:msg)
      with_timeout(2) { thread.join }
      assert_equal([:call, :msg], thread.value)
    end

    it "ignores the shared queue when accept_calls is false" do
      dispatcher.enqueue_call(:msg)
      thread = ::Thread.new { dispatcher.dequeue(0, accept_calls: false) }
      sleep 0.1
      assert(thread.alive?, "worker should not have taken the call")
      # Unblock by registering a fiber and pushing a resume so the test exits.
      dispatcher.register_fiber(42, 0)
      dispatcher.enqueue_fiber_resume(FiberResume.new(fiber_id: 42, payload: :resume))
      with_timeout(2) { thread.join }
      assert_equal(:resume, thread.value.first)
    end
  end

  describe "per-worker queue" do
    it "routes a fiber resume to the worker that owns the fiber" do
      dispatcher.register_fiber(7, 1)
      dispatcher.enqueue_fiber_resume(FiberResume.new(fiber_id: 7, payload: :hello))
      kind, msg = dispatcher.dequeue(1, accept_calls: false)
      assert_equal(:resume, kind)
      assert_equal(7, msg.fiber_id)
      assert_equal(:hello, msg.payload)
    end

    it "does not deliver a fiber resume to a non-owning worker" do
      dispatcher.register_fiber(7, 1)
      dispatcher.enqueue_fiber_resume(FiberResume.new(fiber_id: 7, payload: :hello))
      thread = ::Thread.new { dispatcher.dequeue(0, accept_calls: false) }
      sleep 0.1
      assert(thread.alive?, "worker 0 should not have taken worker 1's resume")
      dispatcher.register_fiber(99, 0)
      dispatcher.enqueue_fiber_resume(FiberResume.new(fiber_id: 99, payload: :other))
      with_timeout(2) { thread.join }
      kind, msg = thread.value
      assert_equal(:resume, kind)
      assert_equal(99, msg.fiber_id)
    end

    it "prioritizes the per-worker queue over the shared queue" do
      dispatcher.register_fiber(5, 0)
      dispatcher.enqueue_call(:call_msg)
      dispatcher.enqueue_fiber_resume(FiberResume.new(fiber_id: 5, payload: :resume_msg))
      kind, msg = dispatcher.dequeue(0, accept_calls: true)
      assert_equal(:resume, kind)
      assert_equal(5, msg.fiber_id)
      # The call is still available for a subsequent dequeue.
      kind2, msg2 = dispatcher.dequeue(0, accept_calls: true)
      assert_equal(:call, kind2)
      assert_equal(:call_msg, msg2)
    end

    it "returns false when enqueueing a resume for an unregistered fiber" do
      result = dispatcher.enqueue_fiber_resume(FiberResume.new(fiber_id: 999, payload: :nope))
      assert_equal(false, result)
    end

    it "no-ops when unregistering an unknown fiber" do
      dispatcher.unregister_fiber(123) # should not raise
      assert_equal(false,
                   dispatcher.enqueue_fiber_resume(FiberResume.new(fiber_id: 123, payload: :x)))
    end

    it "stops routing resumes for a fiber after it is unregistered" do
      dispatcher.register_fiber(8, 0)
      dispatcher.unregister_fiber(8)
      assert_equal(false,
                   dispatcher.enqueue_fiber_resume(FiberResume.new(fiber_id: 8, payload: :x)))
    end
  end

  describe "close" do
    it "rejects further enqueue_call after close" do
      dispatcher.close
      assert_equal(false, dispatcher.enqueue_call(:nope))
    end

    it "wakes a blocked worker with a closed signal" do
      thread = ::Thread.new { dispatcher.dequeue(0, accept_calls: true) }
      sleep 0.1
      assert(thread.alive?)
      dispatcher.close
      with_timeout(2) { thread.join }
      assert_equal([:closed, nil], thread.value)
    end

    it "delivers the closed signal to each worker exactly once" do
      dispatcher.close
      assert_equal([:closed, nil], dispatcher.dequeue(0, accept_calls: true))
      assert_equal([:closed, nil], dispatcher.dequeue(1, accept_calls: true))
      # A subsequent dequeue from worker 0 should not see another :closed; it
      # should block waiting for resumes (we time out and assert blocking).
      thread = ::Thread.new { dispatcher.dequeue(0, accept_calls: false) }
      sleep 0.1
      assert(thread.alive?, "worker should block after consuming its closed signal")
      thread.kill
      thread.join
    end

    it "drains the shared queue and returns its contents on close" do
      dispatcher.enqueue_call(:a)
      dispatcher.enqueue_call(:b)
      drained = dispatcher.close
      assert_equal([:a, :b], drained)
    end

    it "returns the closed signal in preference to never-dequeued shared calls" do
      # If close is called while items remain in the shared queue, those items
      # are drained by close itself (verified above). After close, the worker
      # should see :closed, not a stale call from the shared queue.
      dispatcher.enqueue_call(:stale)
      dispatcher.close
      assert_equal([:closed, nil], dispatcher.dequeue(0, accept_calls: true))
    end

    it "still delivers per-worker resumes after close" do
      dispatcher.register_fiber(11, 0)
      dispatcher.close
      assert_equal([:closed, nil], dispatcher.dequeue(0, accept_calls: true))
      dispatcher.enqueue_fiber_resume(FiberResume.new(fiber_id: 11, payload: :late))
      kind, msg = dispatcher.dequeue(0, accept_calls: false)
      assert_equal(:resume, kind)
      assert_equal(11, msg.fiber_id)
    end

    it "is idempotent" do
      dispatcher.close
      assert_equal([], dispatcher.close)
    end
  end

  describe "crash_close" do
    it "wakes a blocked worker with a terminate signal even with no pending resume" do
      thread = ::Thread.new { dispatcher.dequeue(0, accept_calls: false) }
      sleep 0.1
      assert(thread.alive?, "worker should be blocked")
      dispatcher.crash_close
      with_timeout(2) { thread.join }
      assert_equal([:terminate, nil], thread.value)
    end

    it "drains the per-worker queue before returning terminate" do
      dispatcher.register_fiber(7, 0)
      dispatcher.enqueue_fiber_resume(FiberResume.new(fiber_id: 7, payload: :first))
      dispatcher.crash_close
      assert_equal([:resume, FiberResume.new(fiber_id: 7, payload: :first)],
                   dispatcher.dequeue(0, accept_calls: false))
      assert_equal([:terminate, nil], dispatcher.dequeue(0, accept_calls: false))
    end

    it "returns terminate repeatedly (not just once)" do
      dispatcher.crash_close
      assert_equal([:terminate, nil], dispatcher.dequeue(0, accept_calls: false))
      assert_equal([:terminate, nil], dispatcher.dequeue(0, accept_calls: false))
    end

    it "drains the shared queue and returns its contents on first crash_close" do
      dispatcher.enqueue_call(:a)
      dispatcher.enqueue_call(:b)
      drained = dispatcher.crash_close
      assert_equal([:a, :b], drained)
    end

    it "rejects further enqueue_call after crash_close" do
      dispatcher.crash_close
      assert_equal(false, dispatcher.enqueue_call(:nope))
    end

    it "after crash_close, a worker that already saw closed gets terminate next" do
      dispatcher.close
      assert_equal([:closed, nil], dispatcher.dequeue(0, accept_calls: false))
      dispatcher.crash_close
      assert_equal([:terminate, nil], dispatcher.dequeue(0, accept_calls: false))
    end

    it "is idempotent and returns empty drain on subsequent calls" do
      dispatcher.crash_close
      assert_equal([], dispatcher.crash_close)
    end
  end

  describe "concurrency stress" do
    it "delivers every enqueued call to exactly one worker across producers" do
      d = ::Ractor::Wrapper::Server::Dispatcher.new(4)
      total = 200
      received = ::Queue.new
      workers = ::Array.new(4) do |worker_num|
        ::Thread.new do
          loop do
            kind, msg = d.dequeue(worker_num, accept_calls: true)
            break if kind == :closed
            received << msg if kind == :call
          end
        end
      end
      producers = ::Array.new(4) do |producer_num|
        ::Thread.new do
          (total / 4).times do |i|
            d.enqueue_call([producer_num, i])
          end
        end
      end
      with_timeout(5) { producers.each(&:join) }
      with_timeout(5) { sleep 0.05 until received.size == total }
      d.close
      with_timeout(5) { workers.each(&:join) }
      delivered = []
      delivered << received.pop until received.empty?
      assert_equal(total, delivered.size)
      assert_equal(delivered.uniq.size, delivered.size, "no message should be duplicated")
    end
  end
end
