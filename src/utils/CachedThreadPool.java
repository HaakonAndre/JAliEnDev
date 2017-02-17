package utils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Fully cached thread pool. New threads will be added until the capacity is reached, after which all tasks are accepted but queued.
 * 
 * @author costing
 * @since 2016-10-26
 */
public class CachedThreadPool extends ThreadPoolExecutor {
	static final class RejectingQueue extends LinkedBlockingQueue<Runnable> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean offer(final Runnable r) {
			if (size() <= 1)
				return super.offer(r);

			return false;
		}
	}

	static final class MyHandler implements RejectedExecutionHandler {
		@Override
		public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
			try {
				executor.getQueue().put(r);
			} catch (@SuppressWarnings("unused") InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}

		}
	}

	@SuppressWarnings("javadoc")
	public CachedThreadPool(final int maxPoolSize, final long timeout, final TimeUnit timeUnit) {
		super(0, maxPoolSize, timeout, timeUnit, new RejectingQueue(), new MyHandler());
	}

	@SuppressWarnings("javadoc")
	public CachedThreadPool(final int maxPoolSize, final long timeout, final TimeUnit timeUnit, final ThreadFactory factory) {
		super(0, maxPoolSize, timeout, timeUnit, new RejectingQueue(), factory, new MyHandler());
	}

}
