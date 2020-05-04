package org.apache.carbon.flink;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;

import java.util.concurrent.Semaphore;

public abstract class CarbonCommitSemaphore {

  private static final Object LOCK = new Object();

  private static volatile CarbonCommitSemaphore instance;

  public static CarbonCommitSemaphore get() {
    if (instance == null) {
      synchronized (LOCK) {
        if (instance == null) {
          instance = newInstance();
        }
      }
    }
    return instance;
  }

  private static CarbonCommitSemaphore newInstance() {
    final String permitsConfig =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.FLINK_COMMIT_PERMITS);
    final Integer permits;
    if (permitsConfig == null) {
      permits = null;
    } else {
      try {
        permits = Integer.valueOf(permitsConfig);
      } catch (NumberFormatException exception) {
        throw new IllegalArgumentException(
            "Property [" + CarbonCommonConstants.FLINK_COMMIT_PERMITS + "] format error. " +
                permitsConfig
        );
      }
      if (permits < 1) {
        throw new IllegalArgumentException(
            "Property [" + CarbonCommonConstants.FLINK_COMMIT_PERMITS + "] is less than 1. " +
                permits
        );
      }
    }
    if (permits == null) {
      return new CarbonCommitSemaphore() {
        @Override
        public void acquire() {
          // to do nothing.
        }

        @Override
        public void release() {
          // to do nothing.
        }
      };
    } else {
      final Semaphore semaphore = new Semaphore(permits);
      return new CarbonCommitSemaphore() {
        @Override
        public void acquire() throws InterruptedException {
          semaphore.acquire();
        }

        @Override
        public void release() {
          semaphore.release();
        }
      };
    }
  }

  public abstract void acquire() throws InterruptedException;

  public abstract void release();

}
