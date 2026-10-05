package com.rtb.event;

import lombok.Data;

/**
 * 消费结果 · M6
 *
 * <p>三个消费组共用一个返回结构，但语义不同：
 * <ul>
 *   <li>{@code PROCESSED} —— 首次处理并落库</li>
 *   <li>{@code DUPLICATE_SKIPPED} —— 命中唯一键被跳过（幂等生效）</li>
 *   <li>{@code FAILED} —— 处理失败，<b>必须让位点不提交</b></li>
 * </ul>
 */
public final class ConsumeResult {

    public enum Action {
        PROCESSED,
        DUPLICATE_SKIPPED,
        FAILED
    }

    @Data
    public static class Result {
        private final Action action;
        private final long costMs;

        private Result(Action action, long costMs) {
            this.action = action;
            this.costMs = costMs;
        }

        public static Result processed(long costMs) {
            return new Result(Action.PROCESSED, costMs);
        }

        public static Result duplicate(long costMs) {
            return new Result(Action.DUPLICATE_SKIPPED, costMs);
        }

        public static Result failed(long costMs) {
            return new Result(Action.FAILED, costMs);
        }

        public boolean isOk() {
            return action != Action.FAILED;
        }
    }
}
