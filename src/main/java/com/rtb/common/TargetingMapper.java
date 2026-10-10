package com.rtb.common;

/**
 * 定向值映射：把请求里的原始画像值翻译成 ad_target.dim_value 的取值。
 *
 * <p>调用方（流量方）传的是原始值——年龄 28、性别 1；
 * 而 ad_target 里存的是分桶后的值——25-30、male。
 * 映射规则放在服务端而不是让各调用方各写一套，
 * 是为了让 ad_target 的分桶口径有唯一权威来源。
 *
 * <p>分桶边界与造数脚本 {@code AGE_BUCKETS} 保持一致：
 * 18-24 / 25-30 / 31-35 / 36-40 / 41-45 / 46-50 / 51+
 */
public final class TargetingMapper {

    private TargetingMapper() {
    }

    private static final int[] AGE_LOW = {18, 25, 31, 36, 41, 46, 51};
    private static final int[] AGE_HIGH = {25, 31, 36, 41, 46, 51, Integer.MAX_VALUE};
    private static final String[] AGE_BUCKET = {
            "18-24", "25-30", "31-35", "36-40", "41-45", "46-50", "51+"};

    /**
     * 年龄转年龄段。超出范围时归到最接近的桶，不返回 null。
     */
    public static String toAgeBucket(Integer age) {
        if (age == null) {
            return null;
        }
        for (int i = 0; i < AGE_LOW.length; i++) {
            if (age >= AGE_LOW[i] && age < AGE_HIGH[i]) {
                return AGE_BUCKET[i];
            }
        }
        return age < AGE_LOW[0] ? AGE_BUCKET[0] : AGE_BUCKET[AGE_BUCKET.length - 1];
    }

    /**
     * 性别码转 ad_target 取值：1 -> male，2 -> female，其余 -> unknown。
     */
    public static String toGender(Integer gender) {
        if (gender == null) {
            return null;
        }
        return switch (gender) {
            case 1 -> "male";
            case 2 -> "female";
            default -> "unknown";
        };
    }

    /**
     * 取最终的年龄段：显式传入 ageBucket 时以它为准，否则由 age 换算。
     */
    public static String resolveAgeBucket(Integer age, String explicit) {
        return explicit != null && !explicit.isBlank() ? explicit : toAgeBucket(age);
    }
}
