package com.rtb.common;

/**
 * 业务异常：用于「请求本身合法、但业务上不允许」的场景。
 *
 * <p>与参数校验异常（{@code MethodArgumentNotValidException}）的分工：
 * 校验异常由 {@code @Valid} 在进入方法前抛出，一律 400；
 * 本异常由业务逻辑主动抛出，携带各自的 HTTP 语义状态码：
 * 404 资源不存在 / 409 状态冲突 / 400 业务参数不合法。
 *
 * <p>由 {@link GlobalExceptionHandler} 统一转成 {@link ApiResponse} 错误结构。
 */
public class BizException extends RuntimeException {

    /** 对应的 HTTP 状态码 */
    private final int status;

    private BizException(int status, String msg) {
        super(msg);
        this.status = status;
    }

    public int getStatus() {
        return status;
    }

    /** 404：资源不存在（如广告 ID 查不到） */
    public static BizException notFound(String msg) {
        return new BizException(404, msg);
    }

    /** 400：业务参数不合法（如新建广告却没带定向条件） */
    public static BizException badRequest(String msg) {
        return new BizException(400, msg);
    }

    /** 409：状态冲突（如对已删除/已耗尽的对象做不允许的状态流转） */
    public static BizException conflict(String msg) {
        return new BizException(409, msg);
    }
}
