package com.rtb.common;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

/**
 * 统一响应体：所有 HTTP 接口的返回外壳。
 *
 * <p>成功 {@code code=0, msg="ok", data=...}；失败 {@code code=HTTP状态码, msg=错误描述}。
 * {@code wallMs} 是竞价接口专用的可选墙钟耗时，其余接口因 {@link JsonInclude.Include#NON_NULL}
 * 不输出该字段。
 */
@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ApiResponse<T> {

    /** 0 = 成功；非 0 = 失败（取 HTTP 状态码，如 400 / 500） */
    private int code;

    /** 描述信息：成功为 "ok"，失败为具体原因 */
    private String msg;

    /** 业务数据 */
    private T data;

    /** 接口整体墙钟耗时（ms），仅竞价接口填充 */
    private Integer wallMs;

    public static <T> ApiResponse<T> ok(T data) {
        ApiResponse<T> r = new ApiResponse<>();
        r.code = 0;
        r.msg = "ok";
        r.data = data;
        return r;
    }

    public static <T> ApiResponse<T> error(int code, String msg) {
        ApiResponse<T> r = new ApiResponse<>();
        r.code = code;
        r.msg = msg;
        return r;
    }
}
