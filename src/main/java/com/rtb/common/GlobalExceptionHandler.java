package com.rtb.common;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingServletRequestParameterException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import java.util.stream.Collectors;

/**
 * 全局异常处理：把未捕获异常统一收敛为 {@link ApiResponse} 错误结构，
 * 避免异常直接以 500 裸奔、返回体格式不统一。
 *
 * <ul>
 *   <li>{@code MethodArgumentNotValidException}（{@code @Valid} 校验失败）→ 400，聚合字段错误；</li>
 *   <li>{@code HttpMessageNotReadableException}（请求体缺失 / 非法 JSON）→ 400；</li>
 *   <li>{@code MissingServletRequestParameterException}（缺少 @RequestParam）→ 400；</li>
 *   <li>{@code MethodArgumentTypeMismatchException}（参数类型不匹配，如 ?group= 传数字）→ 400；</li>
 *   <li>{@code HttpRequestMethodNotSupportedException}（如用 GET 调 POST 接口）→ 405；</li>
 *   <li>其他未处理异常 → 500（不泄漏内部细节）。</li>
 * </ul>
 *
 * <p>前四个"框架异常"分支必须显式声明：{@code @ExceptionHandler} 的优先级高于 Spring 默认的
 * {@code DefaultHandlerExceptionResolver}，若只留一个 {@code Exception} 兜底，会把框架本该
 * 返回的 400/405 一并吞成 500。
 */
@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiResponse<Void>> onValidation(MethodArgumentNotValidException e) {
        String msg = e.getBindingResult().getFieldErrors().stream()
                .map(fe -> fe.getField() + " " + fe.getDefaultMessage())
                .collect(Collectors.joining("; "));
        return ResponseEntity.badRequest()
                .body(ApiResponse.error(HttpStatus.BAD_REQUEST.value(), msg));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiResponse<Void>> onUnreadable(HttpMessageNotReadableException e) {
        return ResponseEntity.badRequest()
                .body(ApiResponse.error(HttpStatus.BAD_REQUEST.value(), "请求体缺失或格式非法"));
    }

    @ExceptionHandler(MissingServletRequestParameterException.class)
    public ResponseEntity<ApiResponse<Void>> onMissingParam(MissingServletRequestParameterException e) {
        return ResponseEntity.badRequest()
                .body(ApiResponse.error(HttpStatus.BAD_REQUEST.value(),
                        "缺少必要参数 " + e.getParameterName()));
    }

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ResponseEntity<ApiResponse<Void>> onTypeMismatch(MethodArgumentTypeMismatchException e) {
        return ResponseEntity.badRequest()
                .body(ApiResponse.error(HttpStatus.BAD_REQUEST.value(),
                        "参数 " + e.getName() + " 类型非法"));
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<ApiResponse<Void>> onMethodNotSupported(HttpRequestMethodNotSupportedException e) {
        return ResponseEntity.status(HttpStatus.METHOD_NOT_ALLOWED)
                .body(ApiResponse.error(HttpStatus.METHOD_NOT_ALLOWED.value(),
                        "不支持的请求方法 " + e.getMethod()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiResponse<Void>> onError(Exception e) {
        log.error("未处理异常: {}", e.getMessage(), e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(ApiResponse.error(HttpStatus.INTERNAL_SERVER_ERROR.value(), "服务内部错误"));
    }
}
