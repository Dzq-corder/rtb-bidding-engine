package com.rtb.controller;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import com.rtb.common.ApiResponse;
import com.rtb.dto.BidRequest;
import com.rtb.service.BidService;

/**
 * 竞价接口。
 *
 * <p>路径刻意保持与简历/演示手册一致：{@code POST /bid}。
 *
 * <p>入参经 {@code @Valid} 校验（缺失 / 越界由 {@code GlobalExceptionHandler} 统一返回 400）；
 * 响应统一封装为 {@link ApiResponse}。{@code wallMs} 为接口整体墙钟耗时（含序列化与网络写出），
 * 与埋点里的 {@code cost.total}（链路内部累计）区分开。
 */
@RestController
@RequiredArgsConstructor
public class BidController {

    private final BidService bidService;

    @PostMapping("/bid")
    public ApiResponse<BidRequest.Result> bid(@Valid @RequestBody BidRequest req,
                                              HttpServletRequest httpReq) {
        // 频控兜底维度：未传 userId 时按客户端 IP 计数。
        // IP 只从连接上下文取，不接受调用方直传，避免伪造 IP 绕过频控。
        req.setClientIp(resolveClientIp(httpReq));

        long t0 = System.nanoTime();
        BidRequest.Result result = bidService.bid(req);
        long wall = (System.nanoTime() - t0) / 1_000_000;

        ApiResponse<BidRequest.Result> resp = ApiResponse.ok(result);
        resp.setWallMs((int) wall);
        return resp;
    }

    /**
     * 取客户端 IP：优先 {@code X-Forwarded-For} 首段（经过代理/负载均衡时），
     * 否则退回 {@code getRemoteAddr()}。
     */
    private static String resolveClientIp(HttpServletRequest request) {
        String xff = request.getHeader("X-Forwarded-For");
        if (xff != null && !xff.isBlank()) {
            int comma = xff.indexOf(',');
            return (comma > 0 ? xff.substring(0, comma) : xff).trim();
        }
        return request.getRemoteAddr();
    }
}
