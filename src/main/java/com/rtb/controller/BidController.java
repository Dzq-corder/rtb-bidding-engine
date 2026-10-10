package com.rtb.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;
import com.rtb.dto.BidRequest;
import com.rtb.service.BidService;

/**
 * 竞价接口。
 *
 * <p>路径刻意保持与简历/演示手册一致：{@code POST /bid}。
 */
@Slf4j
@RestController
@RequiredArgsConstructor
public class BidController {

    private final BidService bidService;


    @PostMapping("/bid")
    public Map<String, Object> bid(@RequestBody BidRequest req) {
        long t0 = System.nanoTime();
        BidRequest.Result result = bidService.bid(req);
        long wall = (System.nanoTime() - t0) / 1_000_000;

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("code", 0);
        resp.put("msg", "ok");
        resp.put("data", result);
        // 接口整体墙钟耗时，与埋点里的 cost.total 区分开：
        // 后者是链路内部累计，前者含序列化和网络写出
        resp.put("wallMs", wall);
        return resp;
    }

}
