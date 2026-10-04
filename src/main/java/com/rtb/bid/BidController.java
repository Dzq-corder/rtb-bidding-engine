package com.rtb.bid;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

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

    /**
     * 压测入口：一次提交 N 次竞价，返回实际 QPS 与空广告率。
     *
     * <p>演示手册第 7 步用它，避免现场切 JMeter 把笔记本打满。
     * 刻意加了 {@code MAX_BATCH} 上限——演示时宁可少打也不要卡死。
     */
    @PostMapping("/bid/batch")
    public Map<String, Object> batch(@RequestParam(defaultValue = "1000") int count) {
        final int MAX_BATCH = 10000;
        int n = Math.min(count, MAX_BATCH);

        long start = System.nanoTime();
        int hasAd = 0;
        for (int i = 0; i < n; i++) {
            BidRequest.Result r = bidService.bid(randomRequest());
            if (Boolean.TRUE.equals(r.getHasAd())) {
                hasAd++;
            }
        }
        long elapsed = System.nanoTime() - start;

        double qps = n / (elapsed / 1_000_000_000.0);
        double emptyRate = n == 0 ? 0 : (n - hasAd) * 100.0 / n;

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("code", 0);
        resp.put("msg", "ok");
        resp.put("requested", n);
        resp.put("hitCapped", count > MAX_BATCH);
        resp.put("qps", Math.round(qps * 100) / 100.0);
        resp.put("fillRate", Math.round((100 - emptyRate) * 100) / 100.0);
        resp.put("emptyRate", Math.round(emptyRate * 100) / 100.0);
        resp.put("elapsedMs", elapsed / 1_000_000);
        return resp;
    }

    /**
     * 随机生成一个竞价请求。
     *
     * <p>分布直接对齐造数脚本的词表与权重，保证演示时看到的
     * 候选数/过滤数比例是可复现的。
     */
    private BidRequest randomRequest() {
        String[] regions = {"110000", "310000", "440100", "440300", "330100",
                "320100", "510100", "420100", "610100", "370200"};
        int[] ages = {24, 28, 32, 35, 40, 45, 52, 26, 30, 38};
        String[] categories = {"mobile", "fitness", "home", "beauty", "education",
                "auto", "food", "finance", "game", "travel"};
        String[] slots = {"feed_top", "feed_mid", "feed_bottom"};
        String[] interests = {"运动健身", "美妆护肤", "数码科技", "美食探店",
                "游戏电竞", "家居生活", "旅游出游"};

        var rnd = ThreadLocalRandom.current();
        BidRequest req = new BidRequest();
        req.setSlotCode(slots[rnd.nextInt(slots.length)]);
        req.setRegion(regions[rnd.nextInt(regions.length)]);
        req.setAge(ages[rnd.nextInt(ages.length)]);
        req.setGender(rnd.nextInt(10) < 5 ? 1 : 2);
        req.setInterests(interests[rnd.nextInt(interests.length)]);
        req.setCategoryCode(categories[rnd.nextInt(categories.length)]);
        return req;
    }
}
