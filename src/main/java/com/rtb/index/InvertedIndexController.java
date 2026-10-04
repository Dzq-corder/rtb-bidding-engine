package com.rtb.index;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 倒排索引诊断接口。
 *
 * <p>M2 排查用：验证「内存求交的结果」与「数据库 JOIN 的结果」是否一致。
 * 接口保留在代码里，演示时可以直接调给人看求交过程。
 */
@RestController
@RequiredArgsConstructor
public class InvertedIndexController {

    private final InvertedIndex index;

    @GetMapping("/index/status")
    public Map<String, Object> status() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ready", index.isReady());
        m.put("loadCostMs", index.getLoadCostMs());
        m.put("shape", index.getIndexShape());
        return m;
    }

    /**
     * 单个定向值的命中数，用来对照数据库的 COUNT 结果。
     *
     * <p>例：{@code /index/probe?dimType=1&dimValue=5104}
     * 返回的 ads 数应等于
     * {@code SELECT COUNT(DISTINCT ad_id) FROM ad_target WHERE dim_type=1 AND dim_value='5104'}
     */
    @GetMapping("/index/probe")
    public Map<String, Object> probe(@RequestParam int dimType,
                                     @RequestParam String dimValue) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("dimType", dimType);
        m.put("dimValue", dimValue);
        m.put("ads", index.hitCount(dimType, dimValue));
        return m;
    }

    /**
     * 多维求交，看每一步的结果。
     *
     * <p>例：{@code /index/intersect?region=5104&ageBucket=18-24&gender=female}
     */
    @GetMapping("/index/intersect")
    public Map<String, Object> intersect(
            @RequestParam(required = false) String region,
            @RequestParam(required = false) String ageBucket,
            @RequestParam(required = false) String gender) {
        return index.debugIntersect(region, ageBucket, gender);
    }
}
