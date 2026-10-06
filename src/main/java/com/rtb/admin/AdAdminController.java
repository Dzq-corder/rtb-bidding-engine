package com.rtb.admin;

import com.rtb.ad.Ad;
import com.rtb.ad.AdMapper;
import com.rtb.budget.BudgetService;
import com.rtb.event.FreqAntifraudConsumer;
import com.rtb.event.HourlyPreaggConsumer;
import com.rtb.index.InvertedIndex;
import com.rtb.log.BidLogBuffer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 投放管理端 · M9
 *
 * <p><b>这个类解决的是一个演示层的问题，但背后是三个真实的工程问题</b>：
 * <ol>
 *   <li><b>缓存一致性</b>——改预算 / 改定向要「保存即生效」，
 *       而 M2 把 50 万广告与 596 万定向全缓存进了内存，改库不够</li>
 *   <li><b>演示可复位</b>——演示第 5 步必然把预算耗尽，
 *       没有重置接口就得靠手工 SQL，演示前手抖一次就翻车</li>
 *   <li><b>可观测优于可信任</b>——「解耦」「热更新」都得能现场展示</li>
 * </ol>
 *
 * <p><b>为什么不在演示前手工改库</b>：面试现场手敲 SQL 改预算，
 * 观众注意力会从系统转到你的手速上，而且一旦输错就是静默失败。
 * 一个按钮能点出「已生效 · 服务未重启」这句话，是演示可信度的一部分。
 */
@Slf4j
@RestController
@RequestMapping("/admin")
public class AdAdminController {

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");

    /**
     * 演示广告 A。
     *
     * <p><b>为什么写死一个 ID 而不是「取预算最小的那条」</b>：
     * 动态挑选每次运行可能选中不同广告，截图和讲稿就对不上了。
     * 固定 ID 让「A 的预算被扣光」这件事可复现、可提前准备。
     */
    private static final long DEMO_AD_ID = 88001L;

    /**
     * 演示重置后的预算。
     *
     * <p><b>为什么是 300 而不是手册原先写的 20</b>——这是 M9 实测踩出来的：
     * <ul>
     *   <li>演示画像下第 2 名出价 17.43，二价规则下单次扣 <b>17.44 元</b>
     *       （= 次高价 + 0.01，与胜出者自己报多少无关）</li>
     *   <li>预算给 20 元的话，A <b>一次就扣掉 87%</b>，
     *       第二次刷新直接余额不足返回空——不是「逐渐消失」，是「刷两次就没了」</li>
     *   <li>手册第 5 步要刷 15~20 次，预算必须 ≥ 17.44 × 15 ≈ 262 元。
     *       取 300 元 ≈ 17 次，正落在区间内</li>
     * </ul>
     *
     * <p><b>这个坑的教训比数字本身更值得记</b>：
     * 手册上「改成 20 元」这个数字是当初凭直觉写的，
     * 没有人核对过「单次扣费到底是多少」。
     * 而二价规则有个反直觉的性质——<b>抬高胜出者出价不会提高扣费</b>，
     * 所以直觉上「多给点预算撑住」和「把出价抬高点」这两个动作都不管用，
     * 唯一的杠杆是「减少同画像下的竞争者」或「直接给够预算」。
     */
    private static final BigDecimal DEMO_BUDGET = new BigDecimal("300.00");

    private final AdMapper adMapper;
    private final InvertedIndex invertedIndex;
    private final BudgetService budgetService;
    private final StringRedisTemplate redis;
    private final HourlyPreaggConsumer preagg;
    private final FreqAntifraudConsumer freq;
    private final BidLogBuffer logBuffer;

    public AdAdminController(AdMapper adMapper,
                             InvertedIndex invertedIndex,
                             BudgetService budgetService,
                             StringRedisTemplate redis,
                             HourlyPreaggConsumer preagg,
                             FreqAntifraudConsumer freq,
                             BidLogBuffer logBuffer) {
        this.adMapper = adMapper;
        this.invertedIndex = invertedIndex;
        this.budgetService = budgetService;
        this.redis = redis;
        this.preagg = preagg;
        this.freq = freq;
        this.logBuffer = logBuffer;
    }

    // =================================================================
    // 一、演示重置
    // =================================================================

    /**
     * 重置演示数据。
     *
     * <p><b>演示手册第 5 步必然把 A 的预算扣光</b>，所以每场面试前都要重置。
     * 手工改库的问题：既要改 MySQL 的 {@code daily_budget}，
     * 还要删 Redis 里当天的预算余额 key，<b>两步漏一步就不对</b>——
     * 只改库的话 Redis 里还留着旧余额，扣减永远扣不穿，A 不会消失。
     *
     * <p>这里把三件事一次做完，并<b>每一步都返回实际结果</b>：
     * 演示时可以念出「MySQL 已改、Redis 余额已清、内存缓存已刷新」，
     * 这比「重置了」三个字有说服力得多。
     */
    @PostMapping("/demo/reset")
    public Map<String, Object> resetDemo(@RequestParam(required = false) Long adId) {
        long target = adId == null ? DEMO_AD_ID : adId;
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("adId", target);

        Ad before = adMapper.findById(target);
        if (before == null) {
            out.put("error", "广告不存在：" + target);
            return out;
        }
        out.put("before", Map.of(
                "dailyBudget", before.getDailyBudget(),
                "status", before.getStatus()));

        // 1. MySQL：预算回 500，状态从「已耗尽」拉回「在投」
        int rows = adMapper.resetBudgetForDemo(target, DEMO_BUDGET);
        out.put("dbUpdated", rows);

        // 2. Redis：删掉当天的余额 key
        //    不删的话下一次 getBalance 会读到旧余额（可能是 0），
        //    而 Lua 脚本只在 key 不存在时才用 daily_budget 初始化
        String redisKey = "budget:" + target + ":" + LocalDate.now().format(DATE_FMT);
        Boolean deleted = redis.delete(redisKey);
        out.put("redisKey", redisKey);
        out.put("redisCleared", Boolean.TRUE.equals(deleted));

        // 3. 内存：刷新 Ad 缓存，让扣减用上新预算
        Ad after = adMapper.findById(target);
        invertedIndex.replaceAd(after);
        out.put("cacheRefreshed", true);
        out.put("after", Map.of(
                "dailyBudget", after.getDailyBudget(),
                "status", after.getStatus()));

        // 4. 自检：A 在演示画像下到底能不能赢
        //
        // 【为什么重置时必须带这一步】
        // 演示手册第 5 步是「连刷到 A 消失」。但 M9 实测发现，
        // A 的出价 1.28 在演示画像下排 20 名开外——<b>它从来没赢过</b>，
        // 「消失」这个现象根本不会发生。
        // 而当时重置接口只管把预算改回去，不管出价够不够高，
        // 于是每场面试都会在一个不成立的前提上开始。
        //
        // 把自检放在重置里，是因为「重置」这个词对使用者的含义是
        // 「回到可演示状态」。只重置预算却不检查前提是否成立，
        // 那不叫可演示状态。修复见 sql/m9_demo_ad_fix.sql。
        out.put("selfCheck", selfCheck(target, after));

        log.warn("演示数据已重置：adId={} 预算回到 {} 元（演示手册要求每场面试前执行）",
                target, DEMO_BUDGET);
        out.put("hint", "A 广告已复位。回到 app.html 连刷几次就能看到它重新出现。");
        return out;
    }

    /**
     * 检查演示广告在自己的定向组合下是不是最高出价。
     *
     * <p><b>为什么用「数一数有几条比它贵」而不是「跑一次竞价看结果」</b>：
     * 跑竞价只能看到一次抽样的结果，而演示要求的是
     * 「15~20 次连刷内必然耗尽」——这是稳定性质，单次成功说明不了。
     * 直接数「比我贵的有几条」是确定性判据：0 条才代表它稳赢。
     *
     * <p>用内存倒排索引求交而不是 SQL JOIN：
     * 596 万行 ad_target 上做三表 JOIN 会超时（实测过）。
     * 而索引本来就在内存里，纳秒级。
     */
    private Map<String, Object> selfCheck(long adId, Ad ad) {
        Map<String, Object> out = new LinkedHashMap<>();
        Set<String> keys = invertedIndex.targetingKeysOf(adId);
        List<String[]> conds = new ArrayList<>();
        for (String k : keys) {
            int sp = k.indexOf(' ');
            conds.add(new String[]{k.substring(0, sp), k.substring(sp + 1)});
        }
        if (conds.isEmpty()) {
            out.put("winnable", false);
            out.put("reason", "内存索引里没有它的定向项，竞价根本不会检索到它");
            return out;
        }

        int higher = 0;
        int total = 0;
        try {
            List<Ad> matched = invertedIndex.intersect(conds);
            total = matched.size();
            for (Ad m : matched) {
                // ⚠️ 不能写 m.getId() == adId：
                //    getId() 返回 Long（对象），adId 是 long（基本类型），
                //    对象与基本类型比较会自动拆箱，这次恰好能编译通过，
                //    但语义上依赖的是「拆箱后比值」而不是比对象——写法本身有歧义，
                //    显式比 long 值更清楚。
                if (m.getId() == null || m.getId().longValue() == adId) {
                    continue;
                }
                if (ad.getBid() == null || m.getBid() == null
                        || m.getBid().compareTo(ad.getBid()) > 0) {
                    higher++;
                }
            }
        } catch (Exception e) {
            out.put("winnable", null);
            out.put("reason", "求交失败：" + e.getMessage());
            return out;
        }

        out.put("bid", ad.getBid());
        out.put("matched", total);
        out.put("higherBidCount", higher);
        out.put("winnable", higher == 0);
        if (higher > 0) {
            out.put("reason", "有 " + higher + " 条广告出价高于它，它赢不了任何一次竞价——"
                    + "演示第 5 步「刷到它消失」不会发生。"
                    + "修复见 sql/m9_demo_ad_fix.sql");
        } else {
            out.put("reason", "它在 " + total + " 条候选里出价最高，"
                    + "连刷会稳定扣它的预算，耗尽现象可复现");
        }
        return out;
    }

    /**
     * 一键清空所有观测计数器。
     *
     * <p><b>为什么要单独提供</b>：压测和演示都会让
     * {@code enqueued / droppedEmpty / flushedRows} 这些计数不断增长。
     * 现场要讲「队列满了丢了 1,440 条低价值明细」时，
     * 需要一个干净的起点，否则只能说「到目前为止一共丢了多少」——
     * 那句话里的「本轮」就无从证明了。
     */
    @PostMapping("/metrics/reset")
    public Map<String, Object> resetMetrics() {
        logBuffer.resetCounters();
        return Map.of(
                "logBuffer", "cleared",
                "note", "明细队列计数已清零。Kafka 侧计数无法清零（producer 内部累计），"
                        + "重置服务才会归零");
    }

    // =================================================================
    // 二、消费组控制（演示第 6 步）
    // =================================================================

    /**
     * 暂停 / 恢复下游消费组。
     *
     * <p><b>为什么只支持预聚合与频次，不支持计费组</b>：
     * 计费组是「金钱事实不可丢」的那一路，暂停它等于制造资损，
     * 演示时也没有「停掉计费」这个场景。刻意不提供这个能力——
     * <b>接口设计上就要把危险操作挡掉，而不是靠使用方记得别点</b>。
     */
    @PostMapping("/consumer/{group}")
    public Map<String, Object> controlConsumer(@PathVariable String group,
                                               @RequestParam boolean pause) {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("group", group);
        out.put("action", pause ? "pause" : "resume");

        long rowsBefore = preagg.getFlushedRows();
        out.put("preaggFlushedRowsBefore", rowsBefore);

        switch (group) {
            case "hourly-preagg" -> {
                preagg.setPaused(pause);
                if (!pause) {
                    // 恢复时立刻把攒着的数据刷掉，
                    // 否则演示要等下一个 flush 周期才看到数字变化
                    preagg.flush();
                }
                out.put("paused", preagg.isPaused());
            }
            case "freq-antifraud" -> {
                freq.setPaused(pause);
                out.put("paused", freq.isPaused());
                out.put("processedBefore", freq.getProcessedCount());
            }
            case "billing-recon" -> {
                out.put("error", "计费消费组不支持暂停");
                out.put("reason", "billing_event 是扣费事实，暂停它等于制造资损。"
                        + "接口层面直接拒绝，而不是靠使用方记得别点");
                return out;
            }
            default -> {
                out.put("error", "未知消费组：" + group);
                out.put("allowed", List.of("hourly-preagg", "freq-antifraud"));
                return out;
            }
        }

        out.put("preaggFlushedRowsAfter", preagg.getFlushedRows());
        out.put("note", pause
                ? "判定依据是 preaggFlushedRows 停没停住，不是 lag——"
                  + "暂停时本组仍 ack，lag 恒为 0（刻意设计，见 EventAdminController 注释）"
                : "恢复后积压会被追平，等几秒再刷新本页面看数字增长");
        log.warn("消费组 {} 已{}（演示操作）", group, pause ? "暂停" : "恢复");
        return out;
    }

    // =================================================================
    // 三、广告管理：改预算 / 改出价 / 改状态 / 改定向
    // =================================================================

    /**
     * 管理端广告列表。
     *
     * <p>返回 {@code cached} 字段标出这条广告是否在内存索引里——
     * 演示时可以顺手验证「改了库里就一定改了内存」这条断言。
     */
    @GetMapping("/ads")
    public Map<String, Object> listAds(@RequestParam(defaultValue = "50") int limit) {
        List<Ad> ads = adMapper.listTopByBid(Math.min(Math.max(limit, 1), 200));
        List<Map<String, Object>> rows = new ArrayList<>(ads.size());
        for (Ad a : ads) {
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("id", a.getId());
            r.put("advertiserId", a.getAdvertiserId());
            r.put("title", a.getTitle());
            r.put("categoryCode", a.getCategoryCode());
            r.put("bid", a.getBid());
            r.put("dailyBudget", a.getDailyBudget());
            r.put("status", a.getStatus());
            r.put("cached", invertedIndex.cachedAd(a.getId()) != null);
            r.put("balance", safeBalance(a));
            r.put("targeting", invertedIndex.targetingKeysOf(a.getId()));
            rows.add(r);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("total", rows.size());
        out.put("ads", rows);
        out.put("demoAdId", DEMO_AD_ID);
        return out;
    }

    /**
     * 改日预算 —— 保存即生效。
     *
     * <p><b>返回体里的 serviceRestarted 永远为 false，这是刻意暴露给面试官看的</b>：
     * 演示手册第 4 步的台词是「注意我没有重启服务」，
     * 界面上直接把这个事实显示出来，比口头强调可信。
     *
     * <p><b>为什么改了库还必须改内存</b>：
     * {@code BidService} 扣费用的是 {@code Ad} 对象里的 {@code dailyBudget}，
     * 而那个对象是 M2 启动时从库里读进 {@code adCache} 的。
     * 只 UPDATE 数据库，扣减逻辑仍然拿着旧值——
     * 界面显示「预算已改为 20」，行为却还是按 500 算，这叫假生效。
     */
    @PostMapping("/ads/{id}/budget")
    public Map<String, Object> updateBudget(@PathVariable Long id,
                                            @RequestParam BigDecimal dailyBudget) {
        Map<String, Object> out = new LinkedHashMap<>();
        Ad before = adMapper.findById(id);
        if (before == null) {
            out.put("error", "广告不存在：" + id);
            return out;
        }
        if (dailyBudget.signum() < 0) {
            out.put("error", "预算不能为负");
            return out;
        }

        adMapper.updateBudget(id, dailyBudget);
        Ad after = adMapper.findById(id);
        invertedIndex.replaceAd(after);

        out.put("adId", id);
        out.put("budgetBefore", before.getDailyBudget());
        out.put("budgetAfter", after.getDailyBudget());
        out.put("serviceRestarted", false);
        out.put("cacheHit", invertedIndex.cachedAd(id) != null);
        out.put("appliedIn", "MySQL + 内存倒排索引 adCache");

        // 预算改小之后，Redis 里可能还留着比新预算大的余额。
        // 这里主动把它压到不超过新预算，否则「改小预算立刻生效」是假的——
        // 脚本只在 key 不存在时才按 daily_budget 初始化，
        // 余额还在就一直用旧余额扣，广告永远不会「耗尽」。
        //
        // ⚠️ 【踩过的坑：守卫条件写错，报告与实际不一致】
        // 第一版写的是「预算变了才 clamp」：
        //     if (before.getDailyBudget().compareTo(dailyBudget) != 0) { clamp(); }
        // 看起来合理，实际会漏。演示时先 reset（预算 500）再改成 20，
        // 两次调用之间预算确实变了，能 clamp；
        // 但如果连续两次都传 20（幂等重试、或页面上重复点保存），
        // 第二次 before==after==20，clamp 被跳过——
        // 而 Redis 里的余额可能还停留在更小的旧值上，
        // 于是接口返回「余额已压到 20」而实际没压，广告直接卡死不再参竞。
        //
        // 正确的判据是「当前余额是否超过新预算」，与预算是否变化无关。
        // clampBalance 内部已经是幂等的（没超就不动），
        // 所以这里无条件调用即可，不需要外层守卫。
        Map<String, Object> clamp = clampBalance(id, after);
        out.put("balance", clamp.get("balance"));
        out.put("balanceClamped", clamp.get("clamped"));

        log.warn("广告 {} 日预算 {} -> {}（已热生效，未重启）",
                id, before.getDailyBudget(), dailyBudget);
        return out;
    }

    /** 改出价，改完立刻影响二价结算。 */
    @PostMapping("/ads/{id}/bid")
    public Map<String, Object> updateBid(@PathVariable Long id,
                                         @RequestParam BigDecimal bid) {
        Ad before = adMapper.findById(id);
        if (before == null) {
            return Map.of("error", "广告不存在：" + id);
        }
        if (bid.signum() <= 0) {
            return Map.of("error", "出价必须大于 0");
        }
        adMapper.updateBid(id, bid);
        Ad after = adMapper.findById(id);
        invertedIndex.replaceAd(after);
        log.warn("广告 {} 出价 {} -> {}（二价结算即时生效）", id, before.getBid(), bid);
        return Map.of(
                "adId", id,
                "bidBefore", before.getBid(),
                "bidAfter", after.getBid(),
                "serviceRestarted", false,
                "note", "出价参与二价结算的次高价计算，改完下一次竞价就按新价算扣费");
    }

    /**
     * 改在投状态：暂停 / 恢复。
     *
     * <p>恢复时把 status 从 2/3 拉回 1，同时
     * {@link InvertedIndex#replaceAd} 会把广告重新放回 {@code adCache}——
     * 因为暂停时缓存里已经移除了它，不放回去就是「界面显示在投但不参竞」。
     */
    @PostMapping("/ads/{id}/status")
    public Map<String, Object> updateStatus(@PathVariable Long id,
                                            @RequestParam Integer status) {
        if (status == null || status < 1 || status > 3) {
            return Map.of("error", "status 只能是 1在投 / 2暂停 / 3已耗尽");
        }
        Ad before = adMapper.findById(id);
        if (before == null) {
            return Map.of("error", "广告不存在：" + id);
        }
        adMapper.updateStatus(id, status);
        Ad after = adMapper.findById(id);
        invertedIndex.replaceAd(after);
        log.warn("广告 {} 状态 {} -> {}（已热生效）", id, before.getStatus(), status);
        return Map.of(
                "adId", id,
                "statusBefore", before.getStatus(),
                "statusAfter", status,
                "cached", invertedIndex.cachedAd(id) != null,
                "serviceRestarted", false);
    }

    /**
     * 覆盖式保存定向条件。
     *
     * <p><b>为什么是「覆盖」而不是「增删」</b>：
     * 界面上是一整张定向表单，提交的是这份表单的最终状态。
     * 如果做成增删，就得额外传「哪些是新增的、哪些是删掉的」，
     * 而这个信息前端算出来就不可信了。覆盖式只需一个最终态，实现也简单。
     *
     * <p><b>两步的顺序：先库后内存</b>。反过来（先改内存后写库）的话，
     * 库写失败时内存已经改了，界面上却还是旧值——
     * 这种不一致比「改了库没生效」更难排查。
     * 现在这个顺序最坏情况是内存没同步，刷新一次管理端就恢复。
     */
    @PostMapping("/ads/{id}/targeting")
    public Map<String, Object> saveTargeting(@PathVariable Long id,
                                             @RequestBody List<Map<String, Object>> body) {
        Ad ad = adMapper.findById(id);
        if (ad == null) {
            return Map.of("error", "广告不存在：" + id);
        }

        // 1. 解析并校验入参
        List<InvertedIndex.TargetRow> want = new ArrayList<>();
        Set<String> wantKeys = new java.util.LinkedHashSet<>();
        Set<Integer> validDims = Set.of(1, 2, 3, 4, 5, 6);
        for (Map<String, Object> item : body) {
            int dimType = (int) toLong(item.get("dimType"));
            String dimValue = String.valueOf(item.get("dimValue"));
            if (!validDims.contains(dimType)) {
                return Map.of("error", "非法 dimType：" + dimType,
                        "allowed", validDims.stream().map(String::valueOf).toList());
            }
            if (dimValue == null || dimValue.isBlank() || "null".equals(dimValue)) {
                return Map.of("error", "dimValue 不能为空（dimType=" + dimType + "）");
            }
            if (dimValue.length() > 64) {
                // ad_target.dim_value 是 VARCHAR(64)，超长在批量提交时才暴露
                return Map.of("error", "dimValue 超长（" + dimValue.length()
                        + " > 64），dimType=" + dimType);
            }
            // 顺手去重：表单里重复勾了同一个值时，
            // 若不去重会插两条重复行——而 ad_target 没有唯一约束挡着，
            // 重复行会让倒排求交把同一条广告算两次。
            wantKeys.add(dimType + " " + dimValue);
        }
        for (String k : wantKeys) {
            int sp = k.indexOf(' ');
            want.add(new InvertedIndex.TargetRow(null,
                    Integer.parseInt(k.substring(0, sp)), k.substring(sp + 1), id));
        }

        // 2. 写库：先读现状，只删真正要删的、只插真正要加的
        //
        // 【为什么不「全删再全插」】
        // 第一版就是这么写的，结果 DELETE FROM ad_target WHERE ad_id=?
        // 在 596 万行上全表扫，直接撞 5 秒超时。
        // 改成逐条按 (ad_id, dim_type, dim_value) 删之后走 idx_dim_lookup，
        // rows=1，零新增索引就解决了。
        //
        // 【为什么不直接用 deleteTargetingOf 全删】
        // 那条语句还在，但它扫全表、是兜底路径。
        // 正常保存走差集，代价与「改动条数」成正比而不是与表大小成正比。
        List<InvertedIndex.TargetRow> current = adMapper.listTargetingOf(id);
        Set<String> currentKeys = new java.util.HashSet<>();
        for (InvertedIndex.TargetRow r : current) {
            currentKeys.add(r.dimType() + " " + r.dimValue());
        }

        int deleted = 0;
        for (String k : currentKeys) {
            if (!wantKeys.contains(k)) {
                int sp = k.indexOf(' ');
                adMapper.deleteTargetingOne(id,
                        Integer.parseInt(k.substring(0, sp)), k.substring(sp + 1));
                deleted++;
            }
        }
        int inserted = 0;
        for (String k : wantKeys) {
            if (!currentKeys.contains(k)) {
                int sp = k.indexOf(' ');
                adMapper.insertTargeting(id,
                        Integer.parseInt(k.substring(0, sp)), k.substring(sp + 1));
                inserted++;
            }
        }

        // 3. 同步内存索引，算出实际改了几条
        Map<String, Object> diff = invertedIndex.syncTargeting(id, want);

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("adId", id);
        out.put("saved", want.size());
        out.put("dbDeleted", deleted);
        out.put("dbInserted", inserted);
        out.put("diff", diff);
        out.put("serviceRestarted", false);
        out.put("appliedIn", "MySQL ad_target + 内存倒排索引");
        out.put("note", "effective=" + diff.get("effective")
                + " 表示实际变更多少条定向；等于 0 说明提交的内容与库中原有的一致");

        // 定向改动影响命中范围，直接影响该广告下次是否参竞——
        // 演示时这是「改定向即时生效」最直观的证据
        out.put("candidateHint", "该广告当前各定向维度命中数："
                + dimHitSummary(id, want));
        log.warn("广告 {} 定向已覆盖保存：{} 项，实际变更 {}（已热生效，未重启）",
                id, want.size(), diff.get("effective"));
        return out;
    }

    // =================================================================
    // 内部工具
    // =================================================================

    /**
     * 把 Redis 里的余额压到不超过日预算。
     *
     * <p><b>为什么改小预算时必须做这一步</b>：
     * {@code deduct_budget.lua} 只在 key 不存在时用 daily_budget 初始化。
     * 预算从 500 改成 20 而余额还剩 480 时，
     * 后续扣减会一直从 480 里扣，广告<b>永远耗不尽</b>，
     * 演示第 5 步「连刷到 A 消失」就永远等不到。
     *
     * <p><b>只压小不抬高</b>：预算从 20 调回 500 时，
     * 余额不会自动涨到 500。这是对的——广告主加预算不等于送钱，
     * 余额只能靠消耗与充值变多。
     *
     * <p><b>返回 Map 而不是 BigDecimal</b>：
     * 调用方要同时知道「余额是多少」和「有没有真的压过」，
     * 只回一个数值的话两者混在一起，
     * 就会出现本轮实测到的问题——接口报「余额 20 元」而 Redis 里还是旧值。
     * 把 {@code clamped} 单独返回，才能在演示前一眼看出这一步有没有生效。
     *
     * <p><b>幂等</b>：余额没超就原样返回，重复调用安全。
     */
    private Map<String, Object> clampBalance(Long adId, Ad ad) {
        String key = "budget:" + adId + ":" + LocalDate.now().format(DATE_FMT);
        Map<String, Object> out = new LinkedHashMap<>();
        try {
            String cur = redis.opsForValue().get(key);
            long cap = ad.getDailyBudget().movePointRight(2).longValue();
            if (cur == null) {
                // key 不存在：下次扣费时 Lua 会用 daily_budget 初始化，无需干预
                out.put("balance", ad.getDailyBudget());
                out.put("clamped", false);
                out.put("reason", "余额 key 不存在，扣费时会按日预算自动初始化");
                return out;
            }
            long cents = Long.parseLong(cur);
            if (cents > cap) {
                redis.opsForValue().set(key, String.valueOf(cap));
                out.put("balance", ad.getDailyBudget());
                out.put("clamped", true);
                out.put("reason", "余额 " + new BigDecimal(cents).movePointLeft(2)
                        + " 元超过新日预算，已压到 " + ad.getDailyBudget() + " 元");
            } else {
                out.put("balance", new BigDecimal(cents).movePointLeft(2));
                out.put("clamped", false);
                out.put("reason", "余额未超过日预算，无需调整");
            }
            return out;
        } catch (Exception e) {
            log.warn("调整余额上限失败 adId={}：{}", adId, e.getMessage());
            out.put("balance", null);
            out.put("clamped", false);
            out.put("error", e.getMessage());
            return out;
        }
    }

    private BigDecimal safeBalance(Ad ad) {
        try {
            return budgetService.getBalance(ad.getId(), ad.getDailyBudget());
        } catch (Exception e) {
            return null;
        }
    }

    private String dimHitSummary(Long adId, List<InvertedIndex.TargetRow> want) {
        StringBuilder sb = new StringBuilder();
        for (InvertedIndex.TargetRow r : want) {
            if (sb.length() > 0) {
                sb.append("，");
            }
            sb.append("dim").append(r.dimType()).append("=").append(r.dimValue())
                    .append(" 命中 ").append(invertedIndex.hitCount(r.dimType(), r.dimValue()))
                    .append(" 条");
        }
        return sb.toString();
    }

    private static long toLong(Object v) {
        if (v instanceof Number n) {
            return n.longValue();
        }
        return Long.parseLong(String.valueOf(v));
    }
}
