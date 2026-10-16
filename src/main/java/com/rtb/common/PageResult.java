package com.rtb.common;

import lombok.Data;

import java.util.List;

/**
 * 分页结果外壳，配合 {@link ApiResponse#ok(Object)} 返回。
 *
 * <p>采用「一次查总数 + 一次查当页」的实现（{@code COUNT(*)} + {@code LIMIT/OFFSET}），
 * 而不是游标分页：管理后台是低频查询、需要跳页，总量必须可知。
 * 海量明细表（bid_log）的查询若要做深分页，应改用游标（keyset）方式，
 * 但那是另一类接口（结果表查询），不在此处。
 */
@Data
public class PageResult<T> {

    /** 满足条件的总行数 */
    private long total;

    /** 当前页码，从 1 开始 */
    private int pageNum;

    /** 每页条数 */
    private int pageSize;

    /** 总页数 */
    private int pages;

    /** 当页数据 */
    private List<T> list;

    public static <T> PageResult<T> of(long total, int pageNum, int pageSize, List<T> list) {
        PageResult<T> r = new PageResult<>();
        r.total = total;
        r.pageNum = pageNum;
        r.pageSize = pageSize;
        r.pages = pageSize <= 0 ? 0 : (int) ((total + pageSize - 1) / pageSize);
        r.list = list;
        return r;
    }
}
