package com.atguigu.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ItemCount1 {

    private Long itemId;//商品id
    private Long windowEnd;//窗口结束时间
    private Long count;//窗口累计
}
