package com.atguigu.bean;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ItemCount {
    private Long itemId;//商品id
    private Long windowEnd;
    private Long count;
}
