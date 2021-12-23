package com.bjke.flink.model

//商品类(商品id,商品名称,商品价格)
//订单明细类(订单id,商品id,商品数量)
//关联结果(商品id,商品名称,商品数量,商品价格*商品数量)
case class FactOrderItem(goodsId: String, goodsName: String, count: BigDecimal, totalMoney: BigDecimal) {}
