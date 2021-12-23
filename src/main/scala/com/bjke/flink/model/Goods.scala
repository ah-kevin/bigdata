package com.bjke.flink.model

import com.google.gson.Gson
import scala.util.Random

//商品类(商品id,商品名称,商品价格)
case object Goods {
  var r: Random = _
  var GOODS_LIST: Array[Goods] = _
  r = new Random()
  GOODS_LIST = new Array[Goods](6)
  GOODS_LIST(0) = Goods("1", "小米12", BigDecimal(4890))
  GOODS_LIST(1) = Goods("2", "iphone12", BigDecimal(12000))
  GOODS_LIST(2) = Goods("3", "MacBookPro", BigDecimal(15000))
  GOODS_LIST(3) = Goods("4", "ThinkPad X1", BigDecimal(9800))
  GOODS_LIST(4) = Goods("5", "MeiZu One", BigDecimal(3200))
  GOODS_LIST(5) = Goods("6", "Mate 40", BigDecimal(6500))

  def randomGoods(): Goods = {
    val rIndex: Int = r.nextInt(GOODS_LIST.length);
    GOODS_LIST(rIndex)
  }
}

case class Goods(goodsId: String, goodsName: String, goodsPrice: BigDecimal) {
}


