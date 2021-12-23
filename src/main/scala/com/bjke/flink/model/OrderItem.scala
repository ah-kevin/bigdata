package com.bjke.flink.model

import com.google.gson.Gson

case class OrderItem(itemId: String, var goodsId: String, count: Int) {
}
