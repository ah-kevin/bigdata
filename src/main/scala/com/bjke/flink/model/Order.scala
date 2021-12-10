package com.bjke.flink.model

case class Order(orderId: String, userId: Int, money: Int, eventTime: Long) {}
