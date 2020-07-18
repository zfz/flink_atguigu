package com.atguigu.ch03_transform

import com.atguigu.SensorEntity
import org.apache.flink.api.common.functions.FilterFunction

class HighFilter(temperature: Double) extends FilterFunction[SensorEntity] {
  override def filter(t: SensorEntity): Boolean = {
    t.temperature > temperature
  }
}
