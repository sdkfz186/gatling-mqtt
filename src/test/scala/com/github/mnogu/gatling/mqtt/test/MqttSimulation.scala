package com.github.mnogu.gatling.mqtt.test

import com.github.mnogu.gatling.mqtt.Predef._
import io.gatling.core.Predef._
import org.fusesource.mqtt.client.QoS

import scala.concurrent.duration._

class MqttSimulation extends Simulation {
  val mqttConf = mqtt.host("tcp://192.168.132.137:11883")

  val connect = exec(mqtt("connect")
    .connect())

  val publish = repeat(100) {
    exec(mqtt("publish")
      .publish("mqtt_upload_message", "Hello", QoS.AT_LEAST_ONCE, retain = false))
      .pause(1000 milliseconds)
  }

  val disconnect = exec(mqtt("disconnect")
    .disconnect())

  val scn = scenario("MQTT Test")
    .exec(connect, publish, disconnect)

  setUp(
    scn
      .inject(rampUsers(10) during (1 seconds))
  ).protocols(mqttConf)
}
