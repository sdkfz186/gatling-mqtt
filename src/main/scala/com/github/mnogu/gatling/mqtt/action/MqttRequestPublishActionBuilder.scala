package com.github.mnogu.gatling.mqtt.action

import com.github.mnogu.gatling.mqtt.protocol.{MqttComponents, MqttProtocol}
import com.github.mnogu.gatling.mqtt.request.builder.MqttAttributes
import io.gatling.core.action.Action
import io.gatling.core.action.builder.ActionBuilder
import io.gatling.core.structure.ScenarioContext
import io.gatling.commons.util.DefaultClock
class MqttRequestPublishActionBuilder(mqttAttributes: MqttAttributes)
  extends ActionBuilder {

  override def build(
    ctx: ScenarioContext, next: Action
  ): Action = {
    import ctx._

    val mqttComponents : MqttComponents = protocolComponentsRegistry.components(MqttProtocol.MqttProtocolKey)
    
    new MqttRequestPublishAction(
      mqttAttributes,
      coreComponents,
      mqttComponents.mqttProtocol,
      new DefaultClock(),
      next
    )
  }
}
