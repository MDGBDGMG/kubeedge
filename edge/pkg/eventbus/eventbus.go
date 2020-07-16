package eventbus

import (
	"encoding/json"
	"fmt"
	"os"

	"k8s.io/klog"

	"github.com/kubeedge/beehive/pkg/core"
	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/kubeedge/edge/pkg/common/modules"
	"github.com/kubeedge/kubeedge/edge/pkg/eventbus/common/util"
	eventconfig "github.com/kubeedge/kubeedge/edge/pkg/eventbus/config"
	mqttBus "github.com/kubeedge/kubeedge/edge/pkg/eventbus/mqtt"
	"github.com/kubeedge/kubeedge/pkg/apis/componentconfig/edgecore/v1alpha1"
)

var mqttServer *mqttBus.Server

// eventbus struct
type eventbus struct {
	enable bool
}

func newEventbus(enable bool) *eventbus {
	return &eventbus{
		enable: enable,
	}
}

// Register register eventbus
func Register(eventbus *v1alpha1.EventBus, nodeName string) {
	eventconfig.InitConfigure(eventbus, nodeName)
	core.Register(newEventbus(eventbus.Enable))
}

func (*eventbus) Name() string {
	return "eventbus"
}

func (*eventbus) Group() string {
	return modules.BusGroup
}

// Enable indicates whether this module is enabled
func (eb *eventbus) Enable() bool {
	return eb.enable
}

//按照配置文件创建broker和client，然后开启无限循环，将cloud端的信息发布给edge
func (eb *eventbus) Start() {
	//若mqttmode规则为 1或2 ，说明仅外部mqtt的broker可用。
	if eventconfig.Config.MqttMode >= v1alpha1.MqttModeBoth {
		hub := &mqttBus.Client{
			MQTTUrl: eventconfig.Config.MqttServerExternal,
		} //将config中的外部mqtt的url赋值给clint的MQTTUrl
		mqttBus.MQTTHub = hub //将上述client赋值给MQTTHub
		hub.InitSubClient()   //初始化SubClient
		hub.InitPubClient()   //初始化PubClient
		klog.Infof("Init Sub And Pub Client for externel mqtt broker %v successfully", eventconfig.Config.MqttServerExternal)
	}
	//若mqttmode规则为 0或1，说明仅内部mqtt的broker可用
	if eventconfig.Config.MqttMode <= v1alpha1.MqttModeBoth {
		mqttServer = mqttBus.NewMqttServer( //依据eventconfig的配置信息创建一个内部MqttServer
			int(eventconfig.Config.MqttSessionQueueSize),
			eventconfig.Config.MqttServerInternal,
			eventconfig.Config.MqttRetain,
			int(eventconfig.Config.MqttQOS))
		mqttServer.InitInternalTopics() //设置topic为内部默认的topic
		err := mqttServer.Run()         //配置mqttServer，并生产一个engine运行server
		if err != nil {
			klog.Errorf("Launch internel mqtt broker failed, %s", err.Error())
			os.Exit(1)
		}
		klog.Infof("Launch internel mqtt broker %v successfully", eventconfig.Config.MqttServerInternal)
	}
	//无限循环，先用beehiveContext按照eb.name获取msg
	//然后按照msg.operation（subscribe、message、publish、get_result）处理信息
	eb.pubCloudMsgToEdge()
}

//qos为1，即得到ack之前会一直发送。
func pubMQTT(topic string, payload []byte) {
	token := mqttBus.MQTTHub.PubCli.Publish(topic, 1, false, payload)
	if token.WaitTimeout(util.TokenWaitTime) && token.Error() != nil {
		klog.Errorf("Error in pubMQTT with topic: %s, %v", topic, token.Error())
	} else {
		klog.Infof("Success in pubMQTT with topic: %s", topic)
	}
}

//针对不同的消息类型进行不同的操作
func (eb *eventbus) pubCloudMsgToEdge() {
	for {
		select {
		case <-beehiveContext.Done(): //检查beehiveContext是否正常
			klog.Warning("EventBus PubCloudMsg To Edge stop")
			return
		default:
		}
		accessInfo, err := beehiveContext.Receive(eb.Name()) //使用beehiveContext获取该模块的msg
		if err != nil {
			klog.Errorf("Fail to get a message from channel: %v", err)
			continue
		}
		operation := accessInfo.GetOperation() //解析msg
		resource := accessInfo.GetResource()
		switch operation {
		case "subscribe":
			eb.subscribe(resource) //若操作为订阅，则订阅该resource
			klog.Infof("Edge-hub-cli subscribe topic to %s", resource)
		case "message": //若操作为message，则将消息转化为json发布到topic中
			body, ok := accessInfo.GetContent().(map[string]interface{})
			if !ok {
				klog.Errorf("Message is not map type")
				return
			}
			message := body["message"].(map[string]interface{})
			topic := message["topic"].(string)
			payload, _ := json.Marshal(&message) //使用Marshal将msg转化为json格式
			eb.publish(topic, payload)
		case "publish": //若操作为publish，则将消息转为字节码发布到topic中
			topic := resource
			var ok bool
			// cloud and edge will send different type of content, need to check
			payload, ok := accessInfo.GetContent().([]byte)
			if !ok {
				content := accessInfo.GetContent().(string)
				payload = []byte(content)
			}
			eb.publish(topic, payload)
		case "get_result": //若操作为get_result,则将消息转化为json发布到topic中
			if resource != "auth_info" {
				klog.Info("Skip none auth_info get_result message")
				return
			}
			topic := fmt.Sprintf("$hw/events/node/%s/authInfo/get/result", eventconfig.Config.NodeName)
			payload, _ := json.Marshal(accessInfo.GetContent())
			eb.publish(topic, payload)
		default:
			klog.Warningf("Action not found")
		}
	}
}

//分情况调用Mqtt的API订阅消息/发布topic
func (eb *eventbus) publish(topic string, payload []byte) {
	if eventconfig.Config.MqttMode >= v1alpha1.MqttModeBoth {
		// pub msg to external mqtt broker.
		pubMQTT(topic, payload) //针对外部broker需要用Eclipse包的Mqtt的API
	}

	if eventconfig.Config.MqttMode <= v1alpha1.MqttModeBoth {
		// pub msg to internal mqtt broker.
		mqttServer.Publish(topic, payload) //内部broker，调用256dpi包的gomqtt的API发布消息
	}
}

//分情况调用Eclipse包的Mqtt的API订阅消息/调用256dpi包的gomqtt的API绑定topic
func (eb *eventbus) subscribe(topic string) {
	if eventconfig.Config.MqttMode >= v1alpha1.MqttModeBoth {
		// subscribe topic to external mqtt broker.
		token := mqttBus.MQTTHub.SubCli.Subscribe(topic, 1, mqttBus.OnSubMessageReceived)
		if rs, err := util.CheckClientToken(token); !rs { //检验订阅是否成功
			klog.Errorf("Edge-hub-cli subscribe topic: %s, %v", topic, err)
		}
	}

	if eventconfig.Config.MqttMode <= v1alpha1.MqttModeBoth {
		// set topic to internal mqtt broker.
		mqttServer.SetTopic(topic) //将topic绑定在内部broker中
	}
}
