package dtmanager

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"k8s.io/klog"

	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/beehive/pkg/core/model"
	connect "github.com/kubeedge/kubeedge/edge/pkg/common/cloudconnection"
	"github.com/kubeedge/kubeedge/edge/pkg/devicetwin/dtcommon"
	"github.com/kubeedge/kubeedge/edge/pkg/devicetwin/dtcontext"
	"github.com/kubeedge/kubeedge/edge/pkg/devicetwin/dttype"
)

var (
	//ActionCallBack map for action to callback
	ActionCallBack map[string]CallBack
)

//CommWorker deal app response event
type CommWorker struct {
	Worker
	Group string
}

//Start worker
//启动communicate类型的worker
func (cw CommWorker) Start() {
	initActionCallBack() //将msg转化格式后存入confirmMap，通知websocket；或者从confirmMap中删掉此msg.parentID
	for {
		select {
		case msg, ok := <-cw.ReceiverChan:
			klog.Info("receive msg commModule")
			if !ok {
				return
			}
			if dtMsg, isDTMessage := msg.(*dttype.DTMessage); isDTMessage {
				if fn, exist := ActionCallBack[dtMsg.Action]; exist {
					_, err := fn(cw.DTContexts, dtMsg.Identity, dtMsg.Msg)
					if err != nil {
						klog.Errorf("CommModule deal %s event failed: %v", dtMsg.Action, err)
					}
				} else {
					klog.Errorf("CommModule deal %s event failed, not found callback", dtMsg.Action)
				}
			}

		case <-time.After(time.Duration(60) * time.Second):
			cw.checkConfirm(cw.DTContexts, nil)
		case v, ok := <-cw.HeartBeatChan:
			if !ok {
				return
			}
			if err := cw.DTContexts.HeartBeat(cw.Group, v); err != nil {
				return
			}
		}
	}
}

func initActionCallBack() {
	ActionCallBack = make(map[string]CallBack)
	//将message转化格式，发送到websocket的channel中，存入confirmMap中
	ActionCallBack[dtcommon.SendToCloud] = dealSendToCloud
	//将msg发送到eventbus的channel中
	ActionCallBack[dtcommon.SendToEdge] = dealSendToEdge
	//初始化一个固定的detail结构体，包装为message，存入confirmMap，发送到websocket
	ActionCallBack[dtcommon.LifeCycle] = dealLifeCycle
	//将msg转为message格式，从confirmMap里删掉message.parentID
	ActionCallBack[dtcommon.Confirm] = dealConfirm
}

func dealSendToEdge(context *dtcontext.DTContext, resource string, msg interface{}) (interface{}, error) {
	beehiveContext.Send(dtcommon.EventHubModule, *msg.(*model.Message))
	return nil, nil
}
func dealSendToCloud(context *dtcontext.DTContext, resource string, msg interface{}) (interface{}, error) {
	if strings.Compare(context.State, dtcommon.Disconnected) == 0 {
		klog.Infof("Disconnected with cloud,not send msg to cloud")
		return nil, nil
	} //检查context的状态是否断开
	message, ok := msg.(*model.Message) //将msg转换为message格式
	if !ok {
		return nil, errors.New("msg not Message type")
	}
	beehiveContext.Send(dtcommon.HubModule, *message) //将message发送道websocket的channel
	msgID := message.GetID()                          //将msg存到confirmMap中
	context.ConfirmMap.Store(msgID, &dttype.DTMessage{Msg: message, Action: dtcommon.SendToCloud, Type: dtcommon.CommModule})
	return nil, nil
}
func dealLifeCycle(context *dtcontext.DTContext, resource string, msg interface{}) (interface{}, error) {
	klog.Infof("CONNECTED EVENT")
	message, ok := msg.(*model.Message)
	if !ok {
		return nil, errors.New("msg not Message type")
	}
	connectedInfo, _ := (message.Content.(string))
	if strings.Compare(connectedInfo, connect.CloudConnected) == 0 { //比较连接信息是否一致
		if strings.Compare(context.State, dtcommon.Disconnected) == 0 { //比较状态是否断开
			_, err := detailRequest(context, msg)
			if err != nil {
				klog.Errorf("detail request: %v", err)
				return nil, err
			}
		}
		context.State = dtcommon.Connected
	} else if strings.Compare(connectedInfo, connect.CloudDisconnected) == 0 {
		context.State = dtcommon.Disconnected
	}
	return nil, nil
}
func dealConfirm(context *dtcontext.DTContext, resource string, msg interface{}) (interface{}, error) {
	klog.Infof("CONFIRM EVENT")
	value, ok := msg.(*model.Message)

	if ok {
		parentMsgID := value.GetParentID()
		klog.Infof("CommModule deal confirm msgID %s", parentMsgID)
		context.ConfirmMap.Delete(parentMsgID)
	} else {
		return nil, errors.New("CommModule deal confirm, type not correct")
	}
	return nil, nil
}

func detailRequest(context *dtcontext.DTContext, msg interface{}) (interface{}, error) {
	//todo eventid uuid
	getDetail := dttype.GetDetailNode{ //初始化一个Detail结构体
		EventType: "group_membership_event",
		EventID:   "123",
		Operation: "detail",
		GroupID:   context.NodeName,
		TimeStamp: time.Now().UnixNano() / 1000000}
	getDetailJSON, marshalErr := json.Marshal(getDetail) //将结构体转化为JSON
	if marshalErr != nil {
		klog.Errorf("Marshal request error while request detail, err: %#v", marshalErr)
		return nil, marshalErr
	}
	//包装为一个message
	message := context.BuildModelMessage("resource", "", "membership/detail", "get", string(getDetailJSON))
	klog.Info("Request detail")
	msgID := message.GetID() //将message存入到confirmMap中
	context.ConfirmMap.Store(msgID, &dttype.DTMessage{Msg: message, Action: dtcommon.SendToCloud, Type: dtcommon.CommModule})
	beehiveContext.Send(dtcommon.HubModule, *message) //将msg发送到websocket的channel中
	return nil, nil
}

func (cw CommWorker) checkConfirm(context *dtcontext.DTContext, msg interface{}) (interface{}, error) {
	klog.Info("CheckConfirm")
	context.ConfirmMap.Range(func(key interface{}, value interface{}) bool {
		dtmsg, ok := value.(*dttype.DTMessage)
		klog.Info("has msg")
		if !ok {

		} else {
			klog.Info("redo task due to no recv")
			if fn, exist := ActionCallBack[dtmsg.Action]; exist {
				_, err := fn(cw.DTContexts, dtmsg.Identity, dtmsg.Msg)
				if err != nil {
					klog.Errorf("CommModule deal %s event failed: %v", dtmsg.Action, err)
				}
			} else {
				klog.Errorf("CommModule deal %s event failed, not found callback", dtmsg.Action)
			}

		}
		return true
	})
	return nil, nil
}
