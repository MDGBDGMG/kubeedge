package dtmanager

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"k8s.io/klog"

	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/edge/pkg/common/modules"
	"github.com/kubeedge/kubeedge/edge/pkg/devicetwin/dtclient"
	"github.com/kubeedge/kubeedge/edge/pkg/devicetwin/dtcommon"
	"github.com/kubeedge/kubeedge/edge/pkg/devicetwin/dtcontext"
	"github.com/kubeedge/kubeedge/edge/pkg/devicetwin/dttype"
)

var (
	//deviceActionCallBack map for action to callback
	deviceActionCallBack map[string]CallBack
)

//DeviceWorker deal device event
type DeviceWorker struct {
	Worker
	Group string
}

//Start worker
func (dw DeviceWorker) Start() {
	initDeviceActionCallBack()
	for {
		select {
		case msg, ok := <-dw.ReceiverChan: //处理DeviceModule的Event
			if !ok {
				return
			}
			if dtMsg, isDTMessage := msg.(*dttype.DTMessage); isDTMessage {
				//按照action，执行Device的CallBack函数（updatedevice或者updatestatu）
				if fn, exist := deviceActionCallBack[dtMsg.Action]; exist {
					_, err := fn(dw.DTContexts, dtMsg.Identity, dtMsg.Msg)
					if err != nil {
						klog.Errorf("DeviceModule deal %s event failed: %v", dtMsg.Action, err)
					}
				} else {
					klog.Errorf("DeviceModule deal %s event failed, not found callback", dtMsg.Action)
				}
			}
		case v, ok := <-dw.HeartBeatChan: //处理心跳
			if !ok {
				return
			}
			if err := dw.DTContexts.HeartBeat(dw.Group, v); err != nil {
				return
			}
		}
	}
}

//DeviceAction CallBack
func initDeviceActionCallBack() {
	//创建CallBack的map
	deviceActionCallBack = make(map[string]CallBack)
	//定义callback行为-DeviceUpdate；处理update数据，同步sqlite和context；失败则通过communityWorker发到edge的busgroup
	deviceActionCallBack[dtcommon.DeviceUpdated] = dealDeviceUpdated
	//定义callback行为-DeviceStateUpdate；处理state数据，写入DB，并经过commWorker，发的到edge的busgroup和resource
	deviceActionCallBack[dtcommon.DeviceStateUpdate] = dealDeviceStateUpdate
}

func dealDeviceStateUpdate(context *dtcontext.DTContext, resource string, msg interface{}) (interface{}, error) {
	message, ok := msg.(*model.Message)
	if !ok {
		return nil, errors.New("msg not Message type")
	}

	updateDevice, err := dttype.UnmarshalDeviceUpdate(message.Content.([]byte))
	if err != nil {
		klog.Errorf("Unmarshal device info failed, err: %#v", err)
		return nil, err
	}
	deviceID := resource
	defer context.Unlock(deviceID)
	context.Lock(deviceID)
	doc, docExist := context.DeviceList.Load(deviceID)
	if !docExist {
		return nil, nil
	}
	device, ok := doc.(*dttype.Device)
	if !ok {
		return nil, nil
	}
	//比较，若updateDevice.State不是online和offline和unknown,返回nil
	if strings.Compare("online", updateDevice.State) != 0 && strings.Compare("offline", updateDevice.State) != 0 && strings.Compare("unknown", updateDevice.State) != 0 {
		return nil, nil
	}
	lastOnline := time.Now().Format("2006-01-02 15:04:05")
	for i := 1; i <= dtcommon.RetryTimes; i++ {
		//更新state字段，更新为updateDevice.State
		err = dtclient.UpdateDeviceField(device.ID, "state", updateDevice.State)
		//更新last_online,更新为lastOnline（时间）
		err = dtclient.UpdateDeviceField(device.ID, "last_online", lastOnline)
		if err == nil {
			break
		}
		//若出现异常，那么等待重试间隔时间，再次重试
		time.Sleep(dtcommon.RetryInterval)
	}
	if err != nil {

	}
	device.State = updateDevice.State
	device.LastOnline = lastOnline
	payload, err := dttype.BuildDeviceState(dttype.BuildBaseMessage(), *device)
	if err != nil {

	}
	topic := dtcommon.DeviceETPrefix + device.ID + dtcommon.DeviceETStateUpdateSuffix + "/result"
	//将state和lastOnline写入device，然后发布到communityWorker，传给edge。最终然后给busgroup，执行publish操作
	context.Send(device.ID,
		dtcommon.SendToEdge,
		dtcommon.CommModule,
		context.BuildModelMessage(modules.BusGroup, "", topic, "publish", payload))

	msgResource := "device/" + device.ID + "/state"
	//将state和lastOnline写入device，然后发布到communityWorker，传给edge。最终然后给resource，执行update操作
	context.Send(deviceID,
		dtcommon.SendToCloud,
		dtcommon.CommModule,
		context.BuildModelMessage("resource", "", msgResource, "update", string(payload)))
	return nil, nil
}

func dealDeviceUpdated(context *dtcontext.DTContext, resource string, msg interface{}) (interface{}, error) {
	message, ok := msg.(*model.Message) //转化msg格式
	if !ok {
		return nil, errors.New("msg not Message type")
	}

	//将字节数组转化为DeviceUpdate的数据
	updateDevice, err := dttype.UnmarshalDeviceUpdate(message.Content.([]byte))
	if err != nil {
		klog.Errorf("Unmarshal device info failed, err: %#v", err)
		return nil, err
	}

	deviceID := resource

	context.Lock(deviceID)
	//更新device在DB中的属性。写入context中；若写入失败，则发送到communityWorker中，转发到Edge的BusGroup
	DeviceUpdated(context, deviceID, updateDevice.Attributes, dttype.BaseMessage{EventID: updateDevice.EventID}, 0)
	context.Unlock(deviceID)
	return nil, nil
}

//DeviceUpdated update device attributes
//更新device在DB中的属性。写入context中；若写入失败，则发送到communityWorker中，转发到Edge
func DeviceUpdated(context *dtcontext.DTContext, deviceID string, attributes map[string]*dttype.MsgAttr, baseMessage dttype.BaseMessage, dealType int) (interface{}, error) {
	klog.Infof("Begin to update attributes of the device %s", deviceID)
	var err error
	//从map中取出device
	doc, docExist := context.DeviceList.Load(deviceID)
	if !docExist {
		return nil, nil
	}
	Device, ok := doc.(*dttype.Device) //转化格式
	if !ok {
		return nil, nil
	}
	//比较context和attributes中device的差别
	dealAttrResult := DealMsgAttr(context, Device.ID, attributes, dealType)
	add, delete, update, result := dealAttrResult.Add, dealAttrResult.Delete, dealAttrResult.Update, dealAttrResult.Result
	//若有add和delete或update操作
	if len(add) != 0 || len(delete) != 0 || len(update) != 0 {
		for i := 1; i <= dtcommon.RetryTimes; i++ {
			//开启DB事务，对DB进行增删和更新操作
			err = dtclient.DeviceAttrTrans(add, delete, update)
			if err == nil {
				break
			}
			time.Sleep(dtcommon.RetryInterval)
		}
		now := time.Now().UnixNano() / 1e6
		baseMessage.Timestamp = now
		//若上述对DB操作无误
		if err != nil {
			//从sqlite中同步device信息到Context.DeviceList中
			SyncDeviceFromSqlite(context, deviceID)
			klog.Errorf("Update device failed due to writing sql error: %v", err)

		} else {
			//若对DB的操作有误
			klog.Infof("Send update attributes of device %s event to edge app", deviceID)
			//构建payload
			payload, err := dttype.BuildDeviceAttrUpdate(baseMessage, result)
			if err != nil {
				//todo
				klog.Errorf("Build device attribute update failed: %v", err)
			}
			//构建topic
			topic := dtcommon.DeviceETPrefix + deviceID + dtcommon.DeviceETUpdatedSuffix
			//构建msg，将信息发到CommModule中
			context.Send(deviceID, dtcommon.SendToEdge, dtcommon.CommModule,
				context.BuildModelMessage(modules.BusGroup, "", topic, "publish", payload))
		}
	}

	return nil, nil
}

//DealMsgAttr get diff,0:update, 1:detail
func DealMsgAttr(context *dtcontext.DTContext, deviceID string, msgAttrs map[string]*dttype.MsgAttr, dealType int) dttype.DealAttrResult {
	deviceModel, ok := context.GetDevice(deviceID)
	if !ok {

	}
	attrs := deviceModel.Attributes
	if attrs == nil { //若attrs为空则初始化一个
		deviceModel.Attributes = make(map[string]*dttype.MsgAttr)
		attrs = deviceModel.Attributes
	}
	//创建add、deletes、update、result数组
	add := make([]dtclient.DeviceAttr, 0)
	deletes := make([]dtclient.DeviceDelete, 0)
	update := make([]dtclient.DeviceAttrUpdate, 0)
	result := make(map[string]*dttype.MsgAttr)

	for key, msgAttr := range msgAttrs {

		if attr, exist := attrs[key]; exist {
			if msgAttr == nil && dealType == 0 { //若msgAttr不为空
				if *attr.Optional {
					//将device信息加入deletes数组
					deletes = append(deletes, dtclient.DeviceDelete{DeviceID: deviceID, Name: key})
					result[key] = nil
					delete(attrs, key) //从attrs中删除key
				}
				continue
			}
			isChange := false
			cols := make(map[string]interface{})
			result[key] = &dttype.MsgAttr{}
			//若比较attr与msgAttr的值不同，那么僵msgAttr的value赋给attr
			if strings.Compare(attr.Value, msgAttr.Value) != 0 {
				attr.Value = msgAttr.Value

				cols["value"] = msgAttr.Value
				result[key].Value = msgAttr.Value

				isChange = true
			}
			if msgAttr.Metadata != nil { //若metadat不为nil，那么处理result[key]的metadata
				msgMetaJSON, _ := json.Marshal(msgAttr.Metadata)
				attrMetaJSON, _ := json.Marshal(attr.Metadata)
				if strings.Compare(string(msgMetaJSON), string(attrMetaJSON)) != 0 {
					cols["attr_type"] = msgAttr.Metadata.Type
					meta := dttype.CopyMsgAttr(msgAttr)
					attr.Metadata = meta.Metadata
					msgAttr.Metadata.Type = ""
					metaJSON, _ := json.Marshal(msgAttr.Metadata)
					cols["metadata"] = string(metaJSON)
					msgAttr.Metadata.Type = cols["attr_type"].(string)
					result[key].Metadata = meta.Metadata
					isChange = true
				}
			}
			if msgAttr.Optional != nil { //若Optional不为空，配置result[key].Optional
				if *msgAttr.Optional != *attr.Optional && *attr.Optional {
					optional := *msgAttr.Optional
					cols["optional"] = optional
					attr.Optional = &optional
					result[key].Optional = &optional
					isChange = true
				}
			}
			if isChange { //若标记为修改过，那么更新update数组
				update = append(update, dtclient.DeviceAttrUpdate{DeviceID: deviceID, Name: key, Cols: cols})
			} else {
				delete(result, key)
			}

		} else {
			//attrs[key]为空。格式转换msgattr to deviceattr
			deviceAttr := dttype.MsgAttrToDeviceAttr(key, msgAttr)
			deviceAttr.DeviceID = deviceID
			deviceAttr.Value = msgAttr.Value
			if msgAttr.Optional != nil { //为deviceAttr赋值Optional
				optional := *msgAttr.Optional
				deviceAttr.Optional = optional
			}
			if msgAttr.Metadata != nil { //为deviceAttr赋值Metadata
				//todo
				deviceAttr.AttrType = msgAttr.Metadata.Type
				msgAttr.Metadata.Type = ""
				metaJSON, _ := json.Marshal(msgAttr.Metadata)
				msgAttr.Metadata.Type = deviceAttr.AttrType
				deviceAttr.Metadata = string(metaJSON)
			}
			add = append(add, deviceAttr)
			attrs[key] = msgAttr //把msgAttr加入到attrs和result中
			result[key] = msgAttr
		}
	}
	if dealType > 0 { //若处理方式为detail，那么检查attrs里的key是否有数据，无数据则将key删除
		for key := range attrs {
			if _, exist := msgAttrs[key]; !exist {
				deletes = append(deletes, dtclient.DeviceDelete{DeviceID: deviceID, Name: key})
				result[key] = nil
			}
		}
		for _, v := range deletes {
			delete(attrs, v.Name)
		}
	} //将add、delete、update、result返回
	return dttype.DealAttrResult{Add: add, Delete: deletes, Update: update, Result: result, Err: nil}
}
