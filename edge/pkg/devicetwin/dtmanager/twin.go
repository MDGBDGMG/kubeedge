package dtmanager

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"k8s.io/klog"

	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/edge/pkg/common/modules"
	"github.com/kubeedge/kubeedge/edge/pkg/devicetwin/dtclient"
	"github.com/kubeedge/kubeedge/edge/pkg/devicetwin/dtcommon"
	"github.com/kubeedge/kubeedge/edge/pkg/devicetwin/dtcontext"
	"github.com/kubeedge/kubeedge/edge/pkg/devicetwin/dttype"
)

const (
	//RestDealType update from mqtt
	RestDealType = 0
	//SyncDealType update form cloud sync
	SyncDealType = 1
	//DetailDealType detail update from cloud
	DetailDealType = 2
	//SyncTwinDeleteDealType twin delete when sync
	SyncTwinDeleteDealType = 3
	//DealActual deal actual
	DealActual = 1
	//DealExpected deal exepected
	DealExpected = 0
)

var (
	//twinActionCallBack map for action to callback
	twinActionCallBack         map[string]CallBack
	initTwinActionCallBackOnce sync.Once
)

//TwinWorker deal twin event
type TwinWorker struct {
	Worker
	Group string
}

//Start worker
//启动TwinWorker
func (tw TwinWorker) Start() {
	initTwinActionCallBack()
	for {
		select {
		case msg, ok := <-tw.ReceiverChan: //接收msg
			if !ok {
				return
			}
			if dtMsg, isDTMessage := msg.(*dttype.DTMessage); isDTMessage {
				//按照action，调用回调函数
				if fn, exist := twinActionCallBack[dtMsg.Action]; exist {
					_, err := fn(tw.DTContexts, dtMsg.Identity, dtMsg.Msg)
					if err != nil {
						klog.Errorf("TwinModule deal %s event failed", dtMsg.Action)
					}
				} else {
					klog.Errorf("TwinModule deal %s event failed, not found callback", dtMsg.Action)
				}
			}

		case v, ok := <-tw.HeartBeatChan: //心跳检查
			if !ok {
				return
			}
			if err := tw.DTContexts.HeartBeat(tw.Group, v); err != nil {
				return
			}
		}
	}
}

func initTwinActionCallBack() {
	//init仅执行一次
	initTwinActionCallBackOnce.Do(func() {
		twinActionCallBack = make(map[string]CallBack)
		//获取设备twin信息，msgTwin信息，将两者比较并对设备的twin进行更新（add\delete\update）；
		//将更新过的twin信息写入DB
		//将twin与msgTwin的比较结果，经由community发布到mqtt
		//将syncResult，经由community发布到cloud

		//SyncDealType为0，即msg来自于mqtt，那么就是将twin的actualversion自己加1
		twinActionCallBack[dtcommon.TwinUpdate] = dealTwinUpdate

		//校验msg，将msg中包含的twin event包装为payload，发布到edge的mqtt
		twinActionCallBack[dtcommon.TwinGet] = dealTwinGet

		//获取设备twin信息，msgTwin信息，将两者比较并对设备的twin进行更新（add\delete\update）；
		//将更新过的twin信息写入DB
		//将twin与msgTwin的比较结果，经由community发布到mqtt
		//将syncResult，经由community发布到cloud

		//SyncDealType为1，即msg来自于并非，那么就是将cloud的actualversion和exceptversion给twin
		twinActionCallBack[dtcommon.TwinCloudSync] = dealTwinSync
	})
}

func dealTwinSync(context *dtcontext.DTContext, resource string, msg interface{}) (interface{}, error) {
	klog.Infof("Twin Sync EVENT")
	message, ok := msg.(*model.Message)
	if !ok {
		return nil, errors.New("msg not Message type")
	}
	result := []byte("")
	content, ok := message.Content.([]byte)
	if !ok {
		return nil, errors.New("invalid message content")
	}
	//解析msg为msgTwin
	msgTwin, err := dttype.UnmarshalDeviceTwinUpdate(content)
	if err != nil {
		klog.Errorf("Unmarshal update request body failed, err: %#v", err)
		dealUpdateResult(context, "", "", dtcommon.BadRequestCode, errors.New("Unmarshal update request body failed, Please check the request"), result)
		return nil, err
	}

	klog.Infof("Begin to update twin of the device %s", resource)
	eventID := msgTwin.EventID
	context.Lock(resource)
	//获取设备twin信息，msgTwin信息，将两者比较并对设备的twin进行更新（add\delete\update）；将更新过的twin信息写入DB
	//将twin与msgTwin的比较结果，经由community发布到mqtt
	//将syncResult，经由community发布到cloud

	//SyncDealType为1，那么就是将cloud的actualversion和exceptversion给twin
	DealDeviceTwin(context, resource, eventID, msgTwin.Twin, SyncDealType)
	context.Unlock(resource)
	//todo send ack
	return nil, nil
}

//校验msg，将msg中包含的twin event包装为payload，发布到edge的mqtt
func dealTwinGet(context *dtcontext.DTContext, resource string, msg interface{}) (interface{}, error) {
	klog.Infof("Twin Get EVENT")
	message, ok := msg.(*model.Message)
	if !ok {
		return nil, errors.New("msg not Message type")
	}

	content, ok := message.Content.([]byte)
	if !ok {
		return nil, errors.New("invalid message content")
	}
	//将twinevent包装为payload，发布到edge的mqtt
	DealGetTwin(context, resource, content)
	return nil, nil
}

//获取设备twin信息，msgTwin信息，将两者比较并对设备的twin进行更新（add\delete\update）；将更新过的twin信息写入DB
//将twin与msgTwin的比较结果，经由community发布到mqtt
//将syncResult，经由community发布到cloud
func dealTwinUpdate(context *dtcontext.DTContext, resource string, msg interface{}) (interface{}, error) {
	klog.Infof("Twin Update EVENT")
	message, ok := msg.(*model.Message)
	if !ok {
		return nil, errors.New("msg not Message type")
	}

	content, ok := message.Content.([]byte)
	if !ok {
		return nil, errors.New("invalid message content")
	}

	context.Lock(resource)
	//执行updated操作
	//获取设备twin信息，msgTwin信息，将两者比较并对设备的twin进行更新（add\delete\update）；将更新过的twin信息写入DB
	//将twin与msgTwin的比较结果，经由community发布到mqtt
	//将syncResult，经由community发布到cloud
	Updated(context, resource, content)
	context.Unlock(resource)
	return nil, nil
}

// Updated update the snapshot
//更新快照
//获取设备twin信息，msgTwin信息，将两者比较并对设备的twin进行更新（add\delete\update）；将更新过的twin信息写入DB
//将twin与msgTwin的比较结果，经由community发布到mqtt
//将syncResult，经由community发布到cloud
func Updated(context *dtcontext.DTContext, deviceID string, payload []byte) {
	result := []byte("")
	msg, err := dttype.UnmarshalDeviceTwinUpdate(payload) //格式转化
	if err != nil {
		//若格式转化失败
		klog.Errorf("Unmarshal update request body failed, err: %#v", err)
		//将contest转化格式，写入result经过CommWorker,发送到edge的busgroup的channel
		dealUpdateResult(context, "", "", dtcommon.BadRequestCode, err, result)
		return
	}
	klog.Infof("Begin to update twin of the device %s", deviceID)
	eventID := msg.EventID
	//若格式转化成功
	//处理设备孪生
	//获取设备twin信息，msgTwin信息，将两者比较并对设备的twin进行更新（add\delete\update）；将更新过的twin信息写入DB
	//将twin与msgTwin的比较结果，经由community发布到mqtt
	//将syncResult，经由community发布到cloud
	DealDeviceTwin(context, deviceID, eventID, msg.Twin, RestDealType)
}

//DealDeviceTwin deal device twin
//处理设备孪生
//获取设备twin信息，msgTwin信息，将两者比较并对设备的twin进行更新（add\delete\update）；将更新过的twin信息写入DB
//将twin与msgTwin的比较结果，经由community发布到mqtt
//将syncResult，经由community发布到cloud
func DealDeviceTwin(context *dtcontext.DTContext, deviceID string, eventID string, msgTwin map[string]*dttype.MsgTwin, dealType int) error {
	klog.Infof("Begin to deal device twin of the device %s", deviceID)
	now := time.Now().UnixNano() / 1e6
	result := []byte("")
	deviceModel, isExist := context.GetDevice(deviceID) //从context中获取device
	if !isExist {
		klog.Errorf("Update twin rejected due to the device %s is not existed", deviceID)
		//若context中不存在deviceID，那么将result经过CommWorker,发送到edge的busgroup的channel
		dealUpdateResult(context, deviceID, eventID, dtcommon.NotFoundCode, errors.New("Update rejected due to the device is not existed"), result)
		return errors.New("Update rejected due to the device is not existed")
	}
	content := msgTwin
	var err error
	if content == nil { //msgTwin为nil，则报异常。并将result经过CommWorker,发送到edge的busgroup的channel
		klog.Errorf("Update twin of device %s error, the update request body not have key:twin", deviceID)
		err = errors.New("Update twin error, the update request body not have key:twin")
		dealUpdateResult(context, deviceID, eventID, dtcommon.BadRequestCode, err, result)
		return err
	}
	//比较device的twin信息和MsgTwin中信息的区别，并将比较结果写入result；返回比较结果
	//新建/更新/删除device的twin信息，并将device写入context.deviceList管理起来
	dealTwinResult := DealMsgTwin(context, deviceID, content, dealType)

	add, deletes, update := dealTwinResult.Add, dealTwinResult.Delete, dealTwinResult.Update
	//若msg来自mqtt，并且twin信息的比较结果有异常
	if dealType == RestDealType && dealTwinResult.Err != nil {
		//那么从sqlite中同步device信息到Context.DeviceList中
		SyncDeviceFromSqlite(context, deviceID)
		err = dealTwinResult.Err
		//构建一个结构体，包含{eventid，timestamp，msgtwin}，打包为一个payload
		updateResult, _ := dttype.BuildDeviceTwinResult(dttype.BaseMessage{EventID: eventID, Timestamp: now}, dealTwinResult.Result, 0)
		//将result经过CommWorker,发送到edge的busgroup的channel
		dealUpdateResult(context, deviceID, eventID, dtcommon.BadRequestCode, err, updateResult)
		//返回异常
		return err
	}
	//若device的twin和msgTwin比较过程中，有add、delete、update操作
	if len(add) != 0 || len(deletes) != 0 || len(update) != 0 {
		for i := 1; i <= dtcommon.RetryTimes; i++ {
			//开启事务，将DB中的device twin信息进行相应的add、delete、update
			err = dtclient.DeviceTwinTrans(add, deletes, update)
			if err == nil {
				break
			}
			time.Sleep(dtcommon.RetryInterval)
		}
		if err != nil {
			////从sqlite中同步device信息到Context.DeviceList中
			SyncDeviceFromSqlite(context, deviceID)
			//打印异常
			klog.Errorf("Update device twin failed due to writing sql error: %v", err)
		}
	}
	//若deviceTwin组件与DB操作时有异常
	if err != nil && dealType == RestDealType {
		//构建一个结构体，包含{eventid，timestamp，msgtwin}，打包为一个payload
		updateResult, _ := dttype.BuildDeviceTwinResult(dttype.BaseMessage{EventID: eventID, Timestamp: now}, dealTwinResult.Result, dealType)
		//将result经过CommWorker,发送到edge的busgroup的channel
		dealUpdateResult(context, deviceID, eventID, dtcommon.InternalErrorCode, err, updateResult)
		//返回异常
		return err
	}
	//若msg来自mqtt
	if dealType == RestDealType {
		//构建一个结构体，包含{eventid，timestamp，msgtwin}，打包为一个payload
		updateResult, _ := dttype.BuildDeviceTwinResult(dttype.BaseMessage{EventID: eventID, Timestamp: now}, dealTwinResult.Result, dealType)
		//将result经过CommWorker,发送到edge的busgroup的channel
		dealUpdateResult(context, deviceID, eventID, dtcommon.InternalErrorCode, nil, updateResult)
	}
	//若document(twin信息)存在
	if len(dealTwinResult.Document) > 0 {
		////将document(twin信息)打包为payload，将payload经由communityWorker，发送到edge，然后让busGroup对上述topic执行publish操作
		dealDocument(context, deviceID, dttype.BaseMessage{EventID: eventID, Timestamp: now}, dealTwinResult.Document)
	}
	//构建一个payload，包含{eventID，timestamp，DeviceTwin，delta}
	delta, ok := dttype.BuildDeviceTwinDelta(dttype.BuildBaseMessage(), deviceModel.Twin)
	if ok {
		//若构建delta成功，则
		//将delta的payload，经由communityWorker，发送到busgroup，
		//对"$hw/events/device/"+deviceID+"/twin/update/delta"的topic执行publish操作
		dealDelta(context, deviceID, delta)
	}
	//若dealTwinResult.SyncResult有数据
	if len(dealTwinResult.SyncResult) > 0 {
		//将syncresult信息打包为payload，经由communityWorker，将信息发送到cloud的resource组
		//对于resource进行update操作
		//将信息同步到cloud
		dealSyncResult(context, deviceID, dttype.BuildBaseMessage(), dealTwinResult.SyncResult)
	}
	return nil
}

//dealUpdateResult build update result and send result, if success send the current state
//将result经过CommWorker,发送到edge的busgroup的channel
func dealUpdateResult(context *dtcontext.DTContext, deviceID string, eventID string, code int, err error, payload []byte) error {
	klog.Infof("Deal update result of device %s: Build and send result", deviceID)

	topic := dtcommon.DeviceETPrefix + deviceID + dtcommon.TwinETUpdateResultSuffix
	reason := ""
	para := dttype.Parameter{
		EventID: eventID,
		Code:    code,
		Reason:  reason}
	result := []byte("")
	var jsonErr error
	if err == nil {
		result = payload //将payload赋值给result
	} else {
		para.Reason = err.Error()
		result, jsonErr = dttype.BuildErrorResult(para)
		if jsonErr != nil {
			klog.Errorf("Unmarshal error result of device %s error, err: %v", deviceID, jsonErr)
			return jsonErr
		}
	}
	klog.Infof("Deal update result of device %s: send result", deviceID)
	//将result经过CommWorker,发送到edge的busgroup的channel
	return context.Send("",
		dtcommon.SendToEdge,
		dtcommon.CommModule,
		context.BuildModelMessage(modules.BusGroup, "", topic, "publish", result))
}

// dealDelta  send delta
//将delta的payload，经由communityWorker，发送到busgroup，对"$hw/events/device/"+deviceID+"/twin/update/delta"
//的topic执行publish操作
func dealDelta(context *dtcontext.DTContext, deviceID string, payload []byte) error {
	//"$hw/events/device/"+deviceID+"/twin/update/delta"
	topic := dtcommon.DeviceETPrefix + deviceID + dtcommon.TwinETDeltaSuffix
	klog.Infof("Deal delta of device %s: send delta", deviceID)
	return context.Send("",
		dtcommon.SendToEdge,
		dtcommon.CommModule,
		context.BuildModelMessage(modules.BusGroup, "", topic, "publish", payload))
}

// dealSyncResult build and send sync result, is delta update
//将syncresult信息打包为payload，经由communityWorker，将信息发送到cloud的resource组
//对于resource进行update操作
//将信息同步到cloud
func dealSyncResult(context *dtcontext.DTContext, deviceID string, baseMessage dttype.BaseMessage, twin map[string]*dttype.MsgTwin) error {

	klog.Infof("Deal sync result of device %s: sync with cloud", deviceID)
	resource := "device/" + deviceID + "/twin/edge_updated"
	return context.Send("",
		dtcommon.SendToCloud,
		dtcommon.CommModule,
		context.BuildModelMessage("resource", "", resource, "update", dttype.DeviceTwinResult{BaseMessage: baseMessage, Twin: twin}))
}

//dealDocument build document and save current state as last state, update sqlite
//将document(twin信息)打包为payload，将payload经由communityWorker，发送到edge，然后让busGroup对上述topic执行publish操作
func dealDocument(context *dtcontext.DTContext, deviceID string, baseMessage dttype.BaseMessage, twinDocument map[string]*dttype.TwinDoc) error {

	klog.Infof("Deal document of device %s: build and send document", deviceID)
	//构建payload，内容为{eventId，timestamp，Twin信息}
	payload, _ := dttype.BuildDeviceTwinDocument(baseMessage, twinDocument)
	//构建topic："$hw/events/device/"+deviceid+"/twin/update/document"
	topic := dtcommon.DeviceETPrefix + deviceID + dtcommon.TwinETDocumentSuffix
	klog.Infof("Deal document of device %s: send document", deviceID)
	//将payload经由communityWorker，发送到edge，然后让busGroup对上述topic执行publish操作
	return context.Send("",
		dtcommon.SendToEdge,
		dtcommon.CommModule,
		context.BuildModelMessage(modules.BusGroup, "", topic, "publish", payload))
}

// DealGetTwin deal get twin event
//将twinevent包装为payload，发布到edge的mqtt
func DealGetTwin(context *dtcontext.DTContext, deviceID string, payload []byte) error {

	klog.Info("Deal the event of getting twin")
	msg := []byte("")
	para := dttype.Parameter{}
	//解析payload
	edgeGet, err := dttype.UnmarshalBaseMessage(payload)
	if err != nil { //解析payload失败返回异常
		klog.Errorf("Unmarshal twin info %s failed , err: %#v", string(payload), err)
		para.Code = dtcommon.BadRequestCode
		para.Reason = fmt.Sprintf("Unmarshal twin info %s failed , err: %#v", string(payload), err)
		var jsonErr error
		msg, jsonErr = dttype.BuildErrorResult(para)
		if jsonErr != nil {
			klog.Errorf("Unmarshal error result error, err: %v", jsonErr)
			return jsonErr
		}
	} else { //解析payload成功
		para.EventID = edgeGet.EventID
		doc, exist := context.GetDevice(deviceID)
		if !exist { //若在context中找不到payload中的deviceID
			klog.Errorf("Device %s not found while getting twin", deviceID)
			para.Code = dtcommon.NotFoundCode
			para.Reason = fmt.Sprintf("Device %s not found while getting twin", deviceID)
			var jsonErr error
			msg, jsonErr = dttype.BuildErrorResult(para)
			if jsonErr != nil {
				klog.Errorf("Unmarshal error result error, err: %v", jsonErr)
				return jsonErr
			}
		} else { //若在contex存在payload中的deviceID
			now := time.Now().UnixNano() / 1e6
			var err error
			//构建一个结构体，包含{eventid，timestamp，msgtwin}，打包为一个payload
			msg, err = dttype.BuildDeviceTwinResult(dttype.BaseMessage{EventID: edgeGet.EventID, Timestamp: now}, doc.Twin, RestDealType)
			if err != nil { //若有异常，处理异常
				klog.Errorf("Build state while deal get twin err: %#v", err)
				para.Code = dtcommon.InternalErrorCode
				para.Reason = fmt.Sprintf("Build state while deal get twin err: %#v", err)
				var jsonErr error
				msg, jsonErr = dttype.BuildErrorResult(para)
				if jsonErr != nil {
					klog.Errorf("Unmarshal error result error, err: %v", jsonErr)
					return jsonErr
				}
			}
		}
	}
	//将payload经由communityWorker发布到Edge的busgroup，然后对topic进行publish
	topic := dtcommon.DeviceETPrefix + deviceID + dtcommon.TwinETGetResultSuffix
	klog.Infof("Deal the event of getting twin of device %s: send result ", deviceID)
	return context.Send("",
		dtcommon.SendToEdge,
		dtcommon.CommModule,
		context.BuildModelMessage(modules.BusGroup, "", topic, "publish", msg))
}

//dealtype 0:update ,2:cloud_update,1:detail result,3:deleted
//校验twin和msgTwin的版本是否正常，然后将msgTwin版本赋值给twin
//若dealType为0：version.EdgeVersion自行加1
//若dealType不为0：将msgTwin的版本赋值给twin，即提升/未改变twin的版本
//		        version.CloudVersion = reqVesion.CloudVersion
//		        version.EdgeVersion = reqVesion.EdgeVersion
func dealVersion(version *dttype.TwinVersion, reqVesion *dttype.TwinVersion, dealType int) (bool, error) {
	if dealType == RestDealType { //若来自mqtt
		//将twin的edgeversion加1
		version.EdgeVersion = version.EdgeVersion + 1
	} else if dealType >= SyncDealType {
		//若非来自mqtt
		if reqVesion == nil { //若msgTwin的版本为空，同时dealtype为delete，报异常
			if dealType == SyncTwinDeleteDealType {
				return true, nil
			}
			return false, errors.New("Version not allowed be nil while syncing")
		}
		//若twin的cloud版本 > msgTwin的cloud版本，报异常（潜台词就是twin版本必须小于等于msgTwin的版本）
		if version.CloudVersion > reqVesion.CloudVersion {
			return false, errors.New("Version not allowed")
		}
		//若twin的edge版本 > msgTwin的edge版本，报异常
		if version.EdgeVersion > reqVesion.EdgeVersion {
			return false, errors.New("Not allowed to sync due to version conflict")
		}
		//将msgTwin的版本赋值给twin，即提升/未改变twin的版本
		version.CloudVersion = reqVesion.CloudVersion
		version.EdgeVersion = reqVesion.EdgeVersion
	}
	return true, nil
}

//比较twin和msgTwin的 expectversion和 actualversion，然后更新twin的expectversion和 actualversion
//若twin有更新操作，那么将更新信息写入result
//若msgTwin和twin的版本信息校验失败，那么在result中删除key
func dealTwinDelete(returnResult *dttype.DealTwinResult, deviceID string, key string, twin *dttype.MsgTwin, msgTwin *dttype.MsgTwin, dealType int) error {
	document := returnResult.Document
	document[key] = &dttype.TwinDoc{}
	//从*MsgTwin中获取数据，返回MsgTwin
	copytwin := dttype.CopyMsgTwin(twin, true)
	document[key].LastState = &copytwin
	cols := make(map[string]interface{})
	syncResult := returnResult.SyncResult
	syncResult[key] = &dttype.MsgTwin{}
	update := returnResult.Update
	isChange := false
	//两种情况
	//1.msg来自mqtt：且msgTwin为nil。
	//2.msg并非来自mqtt，且msgTwin的tpye为deleted
	if msgTwin == nil && dealType == RestDealType && *twin.Optional || dealType >= SyncDealType && strings.Compare(msgTwin.Metadata.Type, "deleted") == 0 {
		if twin.Metadata != nil && strings.Compare(twin.Metadata.Type, "deleted") == 0 {
			return nil
		}
		//若twin的update信息不是来自mqtt
		if dealType != RestDealType {
			//赋值为twin delete when sync，同步时删除twin
			dealType = SyncTwinDeleteDealType
		}
		hasTwinExpected := true
		//如果twin.ExpectedVersion为空，那么初始化
		if twin.ExpectedVersion == nil {
			twin.ExpectedVersion = &dttype.TwinVersion{}
			hasTwinExpected = false
		}
		//如果twin.ExpectedVersion不为空
		if hasTwinExpected {
			//取出twin的expectedVersion
			expectedVersion := twin.ExpectedVersion

			var msgTwinExpectedVersion *dttype.TwinVersion
			if dealType != RestDealType { //并非来自mqtt
				msgTwinExpectedVersion = msgTwin.ExpectedVersion
			}
			//校验twin和msgTwin的expectedVersion是否正常，然后将msgTwin的expectedVersion赋值给twin
			ok, _ := dealVersion(expectedVersion, msgTwinExpectedVersion, dealType)
			if !ok {
				if dealType != RestDealType { //并非来自mqtt
					copySync := dttype.CopyMsgTwin(twin, false)
					syncResult[key] = &copySync
					//若版本不同，将document的key删除
					delete(document, key)
					//将同步结果写入returnResult
					returnResult.SyncResult = syncResult
					return nil
				}
			} else {
				//若twin的expectedVersion更新成功，更新twin和cols
				expectedVersionJSON, _ := json.Marshal(expectedVersion)
				cols["expected_version"] = string(expectedVersionJSON)
				cols["attr_type"] = "deleted"
				cols["expected_meta"] = nil
				cols["expected"] = nil
				if twin.Expected == nil {
					twin.Expected = &dttype.TwinValue{}
				}
				twin.Expected.Value = nil
				twin.Expected.Metadata = nil
				twin.ExpectedVersion = expectedVersion
				twin.Metadata = &dttype.TypeMetadata{Type: "deleted"}
				if dealType == RestDealType {
					copySync := dttype.CopyMsgTwin(twin, false)
					syncResult[key] = &copySync
				}
				document[key].CurrentState = nil
				isChange = true
			}
		}
		hasTwinActual := true

		//若ActualVersion为空，初始化
		if twin.ActualVersion == nil {
			twin.ActualVersion = &dttype.TwinVersion{}
			hasTwinActual = false
		}
		if hasTwinActual {
			//若twin.ActualVersion不为空
			actualVersion := twin.ActualVersion
			var msgTwinActualVersion *dttype.TwinVersion
			if dealType != RestDealType {
				msgTwinActualVersion = msgTwin.ActualVersion
			}
			//比较twin和msgTwin的actualVersion，并将msgTwin的actualVersion赋值给twin
			ok, _ := dealVersion(actualVersion, msgTwinActualVersion, dealType)
			if !ok { //若actualVersion配置失败
				if dealType != RestDealType {
					copySync := dttype.CopyMsgTwin(twin, false)
					syncResult[key] = &copySync
					delete(document, key)
					returnResult.SyncResult = syncResult
					return nil
				}
			} else {
				//若actualVersion设置成功，则配置twin的参数
				actualVersionJSON, _ := json.Marshal(actualVersion)

				cols["actual_version"] = string(actualVersionJSON)
				cols["attr_type"] = "deleted"
				cols["actual_meta"] = nil
				cols["actual"] = nil
				if twin.Actual == nil {
					twin.Actual = &dttype.TwinValue{}
				}
				twin.Actual.Value = nil
				twin.Actual.Metadata = nil
				twin.ActualVersion = actualVersion
				twin.Metadata = &dttype.TypeMetadata{Type: "deleted"}
				if dealType == RestDealType {
					copySync := dttype.CopyMsgTwin(twin, false)
					syncResult[key] = &copySync
				}
				document[key].CurrentState = nil
				isChange = true
			}
		}
	}

	if isChange {
		//若twin的actualVersion或expectedVersion更新过，将更新的信息写入update
		update = append(update, dtclient.DeviceTwinUpdate{DeviceID: deviceID, Name: key, Cols: cols})
		returnResult.Update = update
		if dealType == RestDealType { //若twin命令来自mqtt
			returnResult.Result[key] = nil
			returnResult.SyncResult = syncResult
		} else {
			//若twin命令并非来自mqtt
			delete(syncResult, key)
		}
		returnResult.Document = document
	} else {
		//若twin的version数据未发生update
		delete(document, key)
		delete(syncResult, key)

	}

	return nil
}

//0:expected ,1 :actual
func isTwinValueDiff(twin *dttype.MsgTwin, msgTwin *dttype.MsgTwin, dealType int) (bool, error) {
	hasTwin := false
	hasMsgTwin := false
	twinValue := twin.Expected
	msgTwinValue := msgTwin.Expected
	//如果处理actual，那么将twin和msgTwin的actual，赋值给变量
	if dealType == DealActual {
		twinValue = twin.Actual
		msgTwinValue = msgTwin.Actual
	}
	//校验
	if twinValue != nil {
		hasTwin = true
	}
	if msgTwinValue != nil {
		hasMsgTwin = true
	}
	valueType := "string"
	if strings.Compare(twin.Metadata.Type, "deleted") == 0 {
		//若twin的Metadata的type是deleted
		if msgTwin.Metadata != nil {
			//取出 msgTwin.Metadata.Type
			valueType = msgTwin.Metadata.Type
		}
	} else {
		//若twin的Metadata的type 不是 deleted
		//取出twin.Metadata.Type
		valueType = twin.Metadata.Type
	}
	if hasMsgTwin {
		if hasTwin {
			//校验*msgTwinValue.Value的数据类型是否与valueType一致
			err := dtcommon.ValidateValue(valueType, *msgTwinValue.Value)
			if err != nil {
				return false, err
			}
			return true, nil
		}
		return true, nil
	}
	return false, nil
}

//270行
func dealTwinCompare(returnResult *dttype.DealTwinResult, deviceID string, key string, twin *dttype.MsgTwin, msgTwin *dttype.MsgTwin, dealType int) error {
	klog.Info("dealtwincompare")
	now := time.Now().UnixNano() / 1e6

	document := returnResult.Document
	document[key] = &dttype.TwinDoc{}
	copytwin := dttype.CopyMsgTwin(twin, true)
	document[key].LastState = &copytwin
	//若twin的metadata为deleted，那么将LastState设置为nil
	if strings.Compare(twin.Metadata.Type, "deleted") == 0 {
		document[key].LastState = nil
	}
	//初始化coles
	cols := make(map[string]interface{})
	//初始化syncResult[key]
	syncResult := returnResult.SyncResult
	syncResult[key] = &dttype.MsgTwin{}
	//DeviceTwinUpdate
	update := returnResult.Update
	isChange := false
	isSyncAllow := true
	if msgTwin == nil {
		return nil
	}
	//校验msgTwin.Value的数据类型是否合法
	expectedOk, expectedErr := isTwinValueDiff(twin, msgTwin, DealExpected)
	if expectedOk {
		//取出msgTwin.Expected.Value
		value := msgTwin.Expected.Value
		meta := dttype.ValueMetadata{Timestamp: now}
		//初始化twin.ExpectedVersion
		if twin.ExpectedVersion == nil {
			twin.ExpectedVersion = &dttype.TwinVersion{}
		}
		version := twin.ExpectedVersion
		var msgTwinExpectedVersion *dttype.TwinVersion
		if dealType != RestDealType {
			msgTwinExpectedVersion = msgTwin.ExpectedVersion
		}
		//校验并更新twin的expectVersion
		ok, err := dealVersion(version, msgTwinExpectedVersion, dealType)
		if !ok {
			//若校验expectVersion失败,即拒绝同步
			// if reject the sync,  set the syncResult and then send the edge_updated msg
			if dealType != RestDealType { //msg并非来自mqtt
				//将twin的信息写入syncResult[key]
				syncResult[key].Expected = &dttype.TwinValue{Value: twin.Expected.Value, Metadata: twin.Expected.Metadata}
				syncResult[key].ExpectedVersion = &dttype.TwinVersion{CloudVersion: twin.ExpectedVersion.CloudVersion, EdgeVersion: twin.ExpectedVersion.EdgeVersion}

				syncOptional := *twin.Optional
				syncResult[key].Optional = &syncOptional

				metaJSON, _ := json.Marshal(twin.Metadata)
				var meta dttype.TypeMetadata
				json.Unmarshal(metaJSON, &meta)
				syncResult[key].Metadata = &meta
				//不允许同步
				isSyncAllow = false
			} else {
				//若msg来自于mqtt，返回异常
				returnResult.Err = err
				return err
			}
		} else {
			//若校验成功，,即可以同步数据
			metaJSON, _ := json.Marshal(meta)
			versionJSON, _ := json.Marshal(version)
			cols["expected"] = value
			cols["expected_meta"] = string(metaJSON)
			cols["expected_version"] = string(versionJSON)
			if twin.Expected == nil {
				twin.Expected = &dttype.TwinValue{}
			}
			//将msgTwin的Expected.Value，赋值给twin
			twin.Expected.Value = value
			//当前时间
			twin.Expected.Metadata = &meta
			//twin的version，或者其更新过的version
			twin.ExpectedVersion = version
			// if rest update, set the syncResult and send the edge_updated msg
			if dealType == RestDealType { //若msg来自于mqtt，更新syncResult
				syncResult[key].Expected = &dttype.TwinValue{Value: value, Metadata: &meta}
				syncResult[key].ExpectedVersion = &dttype.TwinVersion{CloudVersion: version.CloudVersion, EdgeVersion: version.EdgeVersion}
				syncOptional := *twin.Optional
				syncResult[key].Optional = &syncOptional
				metaJSON, _ := json.Marshal(twin.Metadata)
				var meta dttype.TypeMetadata
				json.Unmarshal(metaJSON, &meta)
				syncResult[key].Metadata = &meta
			}
			isChange = true
		}
	} else {
		//若msgTwin.Value的数据类型不合法，且来自于mqtt，就返回err
		if expectedErr != nil && dealType == RestDealType {
			returnResult.Err = expectedErr
			return expectedErr
		}
	}
	//校验msgTwin的actual数据类型是否合法
	actualOk, actualErr := isTwinValueDiff(twin, msgTwin, DealActual)
	if actualOk && isSyncAllow { //若actual数据类型合法，并且可以同步
		value := msgTwin.Actual.Value
		meta := dttype.ValueMetadata{Timestamp: now}
		if twin.ActualVersion == nil {
			twin.ActualVersion = &dttype.TwinVersion{}
		}
		version := twin.ActualVersion
		var msgTwinActualVersion *dttype.TwinVersion
		if dealType != RestDealType {
			msgTwinActualVersion = msgTwin.ActualVersion
		}
		//校验并更新twin的actualVersion
		ok, err := dealVersion(version, msgTwinActualVersion, dealType)
		if !ok { //若更新twin的actualVersion出现异常
			if dealType != RestDealType { //若msg并非来自mqtt
				//配置syncResult
				syncResult[key].Actual = &dttype.TwinValue{Value: twin.Actual.Value, Metadata: twin.Actual.Metadata}
				syncResult[key].ActualVersion = &dttype.TwinVersion{CloudVersion: twin.ActualVersion.CloudVersion, EdgeVersion: twin.ActualVersion.EdgeVersion}
				syncOptional := *twin.Optional
				syncResult[key].Optional = &syncOptional
				metaJSON, _ := json.Marshal(twin.Metadata)
				var meta dttype.TypeMetadata
				json.Unmarshal(metaJSON, &meta)
				syncResult[key].Metadata = &meta
				isSyncAllow = false
			} else {
				//若msg来自mqtt，返回异常
				returnResult.Err = err
				return err
			}
		} else { //若更新twin的actualVersion成功
			//配置cols和twin
			metaJSON, _ := json.Marshal(meta)
			versionJSON, _ := json.Marshal(version)
			cols["actual"] = value
			cols["actual_meta"] = string(metaJSON)
			cols["actual_version"] = string(versionJSON)
			if twin.Actual == nil {
				twin.Actual = &dttype.TwinValue{}
			}
			twin.Actual.Value = value
			twin.Actual.Metadata = &meta
			twin.ActualVersion = version
			if dealType == RestDealType { //若msg来自于mqtt，还需配置syncResult
				syncResult[key].Actual = &dttype.TwinValue{Value: msgTwin.Actual.Value, Metadata: &meta}
				syncOptional := *twin.Optional
				syncResult[key].Optional = &syncOptional
				metaJSON, _ := json.Marshal(twin.Metadata)
				var meta dttype.TypeMetadata
				json.Unmarshal(metaJSON, &meta)
				syncResult[key].Metadata = &meta
				syncResult[key].ActualVersion = &dttype.TwinVersion{CloudVersion: version.CloudVersion, EdgeVersion: version.EdgeVersion}
			}
			isChange = true
		}
	} else { //若actual数据类型不合法，返回err
		if actualErr != nil && dealType == RestDealType {
			returnResult.Err = actualErr
			return actualErr
		}
	}

	if isSyncAllow { //若可以同步
		if msgTwin.Optional != nil { //若msgTwin的option不为空，配置optional
			if *msgTwin.Optional != *twin.Optional && *twin.Optional {
				optional := *msgTwin.Optional
				cols["optional"] = optional
				twin.Optional = &optional
				syncOptional := *twin.Optional
				syncResult[key].Optional = &syncOptional
				isChange = true
			}
		}
		// if update the deleted twin, allow to update attr_type
		if msgTwin.Metadata != nil {
			//若msgTwin.Metadata不为空，
			//那么可以配置attr_type为msgTwin.Metadata.Type
			msgMetaJSON, _ := json.Marshal(msgTwin.Metadata)
			twinMetaJSON, _ := json.Marshal(twin.Metadata)
			//若msgTwin和twin的metadata不一致
			if strings.Compare(string(msgMetaJSON), string(twinMetaJSON)) != 0 {
				meta := dttype.CopyMsgTwin(msgTwin, true)
				meta.Metadata.Type = ""
				metaJSON, _ := json.Marshal(meta.Metadata)
				cols["metadata"] = string(metaJSON)
				//若更新已删除的twin
				if strings.Compare(twin.Metadata.Type, "deleted") == 0 {
					cols["attr_type"] = msgTwin.Metadata.Type
					//更新twin.Metadata.Type
					twin.Metadata.Type = msgTwin.Metadata.Type
					var meta dttype.TypeMetadata
					json.Unmarshal(msgMetaJSON, &meta)
					syncResult[key].Metadata = &meta
				}
				isChange = true
			}
		} else { //若msgTwin.Metadata为空，且twin.Metadata.Type为"deleted"
			if strings.Compare(twin.Metadata.Type, "deleted") == 0 {
				twin.Metadata = &dttype.TypeMetadata{Type: "string"}
				cols["attr_type"] = "string"
				syncResult[key].Metadata = twin.Metadata
				isChange = true
			}
		}

	}
	if isChange { //若校验之后，可以修改
		//将cols写入returnResult
		update = append(update, dtclient.DeviceTwinUpdate{DeviceID: deviceID, Name: key, Cols: cols})
		returnResult.Update = update
		current := dttype.CopyMsgTwin(twin, true)
		document[key].CurrentState = &current
		returnResult.Document = document

		if dealType == RestDealType {
			copyResult := dttype.CopyMsgTwin(syncResult[key], true)
			returnResult.Result[key] = &copyResult
			returnResult.SyncResult = syncResult
		} else {
			if !isSyncAllow {
				returnResult.SyncResult = syncResult
			} else {
				delete(syncResult, key)
			}
		}

	} else { //若校验之后，不可修改，清理现场
		if dealType == RestDealType {
			delete(document, key)
			delete(syncResult, key)
		} else {
			delete(document, key)
			if !isSyncAllow {
				returnResult.SyncResult = syncResult
			} else {
				delete(syncResult, key)
			}
		}

	}
	return nil
}

//处理Twin和msgTwin的ADD过程
func dealTwinAdd(returnResult *dttype.DealTwinResult, deviceID string, key string, twins map[string]*dttype.MsgTwin, msgTwin *dttype.MsgTwin, dealType int) error {
	now := time.Now().UnixNano() / 1e6
	document := returnResult.Document
	document[key] = &dttype.TwinDoc{}
	document[key].LastState = nil
	if msgTwin == nil {
		return errors.New("The request body is wrong")
	}
	deviceTwin := dttype.MsgTwinToDeviceTwin(key, msgTwin)
	deviceTwin.DeviceID = deviceID
	syncResult := returnResult.SyncResult
	syncResult[key] = &dttype.MsgTwin{}
	isChange := false
	//add deleted twin when syncing from cloud: add version
	if dealType != RestDealType && strings.Compare(msgTwin.Metadata.Type, "deleted") == 0 {
		if msgTwin.ExpectedVersion != nil {
			versionJSON, _ := json.Marshal(msgTwin.ExpectedVersion)
			deviceTwin.ExpectedVersion = string(versionJSON)
		}
		if msgTwin.ActualVersion != nil {
			versionJSON, _ := json.Marshal(msgTwin.ActualVersion)
			deviceTwin.ActualVersion = string(versionJSON)
		}
	}

	if msgTwin.Expected != nil {
		version := &dttype.TwinVersion{}
		var msgTwinExpectedVersion *dttype.TwinVersion
		if dealType != RestDealType {
			msgTwinExpectedVersion = msgTwin.ExpectedVersion
		}
		ok, err := dealVersion(version, msgTwinExpectedVersion, dealType)
		if !ok {
			// not match
			if dealType == RestDealType {
				returnResult.Err = err
				return err
			}
			// reject add twin
			return nil
		}
		// value type default string
		valueType := "string"
		if msgTwin.Metadata != nil {
			valueType = msgTwin.Metadata.Type
		}

		err = dtcommon.ValidateValue(valueType, *msgTwin.Expected.Value)
		if err == nil {
			meta := dttype.ValueMetadata{Timestamp: now}
			metaJSON, _ := json.Marshal(meta)
			versionJSON, _ := json.Marshal(version)
			deviceTwin.ExpectedMeta = string(metaJSON)
			deviceTwin.ExpectedVersion = string(versionJSON)
			deviceTwin.Expected = *msgTwin.Expected.Value
			isChange = true
		} else {
			delete(document, key)
			delete(syncResult, key)
			// reject add twin, if rest add return the err, while sync add return nil
			if dealType == RestDealType {
				returnResult.Err = err
				return err
			}
			return nil
		}
	}

	if msgTwin.Actual != nil {
		version := &dttype.TwinVersion{}
		var msgTwinActualVersion *dttype.TwinVersion
		if dealType != RestDealType {
			msgTwinActualVersion = msgTwin.ActualVersion
		}
		ok, err := dealVersion(version, msgTwinActualVersion, dealType)
		if !ok {
			if dealType == RestDealType {
				returnResult.Err = err
				return err
			}
			return nil
		}
		valueType := "string"
		if msgTwin.Metadata != nil {
			valueType = msgTwin.Metadata.Type
		}
		err = dtcommon.ValidateValue(valueType, *msgTwin.Actual.Value)
		if err == nil {
			meta := dttype.ValueMetadata{Timestamp: now}
			metaJSON, _ := json.Marshal(meta)
			versionJSON, _ := json.Marshal(version)
			deviceTwin.ActualMeta = string(metaJSON)
			deviceTwin.ActualVersion = string(versionJSON)
			deviceTwin.Actual = *msgTwin.Actual.Value
			isChange = true
		} else {
			delete(document, key)
			delete(syncResult, key)
			if dealType == RestDealType {
				returnResult.Err = err
				return err
			}
			return nil
		}
	}

	//add the optional of twin
	if msgTwin.Optional != nil {
		optional := *msgTwin.Optional
		deviceTwin.Optional = optional
		isChange = true
	} else {
		deviceTwin.Optional = true
		isChange = true
	}

	//add the metadata of the twin
	if msgTwin.Metadata != nil {
		//todo
		deviceTwin.AttrType = msgTwin.Metadata.Type
		msgTwin.Metadata.Type = ""
		metaJSON, _ := json.Marshal(msgTwin.Metadata)
		deviceTwin.Metadata = string(metaJSON)
		msgTwin.Metadata.Type = deviceTwin.AttrType
		isChange = true
	} else {
		deviceTwin.AttrType = "string"
		isChange = true
	}

	if isChange {
		//将msgTwin的信息写入twins相应的key
		twins[key] = dttype.DeviceTwinToMsgTwin([]dtclient.DeviceTwin{deviceTwin})[key]
		//将msgTwin信息写入result
		add := returnResult.Add
		add = append(add, deviceTwin)
		returnResult.Add = add

		copytwin := dttype.CopyMsgTwin(twins[key], true)
		if strings.Compare(twins[key].Metadata.Type, "deleted") == 0 {
			document[key].CurrentState = nil
		} else {
			document[key].CurrentState = &copytwin
		}
		returnResult.Document = document

		copySync := dttype.CopyMsgTwin(twins[key], false)
		syncResult[key] = &copySync
		if dealType == RestDealType { //如果来自mqtt
			copyResult := dttype.CopyMsgTwin(syncResult[key], true)
			returnResult.Result[key] = &copyResult
			returnResult.SyncResult = syncResult
		} else { //如果不是来自mqtt
			delete(syncResult, key)
		}

	} else {
		delete(document, key)
		delete(syncResult, key)
	}

	return nil

}

//DealMsgTwin get diff while updating twin
//比较device的twin信息和MsgTwin中信息的区别，并将比较结果写入result；
//新建/更新/删除device的twin信息，并将device写入context.deviceList管理起来
func DealMsgTwin(context *dtcontext.DTContext, deviceID string, msgTwins map[string]*dttype.MsgTwin, dealType int) dttype.DealTwinResult {
	add := make([]dtclient.DeviceTwin, 0)
	deletes := make([]dtclient.DeviceDelete, 0)
	update := make([]dtclient.DeviceTwinUpdate, 0)
	result := make(map[string]*dttype.MsgTwin)
	syncResult := make(map[string]*dttype.MsgTwin)
	document := make(map[string]*dttype.TwinDoc)
	returnResult := dttype.DealTwinResult{Add: add,
		Delete:     deletes,
		Update:     update,
		Result:     result,
		SyncResult: syncResult,
		Document:   document,
		Err:        nil}
	//检查deviceID
	deviceModel, ok := context.GetDevice(deviceID)
	if !ok {
		klog.Errorf("invalid device id")
		//返回错误结果
		return dttype.DealTwinResult{Add: add,
			Delete:     deletes,
			Update:     update,
			Result:     result,
			SyncResult: syncResult,
			Document:   document,
			Err:        errors.New("invalid device id")}
	}
	//格式转化，若Device.Twin为空；新建一个
	twins := deviceModel.Twin
	if twins == nil {
		deviceModel.Twin = make(map[string]*dttype.MsgTwin)
		twins = deviceModel.Twin
	}

	var err error
	for key, msgTwin := range msgTwins {
		//msgTwin中有的key，在twin中也有
		if twin, exist := twins[key]; exist {
			//若msgTwin元数据为nil
			if dealType >= 1 && msgTwin != nil && (msgTwin.Metadata == nil) {
				klog.Infof("Not found metadata of twin")
			}
			//msgTwin的metadata类型为“deleted”
			if msgTwin == nil && dealType == 0 || dealType >= 1 && strings.Compare(msgTwin.Metadata.Type, "deleted") == 0 {
				//执行Twin的Delete操作
				//比较twin和msgTwin的version，进行twin的version更新  或  在result里删除key
				err = dealTwinDelete(&returnResult, deviceID, key, twin, msgTwin, dealType)
				if err != nil {
					return returnResult
				}
				continue
			}
			//若不为deleted，执行Twin的比较操作
			//校验twin和msgTwin的版本信息，进行twin的actualVersion和exceptVersion的更新
			//若更新版本信息，则同步value等其他信息
			//将同步的信息过程写入result
			err = dealTwinCompare(&returnResult, deviceID, key, twin, msgTwin, dealType)
			if err != nil {
				return returnResult
			}
		} else {
			//msgTwin中有的key，在twin中不存在
			//执行Twin的add操作
			err = dealTwinAdd(&returnResult, deviceID, key, twins, msgTwin, dealType)
			if err != nil {
				return returnResult
			}

		}

	}
	//将device写入deviceList中
	context.DeviceList.Store(deviceID, deviceModel)
	return returnResult
}
