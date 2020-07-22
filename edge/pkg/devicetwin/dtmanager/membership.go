package dtmanager

import (
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

var (
	//memActionCallBack map for action to callback
	memActionCallBack map[string]CallBack
	mutex             sync.Mutex
)

//MemWorker deal membership event
type MemWorker struct {
	Worker
	Group string
}

//Start Membership Worker
func (mw MemWorker) Start() {
	initMemActionCallBack()
	for { //无限循环
		select {
		case msg, ok := <-mw.ReceiverChan: //若接到membershipWorker信息
			if !ok {
				return
			}
			if dtMsg, isDTMessage := msg.(*dttype.DTMessage); isDTMessage {
				//按action执行命令
				if fn, exist := memActionCallBack[dtMsg.Action]; exist {
					_, err := fn(mw.DTContexts, dtMsg.Identity, dtMsg.Msg)
					if err != nil { //检查数据是否有异常
						klog.Errorf("MemModule deal %s event failed: %v", dtMsg.Action, err)
					}
				} else {
					klog.Errorf("MemModule deal %s event failed, not found callback", dtMsg.Action)
				}
			}

		case v, ok := <-mw.HeartBeatChan: //若收到heartBeat信息，信息
			if !ok {
				return
			}
			if err := mw.DTContexts.HeartBeat(mw.Group, v); err != nil {
				return
			}
		}
	}
}

func initMemActionCallBack() {
	//创建CallBack的MAP
	memActionCallBack = make(map[string]CallBack)
	//将msg里的device信息写入DeviceList，并将DeviceList转化格式为payload，发布到mqtt
	memActionCallBack[dtcommon.MemGet] = dealMerbershipGet
	//处理Membership event，对deviceList和DB进行add\update\remove操作；将结果发布到mqtt
	memActionCallBack[dtcommon.MemUpdated] = dealMembershipUpdated
	//整理device
	//首先将devices的数据写入deviceList,对deviceList和DB进行add\update操作
	//然后比较deviceList与devices的数据差异（即deviceList有，但是devices没有）
	//最后差异的device取出来，移除
	memActionCallBack[dtcommon.MemDetailResult] = dealMembershipDetail
}

//比较context的DeviceList中的deviceID，若devices中不存在此deviceID
//则将此deviceID的device写入toRemove数组中
//返回toRemove数组
func getRemoveList(context *dtcontext.DTContext, devices []dttype.Device) []dttype.Device {
	var toRemove []dttype.Device
	context.DeviceList.Range(func(key interface{}, value interface{}) bool {
		isExist := false
		for _, v := range devices {
			if strings.Compare(v.ID, key.(string)) == 0 {
				isExist = true
			}
		}
		if !isExist {
			toRemove = append(toRemove, dttype.Device{ID: key.(string)})
		}
		return true
	})
	return toRemove
}

//整理device
//首先将devices的数据写入，本质上就是对deviceList进行add/update操作
//然后比较deviceList与devices的数据差异（即deviceList有，但是devices没有）
//最后差异的device取出来，移除
func dealMembershipDetail(context *dtcontext.DTContext, resource string, msg interface{}) (interface{}, error) {
	klog.Info("Deal node detail info")
	message, ok := msg.(*model.Message)
	if !ok {
		return nil, errors.New("msg not Message type")
	}

	contentData, ok := message.Content.([]byte)
	if !ok {
		return nil, errors.New("assertion failed")
	}

	devices, err := dttype.UnmarshalMembershipDetail(contentData)
	if err != nil {
		klog.Errorf("Unmarshal membership info failed , err: %#v", err)
		return nil, err
	}

	baseMessage := dttype.BaseMessage{EventID: devices.EventID}
	defer context.UnlockAll()
	context.LockAll()
	var toRemove []dttype.Device
	isDelta := false
	//将device加入到edge group中
	//将device信息写入db；处理device的twin；将device加入context.DeviceList
	//将上述操作的结果update发布到mqtt
	Added(context, devices.Devices, baseMessage, isDelta)
	//比较context的DeviceList中的deviceID，若devices中不存在此deviceID
	//则将此deviceID的device写入toRemove数组中
	//返回toRemove数组
	toRemove = getRemoveList(context, devices.Devices)

	if toRemove != nil || len(toRemove) != 0 {
		//从DB中依据DeviceID移除Device、DeviceAttr、DeviceTwin
		//从context的DeviceList和DeviceMutex中移除DeviceID
		//将remove消息经由community发布到mqtt
		Removed(context, toRemove, baseMessage, isDelta)
	}
	klog.Info("Deal node detail info successful")
	return nil, nil
}

//处理Membership event，对device进行add和remove操作
func dealMembershipUpdated(context *dtcontext.DTContext, resource string, msg interface{}) (interface{}, error) {
	klog.Infof("Membership event")
	message, ok := msg.(*model.Message)
	if !ok {
		return nil, errors.New("msg not Message type")
	}
	//转化格式为字节数组
	contentData, ok := message.Content.([]byte)
	if !ok {
		return nil, errors.New("assertion failed")
	}
	//将数据解析为MemberShipUpdate格式{baseMessage，add []device，reomve []device}
	updateEdgeGroups, err := dttype.UnmarshalMembershipUpdate(contentData)
	if err != nil {
		klog.Errorf("Unmarshal membership info failed , err: %#v", err)
		return nil, err
	}

	baseMessage := dttype.BaseMessage{EventID: updateEdgeGroups.EventID}
	//若addDevcie有数据，那么将数据写入
	if updateEdgeGroups.AddDevices != nil && len(updateEdgeGroups.AddDevices) > 0 {
		//add device
		//将device加入到edge group中
		//将device信息写入db；处理device的twin；将device加入context.DeviceList
		//将上述操作的结果update发布到mqtt
		Added(context, updateEdgeGroups.AddDevices, baseMessage, false)
	}
	if updateEdgeGroups.RemoveDevices != nil && len(updateEdgeGroups.RemoveDevices) > 0 {
		// delete device
		//从DB中依据DeviceID移除Device、DeviceAttr、DeviceTwin
		//从context的DeviceList和DeviceMutex中移除DeviceID
		//将remove消息经由community发布到mqtt
		Removed(context, updateEdgeGroups.RemoveDevices, baseMessage, false)
	}
	return nil, nil
}

//将msg里的device信息写入DeviceList，并将DeviceList发布到
func dealMerbershipGet(context *dtcontext.DTContext, resource string, msg interface{}) (interface{}, error) {
	klog.Infof("MEMBERSHIP EVENT")
	message, ok := msg.(*model.Message) //转为Message
	if !ok {
		return nil, errors.New("msg not Message type")
	}

	contentData, ok := message.Content.([]byte) //转为字节数组
	if !ok {
		return nil, errors.New("assertion failed")
	}
	//解析消息，消息转化格式，写入DeviceList，包装为一个payload
	//将payload包装为Message，发送到Edge、CommunicateModule组件中
	DealGetMembership(context, contentData)
	return nil, nil
}

// Added add device to the edge group
//将device加入到edge group中
//将device信息写入/更新db；处理device的twin；将device加入context.DeviceList
//将上述操作的结果update发布到mqtt
func Added(context *dtcontext.DTContext, toAdd []dttype.Device, baseMessage dttype.BaseMessage, delta bool) {
	klog.Infof("Add devices to edge group")
	if !delta {
		baseMessage.EventID = ""
	}
	if toAdd == nil || len(toAdd) == 0 {
		return
	}
	dealType := 0
	if !delta {
		dealType = 1
	}
	for _, device := range toAdd {
		//if device has existed, step out
		deviceModel, deviceExist := context.GetDevice(device.ID)
		if deviceExist {
			if delta {
				klog.Errorf("Add device %s failed, has existed", device.ID)
				continue
			}
			//更新device在DB中的属性。写入context中；若写入失败，则发送到communityWorker中，转发到Edge
			DeviceUpdated(context, device.ID, device.Attributes, baseMessage, dealType)
			//处理设备孪生
			//获取设备twin信息，msgTwin信息，将两者比较并对设备的twin进行更新（add\delete\update）；将更新过的twin信息写入DB
			//将twin与msgTwin的比较结果，经由community发布到mqtt
			//将syncResult，经由community发布到cloud
			DealDeviceTwin(context, device.ID, baseMessage.EventID, device.Twin, dealType)
			//todo sync twin
			continue
		}

		var deviceMutex sync.Mutex
		//将deviceID和同步锁写入sync.Map中
		context.DeviceMutex.Store(device.ID, &deviceMutex)

		if delta {
			context.Lock(device.ID)
		}
		//将device.ID以及deviceModel写入DeviceList管理起来
		deviceModel = &dttype.Device{ID: device.ID, Name: device.Name, Description: device.Description, State: device.State}
		context.DeviceList.Store(device.ID, deviceModel)

		//write to sqlite
		var err error
		adds := make([]dtclient.Device, 0)
		addAttr := make([]dtclient.DeviceAttr, 0)
		addTwin := make([]dtclient.DeviceTwin, 0)
		//使用DB事务将Device、DeviceAttr和DeviceTwin写入DB
		//此处addAttr、addTwin均为空值
		adds = append(adds, dtclient.Device{
			ID:          device.ID,
			Name:        device.Name,
			Description: device.Description,
			State:       device.State})
		for i := 1; i <= dtcommon.RetryTimes; i++ {
			err = dtclient.AddDeviceTrans(adds, addAttr, addTwin)
			if err == nil {
				break
			}
			time.Sleep(dtcommon.RetryInterval)
		}
		//若有异常，则将deviceID从DeviceList中删除
		if err != nil {
			klog.Errorf("Add device %s failed due to some error ,err: %#v", device.ID, err)
			context.DeviceList.Delete(device.ID)
			context.Unlock(device.ID)
			continue
			//todo
		}
		if device.Twin != nil {
			klog.Infof("Add device twin during first adding device %s", device.ID)
			//处理设备孪生
			//获取设备twin信息，msgTwin信息，将两者比较并对设备的twin进行更新（add\delete\update）；将更新过的twin信息写入DB
			//将twin与msgTwin的比较结果，经由community发布到mqtt
			//将syncResult，经由community发布到cloud
			DealDeviceTwin(context, device.ID, baseMessage.EventID, device.Twin, dealType)
		}

		if device.Attributes != nil {
			klog.Infof("Add device attr during first adding device %s", device.ID)
			//更新device在DB中的属性。写入context中；若写入失败，则发送到communityWorker中，转发到Edge
			DeviceUpdated(context, device.ID, device.Attributes, baseMessage, dealType)
		}
		topic := dtcommon.MemETPrefix + context.NodeName + dtcommon.MemETUpdateSuffix
		baseMessage := dttype.BuildBaseMessage()
		addedDevices := make([]dttype.Device, 0)
		addedDevices = append(addedDevices, device)
		addedResult := dttype.MembershipUpdate{BaseMessage: baseMessage, AddDevices: addedDevices}
		//将MembershipUpdate中Metadata.Type为delete的device信息设为nil
		//将上述信息构建一个payload
		result, err := dttype.MarshalMembershipUpdate(addedResult)
		if err != nil {

		} else {
			//将device的update信息经由communityworker，发送到edge的busgroup模块，对topic进行publish操作
			context.Send("",
				dtcommon.SendToEdge,
				dtcommon.CommModule,
				context.BuildModelMessage(modules.BusGroup, "", topic, "publish", result))
		}
		if delta {
			context.Unlock(device.ID)
		}
	}
}

// Removed remove device from the edge group
//从DB中依据DeviceID移除Device、DeviceAttr、DeviceTwin
//从context的DeviceList和DeviceMutex中移除DeviceID
//将remove消息经由community发布到mqtt
func Removed(context *dtcontext.DTContext, toRemove []dttype.Device, baseMessage dttype.BaseMessage, delta bool) {
	klog.Infof("Begin to remove devices")
	if !delta {
		baseMessage.EventID = ""
	}
	for _, device := range toRemove {
		//update sqlite
		_, deviceExist := context.GetDevice(device.ID)
		if !deviceExist {
			klog.Errorf("Remove device %s failed, not existed", device.ID)
			continue
		}
		if delta {
			context.Lock(device.ID)
		}
		deletes := make([]string, 0)
		deletes = append(deletes, device.ID)
		for i := 1; i <= dtcommon.RetryTimes; i++ {
			//从DB中依据DeviceID移除Device、DeviceAttr、DeviceTwin
			err := dtclient.DeleteDeviceTrans(deletes)
			if err != nil {
				klog.Errorf("Delete document of device %s failed at %d time, err: %#v", device.ID, i, err)
			} else {
				klog.Infof("Delete document of device %s successful", device.ID)
				break
			}
			time.Sleep(dtcommon.RetryInterval)
		}
		//todo
		//从DeviceList和DeviceMutex中移除DeviceID
		context.DeviceList.Delete(device.ID)
		context.DeviceMutex.Delete(device.ID)
		if delta {
			context.Unlock(device.ID)
		}
		//"$hw/events/node/"+context.NodeName+"/membership/updated"
		topic := dtcommon.MemETPrefix + context.NodeName + dtcommon.MemETUpdateSuffix
		baseMessage := dttype.BuildBaseMessage()
		removeDevices := make([]dttype.Device, 0)
		removeDevices = append(removeDevices, device)
		//构建结构体//{EventID，Timestamp，AddDevices，RemoveDevices}
		deleteResult := dttype.MembershipUpdate{BaseMessage: baseMessage, RemoveDevices: removeDevices}
		//构建payload
		result, err := dttype.MarshalMembershipUpdate(deleteResult)
		if err != nil {

		} else {
			//将device的remove消息经由community，发布到busgroup，对
			//"$hw/events/node/"+context.NodeName+"/membership/updated"
			//的topic执行publish操作
			context.Send("",
				dtcommon.SendToEdge,
				dtcommon.CommModule,
				context.BuildModelMessage(modules.BusGroup, "", topic, "publish", result))
		}

		klog.Infof("Remove device %s successful", device.ID)
	}
}

// DealGetMembership deal get membership event
func DealGetMembership(context *dtcontext.DTContext, payload []byte) error {
	klog.Info("Deal getting membership event")
	result := []byte("")
	//将event msg由json格式转为BaseMessage{eventId，timestamp}
	edgeGet, err := dttype.UnmarshalBaseMessage(payload)
	para := dttype.Parameter{}
	now := time.Now().UnixNano() / 1e6 //获取当前时间
	if err != nil {                    //若转化格式失败，则报异常
		klog.Errorf("Unmarshal get membership info %s failed , err: %#v", string(payload), err)
		para.Code = dtcommon.BadRequestCode
		para.Reason = fmt.Sprintf("Unmarshal get membership info %s failed , err: %#v", string(payload), err)
		var jsonErr error
		result, jsonErr = dttype.BuildErrorResult(para)
		if jsonErr != nil {
			klog.Errorf("Unmarshal error result error, err: %v", jsonErr)
		}
	} else { //若转化格式成功
		para.EventID = edgeGet.EventID //获取EventID
		var devices []*dttype.Device   //遍历DeviceList，将数据转化为Device，写入Device数组
		context.DeviceList.Range(func(key interface{}, value interface{}) bool {
			deviceModel, ok := value.(*dttype.Device)
			if !ok {

			} else {
				devices = append(devices, deviceModel)
			}
			return true
		})
		//将event的ID与deviceList转化格式为json
		payload, err := dttype.BuildMembershipGetResult(dttype.BaseMessage{EventID: edgeGet.EventID, Timestamp: now}, devices)
		if err != nil {
			klog.Errorf("Marshal membership failed while deal get membership ,err: %#v", err)
		} else {
			result = payload //将json赋值给result（字节数组）
		}

	}
	//组合为一个topic
	topic := dtcommon.MemETPrefix + context.NodeName + dtcommon.MemETGetResultSuffix
	klog.Infof("Deal getting membership successful and send the result")
	//将payload包装为Message，发送到Edge、CommunicateModule组件中
	context.Send("",
		dtcommon.SendToEdge,
		dtcommon.CommModule,
		context.BuildModelMessage(modules.BusGroup, "", topic, "publish", result))

	return nil

}

//SyncDeviceFromSqlite sync device from sqlite
//从sqlite中同步device信息到Context.DeviceList中
func SyncDeviceFromSqlite(context *dtcontext.DTContext, deviceID string) error {
	klog.Infof("Sync device detail info from DB of device %s", deviceID)
	_, exist := context.GetDevice(deviceID)
	if !exist { //检查Device是否存在于DeviceList中
		var deviceMutex sync.Mutex //将deviceID写入DeviceMutex的Map中
		context.DeviceMutex.Store(deviceID, &deviceMutex)
	}
	//从DB中查询deviceID，获取Device信息
	devices, err := dtclient.QueryDevice("id", deviceID)
	if err != nil {
		klog.Errorf("query device attr failed: %v", err)
		return err
	}
	if len(*devices) == 0 {
		return errors.New("Not found device")
	}
	dbDoc := (*devices)[0]
	//从DB中查询deviceID，获取DeviceAttr信息
	deviceAttr, err := dtclient.QueryDeviceAttr("deviceid", deviceID)
	if err != nil {
		klog.Errorf("query device attr failed: %v", err)
		return err
	}
	//从DB中查询deviceID，获取DeviceTwin信息
	deviceTwin, err := dtclient.QueryDeviceTwin("deviceid", deviceID)
	if err != nil {
		klog.Errorf("query device twin failed: %v", err)
		return err
	}
	//将信息存储到DeviceList中
	context.DeviceList.Store(deviceID, &dttype.Device{
		ID:          deviceID,
		Name:        dbDoc.Name,
		Description: dbDoc.Description,
		State:       dbDoc.State,
		LastOnline:  dbDoc.LastOnline,
		Attributes:  dttype.DeviceAttrToMsgAttr(*deviceAttr),
		Twin:        dttype.DeviceTwinToMsgTwin(*deviceTwin)})

	return nil
}
