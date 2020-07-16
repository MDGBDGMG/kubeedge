package edgehub

import (
	"crypto/x509"
	"fmt"
	"strings"
	"time"

	"k8s.io/klog"

	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/beehive/pkg/core/model"
	connect "github.com/kubeedge/kubeedge/edge/pkg/common/cloudconnection"
	"github.com/kubeedge/kubeedge/edge/pkg/common/message"
	"github.com/kubeedge/kubeedge/edge/pkg/common/modules"
	"github.com/kubeedge/kubeedge/edge/pkg/edgehub/clients"
	"github.com/kubeedge/kubeedge/edge/pkg/edgehub/common/certutil"
	"github.com/kubeedge/kubeedge/edge/pkg/edgehub/config"
)

const (
	waitConnectionPeriod = time.Minute
	authEventType        = "auth_info_event"
	caURL                = "/ca.crt"
	certURL              = "/edge.crt"
)

var groupMap = map[string]string{
	"resource": modules.MetaGroup,
	"twin":     modules.TwinGroup,
	"func":     modules.MetaGroup,
	"user":     modules.BusGroup,
}

// applyCerts get edge certificate to communicate with cloudcore
func (eh *EdgeHub) applyCerts() error {
	// get ca.crt
	url := config.Config.HTTPServer + caURL
	cacert, err := certutil.GetCACert(url)
	if err != nil {
		return fmt.Errorf("failed to get CA certificate, err: %v", err)
	}

	// validate the CA certificate by hashcode
	tokenParts := strings.Split(config.Config.Token, ".")
	if len(tokenParts) != 4 {
		return fmt.Errorf("token are in the wrong format")
	}
	ok, hash, newHash := certutil.ValidateCACerts(cacert, tokenParts[0])
	if !ok {
		return fmt.Errorf("failed to validate CA certificate. tokenCAhash: %s, CAhash: %s", hash, newHash)
	}
	// save the ca.crt to file
	ca, err := x509.ParseCertificate(cacert)
	if err != nil {
		return fmt.Errorf("failed to parse the CA certificate, error: %v", err)
	}

	if err = certutil.WriteCert(config.Config.TLSCAFile, ca); err != nil {
		return fmt.Errorf("failed to save the CA certificate to local directory: %s, error: %v", config.Config.TLSCAFile, err)
	}

	// get the edge.crt
	url = config.Config.HTTPServer + certURL
	edgecert, err := certutil.GetEdgeCert(url, cacert, strings.Join(tokenParts[1:], "."))
	if err != nil {
		return fmt.Errorf("failed to get edge certificate from the cloudcore, error: %v", err)
	}
	// save the edge.crt to the file
	cert, _ := x509.ParseCertificate(edgecert)
	if err = certutil.WriteCert(config.Config.TLSCertFile, cert); err != nil {
		return fmt.Errorf("failed to save the edge certificate to local directory: %s, error: %v", config.Config.TLSCertFile, err)
	}
	return nil
}

func (eh *EdgeHub) initial() (err error) {

	cloudHubClient, err := clients.GetClient()
	if err != nil {
		return err
	}

	eh.chClient = cloudHubClient

	return nil
}

func (eh *EdgeHub) addKeepChannel(msgID string) chan model.Message {
	eh.keeperLock.Lock()
	defer eh.keeperLock.Unlock()

	tempChannel := make(chan model.Message)
	eh.syncKeeper[msgID] = tempChannel

	return tempChannel
}

func (eh *EdgeHub) deleteKeepChannel(msgID string) {
	eh.keeperLock.Lock()
	defer eh.keeperLock.Unlock()

	delete(eh.syncKeeper, msgID)
}

func (eh *EdgeHub) isSyncResponse(msgID string) bool {
	eh.keeperLock.RLock()
	defer eh.keeperLock.RUnlock()

	_, exist := eh.syncKeeper[msgID]
	return exist
}

//检查keepchannel中是否存在msg的parentid，存在则将msg写入keepchannel中
func (eh *EdgeHub) sendToKeepChannel(message model.Message) error {
	eh.keeperLock.RLock()
	defer eh.keeperLock.RUnlock()
	channel, exist := eh.syncKeeper[message.GetParentID()]
	if !exist {
		klog.Errorf("failed to get sync keeper channel, messageID:%+v", message)
		return fmt.Errorf("failed to get sync keeper channel, messageID:%+v", message)
	}
	// send response into synckeep channel
	select {
	case channel <- message:
	default:
		klog.Errorf("failed to send message to sync keep channel")
		return fmt.Errorf("failed to send message to sync keep channel")
	}
	return nil
}

//将msg发布到msg.group里
func (eh *EdgeHub) dispatch(message model.Message) error {
	// TODO: dispatch message by the message type
	md, ok := groupMap[message.GetGroup()]
	if !ok {
		klog.Warningf("msg_group not found")
		return fmt.Errorf("msg_group not found")
	}

	isResponse := eh.isSyncResponse(message.GetParentID()) //检查parentid是否在syncKeeper的chan里存在
	if !isResponse {
		beehiveContext.SendToGroup(md, message) //使用beeliveContext将message发布给group相应的channel中
		return nil
	}
	return eh.sendToKeepChannel(message) //将message发布到syncKeeper的chan里
}

//edgehub的websocket从cloudhub接收消息，发布给message里的group
func (eh *EdgeHub) routeToEdge() {
	for {
		select {
		case <-beehiveContext.Done():
			klog.Warning("EdgeHub RouteToEdge stop")
			return
		default:

		}
		message, err := eh.chClient.Receive()
		if err != nil {
			klog.Errorf("websocket read error: %v", err)
			eh.reconnectChan <- struct{}{}
			return
		}

		klog.Infof("received msg from cloud-hub:%+v", message)
		//（若parentid在synckeep中）将msg发布到synckeep的channel中
		//（若parentid不在synckeep中）或者使用beehive将消息发送到group的channel中
		err = eh.dispatch(message)
		if err != nil {
			klog.Errorf("failed to dispatch message, discard: %v", err)
		}
	}
}

//chClient将message同步发送给cloud。即发出消息后，等待一段时间接收response，并将response由beehiveContext处理。
func (eh *EdgeHub) sendToCloud(message model.Message) error {
	eh.keeperLock.Lock()
	err := eh.chClient.Send(message)
	eh.keeperLock.Unlock()
	if err != nil {
		klog.Errorf("failed to send message: %v", err)
		return fmt.Errorf("failed to send message, error: %v", err)
	}

	syncKeep := func(message model.Message) {
		tempChannel := eh.addKeepChannel(message.GetID())
		sendTimer := time.NewTimer(time.Duration(config.Config.Heartbeat) * time.Second)
		select {
		case response := <-tempChannel:
			sendTimer.Stop()
			beehiveContext.SendResp(response)
			eh.deleteKeepChannel(response.GetParentID())
		case <-sendTimer.C:
			klog.Warningf("timeout to receive response for message: %+v", message)
			eh.deleteKeepChannel(message.GetID())
		}
	}

	if message.IsSync() {
		go syncKeep(message)
	}

	return nil
}

//beehive从ModuleNameEdgeHub的channel中获取消息，由chclient将消息发送给cloud，并校验消息是否送达
func (eh *EdgeHub) routeToCloud() {
	for {
		select {
		case <-beehiveContext.Done():
			klog.Warning("EdgeHub RouteToCloud stop")
			return
		default:
		}
		message, err := beehiveContext.Receive(ModuleNameEdgeHub)
		if err != nil {
			klog.Errorf("failed to receive message from edge: %v", err)
			time.Sleep(time.Second)
			continue
		}

		// post message to cloud hub
		err = eh.sendToCloud(message)
		if err != nil {
			klog.Errorf("failed to send message to cloud: %v", err)
			eh.reconnectChan <- struct{}{}
			return
		}
	}
}

//每隔15S检查edgehub与cloudhub的连接状态
func (eh *EdgeHub) keepalive() {
	for {
		select {
		case <-beehiveContext.Done():
			klog.Warning("EdgeHub KeepAlive stop")
			return
		default:

		}
		msg := model.NewMessage("").
			BuildRouter(ModuleNameEdgeHub, "resource", "node", "keepalive").
			FillBody("ping")

		// post message to cloud hub
		err := eh.sendToCloud(*msg)
		if err != nil {
			klog.Errorf("websocket write error: %v", err)
			eh.reconnectChan <- struct{}{}
			return
		}

		time.Sleep(time.Duration(config.Config.Heartbeat) * time.Second)
	}
}

//将连接状态发布给其他group中
func (eh *EdgeHub) pubConnectInfo(isConnected bool) {
	// var info model.Message
	content := connect.CloudConnected
	if !isConnected {
		content = connect.CloudDisconnected
	}

	for _, group := range groupMap {
		message := model.NewMessage("").BuildRouter(message.SourceNodeConnection, group,
			message.ResourceTypeNodeConnection, message.OperationNodeConnection).FillBody(content)
		beehiveContext.SendToGroup(group, *message)
	}
}
