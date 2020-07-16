package edgehub

import (
	"crypto/tls"
	"sync"
	"time"

	"k8s.io/klog"

	"github.com/kubeedge/beehive/pkg/core"
	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/edge/pkg/common/modules"
	"github.com/kubeedge/kubeedge/edge/pkg/edgehub/clients"
	"github.com/kubeedge/kubeedge/edge/pkg/edgehub/config"
	"github.com/kubeedge/kubeedge/pkg/apis/componentconfig/edgecore/v1alpha1"
	"github.com/kubeedge/kubeedge/pkg/util/validation"
)

//define edgehub module name
const (
	ModuleNameEdgeHub = "websocket"
)

var HasTLSTunnelCerts = make(chan bool, 1)

//EdgeHub defines edgehub object structure
type EdgeHub struct {
	chClient      clients.Adapter               //向cloudhub发送消息用的client(websocket client)
	reconnectChan chan struct{}                 //keepalive、route cloudhub、route edgehub中若链接断开，则此chan中新增一个空值
	syncKeeper    map[string]chan model.Message //同步的保持器（MAP）
	keeperLock    sync.RWMutex                  //保持器的锁（读写互斥锁）
	enable        bool                          //edgehub是否可用
}

func newEdgeHub(enable bool) *EdgeHub {
	return &EdgeHub{
		reconnectChan: make(chan struct{}),
		syncKeeper:    make(map[string]chan model.Message),
		enable:        enable,
	}
}

// Register register edgehub
func Register(eh *v1alpha1.EdgeHub, nodeName string) {
	config.InitConfigure(eh, nodeName)   //将edgehub的信息写入config中
	core.Register(newEdgeHub(eh.Enable)) //新建EdgeHub，并写入Stageing中的module的map中，key为edgehub的name，即websocket
}

//Name returns the name of EdgeHub module
func (eh *EdgeHub) Name() string {
	return ModuleNameEdgeHub //常量 websocket
}

//Group returns EdgeHub group
func (eh *EdgeHub) Group() string {
	return modules.HubGroup //常量 hub
}

//Enable indicates whether this module is enabled
func (eh *EdgeHub) Enable() bool {
	return eh.enable //定义是否该模块可用
}

//Start sets context and starts the controller
func (eh *EdgeHub) Start() {
	//认证
	// if there is no manual certificate setting or the setting has problems, then the edge applies for the certificate
	if validation.FileIsExist(config.Config.TLSCAFile) && validation.FileIsExist(config.Config.TLSCertFile) && validation.FileIsExist(config.Config.TLSPrivateKeyFile) {
		_, err := tls.LoadX509KeyPair(config.Config.TLSCertFile, config.Config.TLSPrivateKeyFile)
		if err != nil {
			if err := eh.applyCerts(); err != nil {
				klog.Fatalf("Error: %v", err)
				return
			}
		}
	} else {
		if err := eh.applyCerts(); err != nil {
			klog.Fatalf("Error: %v", err)
			return
		}
	}
	HasTLSTunnelCerts <- true
	close(HasTLSTunnelCerts)

	//开启一个无限循环
	//若beehiveContext结束，则重新循环
	//若<route到edgehub> 或 <route到cloudhub>  或 <edgehub到cloudhub> 出现连接问题，则重新循环
	for {
		select {
		case <-beehiveContext.Done():
			klog.Warning("EdgeHub stop")
			return
		default:

		}
		err := eh.initial() //获取一个cloudhub的websocket client，赋值给edgehub的chclient字段
		if err != nil {
			klog.Fatalf("failed to init controller: %v", err)
			return
		}
		err = eh.chClient.Init() //初始化chClient
		if err != nil {
			klog.Errorf("connection error, try again after 60s: %v", err)
			time.Sleep(waitConnectionPeriod)
			continue
		}
		// execute hook func after connect
		eh.pubConnectInfo(true) //向resource、twin、user、func等group发布连接 可用 信息
		//启动线程，edgehub的websocket从cloudhub接收消息，发布给message里的group；若发送失败，则向reconnectChan中新增数据
		go eh.routeToEdge()
		//启动线程，beehive从ModuleNameEdgeHub的channel中获取消息，由chclient将消息发送给cloud，并校验消息是否送达；
		//若发送失败，则向reconnectChan中新增数据
		go eh.routeToCloud()
		//启动线程，每隔15s向cloud发送消息，检查是否与cloud保持连接状态；若连接断开，则向reconnectChan中新增数据
		go eh.keepalive()

		// wait the stop singal
		// stop authinfo manager/websocket connection
		<-eh.reconnectChan   //若重连channel中新增了数据
		eh.chClient.Uninit() //那么将cloudhub的websocket关闭

		// execute hook fun after disconnect
		eh.pubConnectInfo(false) ////向resource、twin、user、func等group发布连接 不可用 信息

		// sleep one period of heartbeat, then try to connect cloud hub again
		time.Sleep(time.Duration(config.Config.Heartbeat) * time.Second * 2) //沉睡 15*2 秒

		// clean channel
	clean:
		for {
			select {
			case <-eh.reconnectChan: //等待重连channel中无数据
			default:
				break clean
			}
		}
	}
}
