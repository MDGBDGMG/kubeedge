package cloudhub

import (
	"os"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"

	"github.com/kubeedge/beehive/pkg/core"
	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/kubeedge/cloud/pkg/client/clientset/versioned"
	crdinformerfactory "github.com/kubeedge/kubeedge/cloud/pkg/client/informers/externalversions"
	"github.com/kubeedge/kubeedge/cloud/pkg/cloudhub/channelq"
	hubconfig "github.com/kubeedge/kubeedge/cloud/pkg/cloudhub/config"
	"github.com/kubeedge/kubeedge/cloud/pkg/cloudhub/servers"
	"github.com/kubeedge/kubeedge/cloud/pkg/cloudhub/servers/httpserver"
	"github.com/kubeedge/kubeedge/cloud/pkg/cloudhub/servers/udsserver"
	"github.com/kubeedge/kubeedge/pkg/apis/componentconfig/cloudcore/v1alpha1"
)

var DoneTLSTunnelCerts = make(chan bool, 1)

type cloudHub struct {
	enable bool
}

func newCloudHub(enable bool) *cloudHub {
	return &cloudHub{
		enable: enable,
	}
}

func Register(hub *v1alpha1.CloudHub, kubeAPIConfig *v1alpha1.KubeAPIConfig) {
	hubconfig.InitConfigure(hub, kubeAPIConfig)
	core.Register(newCloudHub(hub.Enable))
}

func (a *cloudHub) Name() string {
	return "cloudhub"
}

func (a *cloudHub) Group() string {
	return "cloudhub"
}

// Enable indicates whether enable this module
func (a *cloudHub) Enable() bool {
	return a.enable
}

func (a *cloudHub) Start() {
	//创建同步控制器的对象
	//启动协程开始运行ClusterObjectSyncInformer,本质上启动了k8s的controller
	//启动协程开始运行ObjectSyncInformer,本质上启动了k8s的controller
	objectSyncController := newObjectSyncController()

	if !cache.WaitForCacheSync(beehiveContext.Done(), //等待Cache同步
		objectSyncController.ClusterObjectSyncSynced,
		objectSyncController.ObjectSyncSynced,
	) {
		klog.Errorf("unable to sync caches for objectSyncController")
		os.Exit(1)
	}
	//创建通道msg队列，本质是四个map，存放队列信息
	messageq := channelq.NewChannelMessageQueue(objectSyncController)

	// start dispatch message from the cloud to edge node
	go messageq.DispatchMessage() //将信息从cloud拿出来，按照类型的不同写入queue中，以便发送到edge

	// check whether the certificates exist in the local directory,
	// and then check whether certificates exist in the secret, generate if they don't exist
	if err := httpserver.PrepareAllCerts(); err != nil { //准备所有的Cert
		klog.Fatal(err)
	}
	// TODO: Will improve in the future
	DoneTLSTunnelCerts <- true //DoneTLSTunnelCerts本质上是个chan，再此写入true
	close(DoneTLSTunnelCerts)  //关闭DoneTLSTunnelCerts

	// generate Token
	if err := httpserver.GenerateToken(); err != nil { //生成token，并启动协程每隔12小时更新token
		klog.Fatal(err)
	}

	// HttpServer mainly used to issue certificates for the edge
	go httpserver.StartHTTPServer() //启动一个httpserver，用于edge端的身份认证

	servers.StartCloudHub(messageq) //启动CloudHub

	if hubconfig.Config.UnixSocket.Enable { //若UnixSocket可用，则启动相应的协程
		// The uds server is only used to communicate with csi driver from kubeedge on cloud.
		// It is not used to communicate between cloud and edge.
		go udsserver.StartServer(hubconfig.Config.UnixSocket.Address)
	}
}

//创建同步控制器的对象
func newObjectSyncController() *hubconfig.ObjectSyncController {
	config, err := buildConfig() //从master节点或者本地文件中获取config
	if err != nil {
		klog.Errorf("Failed to build config, err: %v", err)
		os.Exit(1)
	}
	//依据config，创建clientset，clientset是一个client的group
	crdClient := versioned.NewForConfigOrDie(config)
	crdFactory := crdinformerfactory.NewSharedInformerFactory(crdClient, 0)
	//TODO 看不太懂informer原理
	clusterObjectSyncInformer := crdFactory.Reliablesyncs().V1alpha1().ClusterObjectSyncs()
	objectSyncInformer := crdFactory.Reliablesyncs().V1alpha1().ObjectSyncs()

	sc := &hubconfig.ObjectSyncController{ //初始化同步控制器，包含clientset、informer、synced、lister
		CrdClient: crdClient,

		ClusterObjectSyncInformer: clusterObjectSyncInformer,
		ObjectSyncInformer:        objectSyncInformer,

		ClusterObjectSyncSynced: clusterObjectSyncInformer.Informer().HasSynced,
		ObjectSyncSynced:        objectSyncInformer.Informer().HasSynced,

		ClusterObjectSyncLister: clusterObjectSyncInformer.Lister(),
		ObjectSyncLister:        objectSyncInformer.Lister(),
	}
	//启动协程开始运行ClusterObjectSyncInformer
	go sc.ClusterObjectSyncInformer.Informer().Run(beehiveContext.Done())
	//启动协程开始运行ObjectSyncInformer
	go sc.ObjectSyncInformer.Informer().Run(beehiveContext.Done())

	return sc
}

// build Config from flags
func buildConfig() (conf *rest.Config, err error) {
	kubeConfig, err := clientcmd.BuildConfigFromFlags(hubconfig.Config.KubeAPIConfig.Master,
		hubconfig.Config.KubeAPIConfig.KubeConfig) //从master节点或者本地文件中获取config信息
	if err != nil {
		return nil, err
	}
	kubeConfig.QPS = float32(hubconfig.Config.KubeAPIConfig.QPS) //每秒查询速率
	kubeConfig.Burst = int(hubconfig.Config.KubeAPIConfig.Burst) //最大查询速率
	kubeConfig.ContentType = "application/json"

	return kubeConfig, nil
}
