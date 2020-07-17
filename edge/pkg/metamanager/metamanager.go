package metamanager

import (
	"time"

	"github.com/astaxie/beego/orm"
	"k8s.io/klog"

	"github.com/kubeedge/beehive/pkg/core"
	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/edge/pkg/common/modules"
	metamanagerconfig "github.com/kubeedge/kubeedge/edge/pkg/metamanager/config"
	"github.com/kubeedge/kubeedge/edge/pkg/metamanager/dao"
	"github.com/kubeedge/kubeedge/pkg/apis/componentconfig/edgecore/v1alpha1"
)

//constant metamanager module name
const (
	MetaManagerModuleName = "metaManager"
)

type metaManager struct {
	enable bool
}

func newMetaManager(enable bool) *metaManager {
	return &metaManager{enable: enable}
}

// Register register metamanager
func Register(metaManager *v1alpha1.MetaManager) {
	metamanagerconfig.InitConfigure(metaManager)
	meta := newMetaManager(metaManager.Enable)
	initDBTable(meta)
	core.Register(meta)
}

// initDBTable create table
func initDBTable(module core.Module) {
	klog.Infof("Begin to register %v db model", module.Name())
	if !module.Enable() {
		klog.Infof("Module %s is disabled, DB meta for it will not be registered", module.Name())
		return
	}
	orm.RegisterModel(new(dao.Meta))
}

func (*metaManager) Name() string {
	return MetaManagerModuleName
}

func (*metaManager) Group() string {
	return modules.MetaGroup
}

func (m *metaManager) Enable() bool {
	return m.enable
}

func (m *metaManager) Start() {
	go func() { //启动新协程，执行定时任务，定时发送msg到metaManager模块的队列中
		period := getSyncInterval()    //从配置文件中获取一个Pod状态同步周期period
		timer := time.NewTimer(period) //利用同步周期period创建一个timer（本质上是个channel），
		// 该timer在period秒之后会有时间数据进入
		for {
			select {
			case <-beehiveContext.Done(): //检查beehiveContext是否正常工作
				klog.Warning("MetaManager stop")
				return
			case <-timer.C:
				//以当前时间为基准，将下一次新的时间数据进入的时间，设置为当前时间的period秒之后。
				//即，过period秒，会有时间数据进入该timer
				timer.Reset(period)
				//Reset配合for、阻塞的select形成了一个无限循环的定时器（若beehive正常），定时触发动作
				msg := model.NewMessage("").BuildRouter(MetaManagerModuleName, GroupResource, model.ResourceTypePodStatus, OperationMetaSync)
				beehiveContext.Send(MetaManagerModuleName, *msg)
				//生成一个固定内容的msg，发布到metaManager模块的队列中
			}
		}
	}()
	m.runMetaManager()
}

func getSyncInterval() time.Duration { //从配置文件中获取一个Pod状态同步周期
	return time.Duration(metamanagerconfig.Config.PodStatusSyncInterval) * time.Second
}
