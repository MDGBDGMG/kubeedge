package servicebus

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"k8s.io/klog"

	"github.com/kubeedge/beehive/pkg/core"
	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/edge/pkg/common/modules"
	"github.com/kubeedge/kubeedge/edge/pkg/servicebus/util"
	"github.com/kubeedge/kubeedge/pkg/apis/componentconfig/edgecore/v1alpha1"
)

const (
	sourceType  = "router_rest"
	maxBodySize = 5 * 1e6
)

// servicebus struct
type servicebus struct {
	enable bool
}

func newServicebus(enable bool) *servicebus {
	return &servicebus{
		enable: enable,
	}
}

// Register register servicebus
func Register(s *v1alpha1.ServiceBus) {
	core.Register(newServicebus(s.Enable))
}

func (*servicebus) Name() string {
	return "servicebus"
}

func (*servicebus) Group() string {
	return modules.BusGroup
}

func (sb *servicebus) Enable() bool {
	return sb.enable
}

//创建urlclient，将servicebus的channel中的msg，按照msg中的地址和方法，发送http请求
//并将得到的response包装后发布给hubgroup组
func (sb *servicebus) Start() {
	// no need to call TopicInit now, we have fixed topic
	var htc = new(http.Client)     //创建HttpClient
	htc.Timeout = time.Second * 10 //设置HttpClent的超时时间

	var uc = new(util.URLClient) //创建URLClient
	uc.Client = htc              //将HttpClient赋值给URLClient的Client属性

	//Get message from channel
	for {
		select {
		case <-beehiveContext.Done(): //检查beehiveContext是否失效
			klog.Warning("ServiceBus stop")
			return
		default:

		}
		msg, err := beehiveContext.Receive("servicebus") //beehive从servicebus模块的channel获取消息
		if err != nil {
			klog.Warningf("servicebus receive msg error %v", err)
			continue
		}
		go func() {
			klog.Infof("ServiceBus receive msg")
			source := msg.GetSource()
			if source != sourceType {
				return
			}
			resource := msg.GetResource()
			r := strings.Split(resource, ":")
			if len(r) != 2 {
				m := "the format of resource " + resource + " is incorrect"
				klog.Warningf(m)
				code := http.StatusBadRequest //code=400
				if response, err := buildErrorResponse(msg.GetID(), m, code); err == nil {
					beehiveContext.SendToGroup(modules.HubGroup, response) //将错误信息发布到HubGroup组
				}
				return
			}
			content, err := json.Marshal(msg.GetContent()) //将msg的内容转化为json格式
			if err != nil {
				klog.Errorf("marshall message content failed %v", err)
				m := "error to marshal request msg content"
				code := http.StatusBadRequest //code=400
				if response, err := buildErrorResponse(msg.GetID(), m, code); err == nil {
					beehiveContext.SendToGroup(modules.HubGroup, response) //将错误信息发布到HubGroup组
				}
				return
			}
			var httpRequest util.HTTPRequest                              //创建HTTPRequest
			if err := json.Unmarshal(content, &httpRequest); err != nil { //将content解码，并写入到httpRequest中
				m := "error to parse http request"
				code := http.StatusBadRequest
				klog.Errorf(m, err)
				if response, err := buildErrorResponse(msg.GetID(), m, code); err == nil {
					beehiveContext.SendToGroup(modules.HubGroup, response)
				}
				return
			}
			operation := msg.GetOperation()
			targetURL := "http://127.0.0.1:" + r[0] + "/" + r[1]
			resp, err := uc.HTTPDo(operation, targetURL, httpRequest.Header, httpRequest.Body) //发出http请求并拿到resp
			if err != nil {
				m := "error to call service"
				code := http.StatusNotFound
				klog.Errorf(m, err)
				if response, err := buildErrorResponse(msg.GetID(), m, code); err == nil {
					beehiveContext.SendToGroup(modules.HubGroup, response)
				}
				return
			}
			resp.Body = http.MaxBytesReader(nil, resp.Body, maxBodySize) //最大读取5*1e6个字节，返回一个Reader
			resBody, err := ioutil.ReadAll(resp.Body)                    //从Reader中读取所有的字节
			if err != nil {
				if err.Error() == "http: request body too large" {
					err = fmt.Errorf("response body too large")
				}
				m := "error to receive response, err: " + err.Error()
				code := http.StatusInternalServerError
				klog.Errorf(m, err)
				if response, err := buildErrorResponse(msg.GetID(), m, code); err == nil {
					beehiveContext.SendToGroup(modules.HubGroup, response)
				}
				return
			}
			//使用上述resp构建一个response
			response := util.HTTPResponse{Header: resp.Header, StatusCode: resp.StatusCode, Body: resBody}
			responseMsg := model.NewMessage(msg.GetID())
			responseMsg.Content = response
			responseMsg.SetRoute("servicebus", modules.UserGroup)      //配置msg的Source属性和Group属性
			beehiveContext.SendToGroup(modules.HubGroup, *responseMsg) //将msg发布给HUBGROUP
		}()
	}
}

//若出现异常，将异常信息写入该函数，将返回值发布到hubgroup的channel中
func buildErrorResponse(parentID string, content string, statusCode int) (model.Message, error) {
	responseMsg := model.NewMessage(parentID)
	h := http.Header{}
	h.Add("Server", "kubeedge-edgecore")
	c := util.HTTPResponse{Header: h, StatusCode: statusCode, Body: []byte(content)}
	responseMsg.Content = c
	responseMsg.SetRoute("servicebus", modules.UserGroup)
	return *responseMsg, nil
}
