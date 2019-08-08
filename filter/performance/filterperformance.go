package filterperformance

import (
	"context"
	"encoding/json"
	"github.com/tsaikd/gogstash/config/goglog"
	"io"
	"net/http"
	"time"

	"github.com/tsaikd/gogstash/config"
	"github.com/tsaikd/gogstash/config/logevent"
)

// ModuleName is the name used in config file
const ModuleName = "performance"

var HandleLine int

// FilterConfig holds the configuration json fields and internal objects
type FilterConfig struct {
	config.FilterConfig
	SystemInfo
}

type SystemInfo struct {
	HandleLine int     `json:"handleLine"` // 本统计周期内已经处理的日志行数
	Tps        float64 `json:"tps"`        // 系统吞出量
	RunTime    string  `json:"runTime"`    // 运行总时间
	ErrNum     int     `json:"errNum"`     // 错误数
	startTime  time.Time
	//tpsSli     []int
	lastHandleLine int //上一个统计周期内的处理行数
}

// DefaultFilterConfig returns an FilterConfig struct with default values
func DefaultFilterConfig() FilterConfig {
	return FilterConfig{
		FilterConfig: config.FilterConfig{
			CommonConfig: config.CommonConfig{
				Type: ModuleName,
			},
		},
	}
}

// InitHandler initialize the filter plugin
func InitHandler(ctx context.Context, raw *config.ConfigRaw) (config.TypeFilterConfig, error) {
	conf := DefaultFilterConfig()
	if err := config.ReflectConfig(raw, &conf); err != nil {
		return nil, err
	}
	conf.startTime = time.Now()

	ticker := time.NewTicker(time.Second * 10)
	go func() {
		for {
			<-ticker.C
			conf.lastHandleLine = conf.HandleLine
			conf.HandleLine = 0
			//conf.tpsSli = append(conf.tpsSli, conf.HandleLine)
			//if len(conf.tpsSli) > 2 {
			//	conf.tpsSli = conf.tpsSli[1:]
			//}
		}
	}()

	go StartMonitorServer(&conf)
	goglog.Logger.Info("性能监控 started...")
	return &conf, nil
}

// Event the main filter event
func (f *FilterConfig) Event(ctx context.Context, event logevent.LogEvent) (logevent.LogEvent, bool) {
	f.HandleLine += 1
	return event, true
}

//启动获取系统运行情况server
func StartMonitorServer(f *FilterConfig) {
	http.HandleFunc("/monitor", func(writer http.ResponseWriter, request *http.Request) {
		f.RunTime = time.Now().Sub(f.startTime).String()
		f.Tps = float64(f.lastHandleLine) / 10
		//if len(f.tpsSli) >= 2 {
		//	f.Tps = float64(f.tpsSli[1]-f.tpsSli[0]) / 5
		//}
		ret, _ := json.MarshalIndent(f.SystemInfo, "", "\t")
		io.WriteString(writer, string(ret))
	})
	http.ListenAndServe(":9193", nil)
}
