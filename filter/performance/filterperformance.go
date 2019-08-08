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
	HandleLine int     `json:"handleLine"` // 总处理日志行数
	Tps        float64 `json:"tps"`        // 系统吞出量
	RunTime    string  `json:"runTime"`    // 运行总时间
	ErrNum     int     `json:"errNum"`     // 错误数
	startTime  time.Time
	tpsSli     []int
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
	var f FilterConfig
	f.startTime = time.Now()
	conf := DefaultFilterConfig()
	if err := config.ReflectConfig(raw, &conf); err != nil {
		return nil, err
	}

	ticker := time.NewTicker(time.Second)
	go func() {
		for {
			<-ticker.C
			f.tpsSli = append(f.tpsSli, f.HandleLine)
			if len(f.tpsSli) > 2 {
				f.tpsSli = f.tpsSli[1:]
			}
		}
	}()

	go StartSetver(&f)
	goglog.Logger.Info("性能监控 started...")
	return &conf, nil
}

// Event the main filter event
func (f *FilterConfig) Event(ctx context.Context, event logevent.LogEvent) (logevent.LogEvent, bool) {
	HandleLine += 1
	return event, true
}

func StartSetver(f *FilterConfig) {
	http.HandleFunc("/monitor", func(writer http.ResponseWriter, request *http.Request) {
		f.RunTime = time.Now().Sub(f.startTime).String()
		f.HandleLine = HandleLine
		if len(f.tpsSli) >= 2 {
			f.Tps = float64(f.tpsSli[1]-f.tpsSli[0]) / 5
		}
		ret, _ := json.MarshalIndent(f.SystemInfo, "", "\t")
		io.WriteString(writer, string(ret))
	})
	http.ListenAndServe(":9193", nil)
}
