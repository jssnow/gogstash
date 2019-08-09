package filtersplit

import (
	"context"
	"strings"

	"github.com/tsaikd/gogstash/config"
	"github.com/tsaikd/gogstash/config/goglog"
	"github.com/tsaikd/gogstash/config/logevent"
)

// ModuleName is the name used in config file
const ModuleName = "split"

// ErrorTag tag added to event when process module failed
const ErrorTag = "gogstash_filter_split_error"

// FilterConfig holds the configuration json fields and internal objects
type FilterConfig struct {
	config.FilterConfig
	Source string `json:"source"` // source message field name
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
	err := config.ReflectConfig(raw, &conf)
	if err != nil {
		return nil, err
	}

	return &conf, nil
}

// Event the main filter event
func (f *FilterConfig) Event(ctx context.Context, event logevent.LogEvent) (logevent.LogEvent, bool) {
	message := event.GetString(f.Source)
	found := false

	values := strings.Split(message, "|")
	if len(values) > 0 {
		found = true
		for key, value := range values {
			//去除空格
			value = strings.Trim(value, " ")
			switch key {
			case 0:
				event.SetValue("datetime", value)
			case 1:
				event.SetValue("level", value)
			case 2:
				event.SetValue("host_name", value)
			case 3:
				event.SetValue("module_name", value)
			case 4:
				event.SetValue("classify", value)
			case 5:
				event.SetValue("content", value)
			default:
			}
		}
	}
	if !found {
		event.AddTag(ErrorTag)
		goglog.Logger.Debugf("日志格式错误:%q", message)
		return event, false
	}

	return event, true
}
