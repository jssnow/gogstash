package filteraccesscount

import (
	"context"
	"github.com/go-xorm/xorm"
	"github.com/tsaikd/gogstash/config"
	"github.com/tsaikd/gogstash/config/goglog"
	"github.com/tsaikd/gogstash/config/logevent"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ModuleName is the name used in config file
const ModuleName = "access_count"

// FilterConfig holds the configuration json fields and internal objects
type FilterConfig struct {
	config.FilterConfig
	Source         string `json:"source"` // source message field name
	Format         string `json:"format"` // nginx log format
	parser         *regexp.Regexp
	urlCount       map[string]int
	urlTimeAverage map[string]float64
	urlTimeMax     map[string]float64
	urlTimeMin     map[string]float64
	urlStartTime   map[string]string
	urlEndTIme     map[string]string
	DbUsername     string `json:"dbUsername"`
	DbPassword     string `json:"dbPassword"`
	DbAddr         string `json:"dbAddr"`
	DbName         string `json:"dbName"`
}

type LogAppAccess struct {
	Id            int
	AppId         int
	AppName       string
	Url           string
	AccessCount   int
	AccessAvgTime float64
	AccessMinTime float64
	AccessMaxTime float64
}

// DefaultFilterConfig returns an FilterConfig struct with default values
func DefaultFilterConfig() *FilterConfig {
	return &FilterConfig{
		FilterConfig: config.FilterConfig{
			CommonConfig: config.CommonConfig{
				Type: ModuleName,
			},
		},
		urlCount:       make(map[string]int),
		urlTimeAverage: make(map[string]float64),
		urlTimeMax:     make(map[string]float64),
		urlTimeMin:     make(map[string]float64),
		urlStartTime:   make(map[string]string),
		urlEndTIme:     make(map[string]string),
	}
}

// InitHandler initialize the filter plugin
func InitHandler(ctx context.Context, raw *config.ConfigRaw) (config.TypeFilterConfig, error) {
	conf := DefaultFilterConfig()
	err := config.ReflectConfig(raw, &conf)
	if err != nil {
		return nil, err
	}
	//连接数据库
	engine, err := xorm.NewEngine("mysql", conf.DbUsername+":"+conf.DbPassword+"@tcp("+conf.DbAddr+")/"+conf.DbName+"?charset=utf8")
	if err != nil {
		goglog.Logger.Error("分析结果持久化存储数据库连接失败")
	}

	err = engine.Ping()
	if err != nil {
		goglog.Logger.Error(err)
	}

	ticker := time.NewTicker(time.Second * 60)
	i := 1
	go func() {
		for {
			<-ticker.C
			goglog.Logger.Info(i)
			i++
			//持久化分析结果
			writeToMysql(conf, engine)
			//删除已有的数据重新统计
			//deleteMap(conf.urlStartTime)
			//deleteMap(conf.urlEndTIme)
			//deleteStringIntMap(conf.urlCount)
			//deleteStringFloat64Map(conf.urlTimeMax)
			//deleteStringFloat64Map(conf.urlTimeMin)
			//deleteStringFloat64Map(conf.urlTimeAverage)
		}
	}()
	conf.parser = regexp.MustCompile(conf.Format)
	return conf, nil
}

// Event the main filter event
func (f *FilterConfig) Event(ctx context.Context, event logevent.LogEvent) (logevent.LogEvent, bool) {
	message := event.GetString(f.Source)
	ret := f.parser.FindStringSubmatch(message)
	if len(ret) < 12 {
		//格式错误或者不是指定的nginx日志文件格式
		return event, false
	}
	reqSli := strings.Split(ret[5], " ")
	if len(reqSli) != 3 {
		//丢弃日志
		event.AddTag(config.DropTag)
		return event, false
	}

	//api路径
	u, err := url.Parse(reqSli[1])
	if err != nil {
		//丢弃日志
		event.AddTag(config.DropTag)
		return event, false
	}
	//路径
	urlPath := u.Path

	//请求时间
	requestTime, _ := strconv.ParseFloat(ret[12], 64)
	//记录接口请求的数量
	f.urlCount[urlPath] += 1
	//记录接口的所求请求时间
	f.urlTimeAverage[urlPath] += requestTime
	//记录接口的最大和最小的请求时间
	if requestTime > f.urlTimeMax[urlPath] {
		f.urlTimeMax[urlPath] = requestTime
	}
	if f.urlTimeMin[urlPath] == 0 {
		f.urlTimeMin[urlPath] = requestTime
	} else {
		if requestTime < f.urlTimeMin[urlPath] {
			f.urlTimeMin[urlPath] = requestTime
		}
	}

	//丢弃日志
	event.AddTag(config.DropTag)
	return event, false
}

//分析结果定时持久化
func writeToMysql(f *FilterConfig, engine xorm.EngineInterface) {
	appAccess := new(LogAppAccess)
	for url, count := range f.urlCount {

		urlTime := f.urlTimeAverage[url]
		urlTimeMin := f.urlTimeMin[url] * 1000
		urlTimeMax := f.urlTimeMax[url] * 1000
		if urlTime > 0 {
			//计算平均时间
			avgTime := (urlTime / float64(count)) * 1000
			appAccess.AppId = 1
			appAccess.AppName = "用户中心"
			appAccess.Url = url
			appAccess.AccessCount = count
			appAccess.AccessAvgTime = avgTime
			appAccess.AccessMinTime = urlTimeMin
			appAccess.AccessMaxTime = urlTimeMax
			//appAccess.UrlStartTime = f.urlStartTime[url]
			//appAccess.UrlEndTime = f.urlEndTIme[url]
			_, err := engine.Insert(appAccess)
			if err != nil {
				goglog.Logger.Errorf("nginx访问日志分析结果写入失败,%s", err)
			}
			goglog.Logger.Info("nginx访问日志分析结果写入成功")
		}
	}
}

func deleteMap(mapData map[string]string) {
	for k := range mapData {
		delete(mapData, k) //删除整个字典的数据
	}
}

func deleteStringIntMap(mapData map[string]int) {
	for k := range mapData {
		delete(mapData, k) //删除整个字典的数据
	}
}

func deleteStringFloat64Map(mapData map[string]float64) {
	for k := range mapData {
		delete(mapData, k) //删除整个字典的数据
	}
}
