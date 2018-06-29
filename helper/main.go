package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"github.com/olivere/elastic"
)

type userRemain struct {
	Id        int64 `json:"id" xorm:"not null INT(11) pk autoincr 'id'"`
	Uid       int64 `json:"uid"  xorm:"not null INT(11) 'uid'"`
	RegDay    int   `json:"data_day"  xorm:"not null INT(11) 'reg_day'"`
	SecondDay int   `json:"second_day"  xorm:"not null INT(11) 'second_day'"`
	SevenDay  int   `json:"seven_day"  xorm:"not null INT(11) 'seven_day'"`
	ThirtyDay int   `json:"thirty_day"  xorm:"not null INT(11) 'thirty_day'"`
}

func (userRemain) TableName() string {
	return "vpn_user_remain"
}
func (userRemain) GetConnect() *xorm.Engine {
	//var once sync.Once
	engine, err := xorm.NewEngine("mysql", "webserver:FrZi+rVO/ZU=@tcp(120.77.244.69:3306)/crontab?charset=utf8")
	if err != nil {
		fmt.Println(err)
	}

	//日志打印SQL
	engine.ShowSQL(true)
	return engine
}

type elkUser struct {
	Uid     int64  `json:"uid"`
	DateDay string `json:"data_day"`
}
type Basemodel struct {
	Id int64 `xorm:"not null pk autoincr INT(11)"`
}

func (Basemodel) GetConnect() *xorm.Engine {
	//var once sync.Once
	engine, err := xorm.NewEngine("mysql", "webserver:FrZi+rVO/ZU=@tcp(120.77.244.69:3306)/jiguang?charset=utf8")
	if err != nil {
		fmt.Println(err)
	}

	//日志打印SQL
	engine.ShowSQL(true)
	return engine
}

type Account struct {
	Basemodel `xorm:"extends"`
	RegTime   int64 `form:"reg_time" xorm:"not null INT(11)"`
}

func (Account) TableName() string {
	return "vpn_account"
}

type Pool struct {
	Queue         chan func() error
	RuntineNumber int
	Total         int

	Result         chan error
	FinishCallback func()
}

//初始化
func (self *Pool) Init(runtineNumber int, total int) {
	self.RuntineNumber = runtineNumber
	self.Total = total
	self.Queue = make(chan func() error, total)
	self.Result = make(chan error, total)
}

func (self *Pool) Start() {
	//开启 number 个goruntine
	for i := 0; i < self.RuntineNumber; i++ {
		go func() {
			for {
				task, ok := <-self.Queue
				if !ok {
					break
				}
				err := task()
				self.Result <- err
			}
		}()
	}

	//获取每个任务的处理结果
	for j := 0; j < self.RuntineNumber; j++ {
		res, ok := <-self.Result
		if !ok {
			break
		}
		if res != nil {
			fmt.Println(res)
		}
	}

	//结束回调函数
	if self.FinishCallback != nil {
		self.FinishCallback()
	}
}

//关闭
func (self *Pool) Stop() {
	close(self.Queue)
	close(self.Result)
}

func (self *Pool) AddTask(task func() error) {
	self.Queue <- task
}

func (self *Pool) SetFinishCallback(fun func()) {
	self.FinishCallback = fun
}

func main() {

	opType := flag.String("type", "second", "op type")
	dateFlag := flag.String("date", "", "op date") //time.Now().Local().Format("20060102")
	flag.Parse()
	if *opType == "second" {
		if len(*dateFlag) <= 0 {

			var p Pool
			p.Init(9, 55)

			t, _ := time.Parse("20060102", "20180504")
			timeToday := time.Now().Unix() - 8*3600
			calTime := t.Unix()
			for calTime < (timeToday - 86400) {
				dateStr := time.Unix(calTime, 0).Format("20060102")
				p.AddTask(func() error {
					return second(&dateStr)
				})
				calTime = calTime + 86400
			}
			p.SetFinishCallback(DownloadFinish)
			p.Start()
			p.Stop()
		} else {
			second(dateFlag)
		}
	}

}
func DownloadFinish() {
	fmt.Println("Download finsh")
}
func second(dateAddress *string) error {
	t, _ := time.Parse("20060102", *dateAddress)
	startTimeStamp := t.Unix() - 8*3600
	endTimeStamp := t.Unix() - 8*3600 + 24*3600 - 1
	startTm := time.Unix(startTimeStamp+86400, 0)
	startDate := startTm.Format("20060102")
	endTm := time.Unix(startTimeStamp+86400, 0)
	endDate := endTm.Format("20060102")
	var accountModel Account
	conn := accountModel.GetConnect()
	page := 1
	for page > 0 {
		perPage := 10000
		startLimit := (page - 1) * perPage
		accounts := make([]Account, 0)
		conn.Where(" reg_time between ? and ? ", startTimeStamp, endTimeStamp).Limit(perPage, startLimit).Find(&accounts)

		for _, accountInfo := range accounts {
			secondRemain(accountInfo, startDate, endDate)
		}
		if len(accounts) > 0 {
			page++
		} else {
			page = 0
		}
	}
	return nil
}

func secondRemain(accountInfo Account, startDate string, endDate string) {
	//fmt.Println(accountInfo.Id)
	client, err := elastic.NewSimpleClient(elastic.SetURL("http://120.78.168.40:9200"), elastic.SetBasicAuth("vpn", "vpnkibana2017"), elastic.SetScheme("http"))
	if err != nil {
		// Handle error
		panic(err)
	}

	ctx := context.Background()
	// 查询elk数据 是否在范围内登录过
	// Search with a term query
	rangeQuery := elastic.NewRangeQuery("data_day").Gte(startDate).Lte(endDate)
	matchQuey := elastic.NewMatchQuery("uid", accountInfo.Id)
	mustQuery := elastic.NewBoolQuery().Must(rangeQuery, matchQuey)
	searchResult, err := client.Search("jg_vpn_activity_log-*").Type("vpn_activity").
		//From(0).Size(10). // take documents 0-9
		Query(mustQuery).
		Pretty(true). // pretty print request and response JSON
		Do(ctx)       // execute
	var userRemainModel userRemain
	conncron := userRemainModel.GetConnect()
	//命中大于0  则在改时间段内登录过
	if searchResult.Hits.TotalHits > 0 {
		//fmt.Printf("Found a total of %d tweets\n", searchResult.Hits.TotalHits)
		var existRemain userRemain
		has, _ := conncron.Where("uid=?", accountInfo.Id).Get(&existRemain)
		//是否已经存在该留存数据
		if has {
			updateUser := new(userRemain)
			updateUser.SecondDay = 1
			affected, err := conncron.Id(existRemain.Id).Update(updateUser)
			fmt.Println(affected)
			fmt.Println(err)
		} else {
			insertUser := new(userRemain)
			insertUser.Uid = accountInfo.Id
			regDataStr := time.Unix(accountInfo.RegTime, 0).Format("20060102")
			regIntData, err := strconv.Atoi(regDataStr)
			insertUser.RegDay = regIntData
			insertUser.SecondDay = 1
			affected, err := conncron.Insert(insertUser)
			fmt.Println(affected)
			fmt.Println(err)
		}

		// // Iterate through results
		// for _, hit := range searchResult.Hits.Hits {
		// 	// hit.Index contains the name of the index

		// 	// Deserialize hit.Source into a Tweet (could also be just a map[string]interface{}).
		// 	var t elkUser
		// 	err := json.Unmarshal(*hit.Source, &t)
		// 	if err != nil {
		// 		// Deserialization failed
		// 	}

		// 	// Work with tweet
		// 	//fmt.Printf("Tweet by %d: %s\n", t.Uid, t.DateDay)
		// }
	} else {
		// No hits
		//fmt.Print("Found no tweets\n")
	}
	if err != nil {
		// Handle error
		panic(err)
	}
	//resp, err := c.Search().
	//fmt.Println(searchResult)
}

func seven(dateAddress *string) {
	t, _ := time.Parse("20060102", *dateAddress)
	startTimeStamp := t.Unix() - 8*3600
	endTimeStamp := t.Unix() - 8*3600 + 24*3600 - 1
	startTm := time.Unix(startTimeStamp+2*86400, 0)
	startDate := startTm.Format("20060102")
	endTm := time.Unix(startTimeStamp+6*86400, 0)
	endDate := endTm.Format("20060102")
	var accountModel Account
	conn := accountModel.GetConnect()

	page := 1
	for page > 0 {
		perPage := 10000
		startLimit := (page - 1) * perPage
		accounts := make([]Account, 0)
		conn.Where(" reg_time between ? and ? ", startTimeStamp, endTimeStamp).Limit(perPage, startLimit).Find(&accounts)
		for _, accountInfo := range accounts {
			sevenRemain(accountInfo, startDate, endDate)
		}
		if len(accounts) > 0 {
			page++
		} else {
			page = 0
		}
	}
}

func sevenRemain(accountInfo Account, startDate string, endDate string) {
	client, err := elastic.NewSimpleClient(elastic.SetURL("http://120.78.168.40:9200"), elastic.SetBasicAuth("vpn", "vpnkibana2017"), elastic.SetScheme("http"))
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	rangeQuery := elastic.NewRangeQuery("data_day").Gte(startDate).Lte(endDate)
	matchQuey := elastic.NewMatchQuery("uid", accountInfo.Id)
	mustQuery := elastic.NewBoolQuery().Must(rangeQuery, matchQuey)
	searchResult, err := client.Search("jg_vpn_activity_log-*").Type("vpn_activity").
		//From(0).Size(10). // take documents 0-9
		Query(mustQuery).
		Pretty(true). // pretty print request and response JSON
		Do(ctx)       // execute
	var userRemainModel userRemain
	conncron := userRemainModel.GetConnect()
	//命中大于0  则在改时间段内登录过
	if searchResult.Hits.TotalHits > 0 {
		//fmt.Printf("Found a total of %d tweets\n", searchResult.Hits.TotalHits)
		var existRemain userRemain
		has, _ := conncron.Where("uid=?", accountInfo.Id).Get(&existRemain)
		//是否已经存在该留存数据
		if has {
			updateUser := new(userRemain)
			updateUser.SevenDay = 1
			affected, err := conncron.Id(existRemain.Id).Update(updateUser)
			fmt.Println(affected)
			fmt.Println(err)
		} else {
			insertUser := new(userRemain)
			insertUser.Uid = accountInfo.Id
			regDataStr := time.Unix(accountInfo.RegTime, 0).Format("20060102")
			regIntData, err := strconv.Atoi(regDataStr)
			insertUser.RegDay = regIntData
			insertUser.SevenDay = 1
			affected, err := conncron.Insert(insertUser)
			fmt.Println(affected)
			fmt.Println(err)
		}

	} else {
		// No hits
		//fmt.Print("Found no tweets\n")
	}
	if err != nil {
		// Handle error
		panic(err)
	}
}

func thirty(dateAddress *string) {
	t, _ := time.Parse("20060102", *dateAddress)
	startTimeStamp := t.Unix() - 8*3600
	endTimeStamp := t.Unix() - 8*3600 + 24*3600 - 1
	startTm := time.Unix(startTimeStamp+7*86400, 0)
	startDate := startTm.Format("20060102")
	endTm := time.Unix(startTimeStamp+29*86400, 0)
	endDate := endTm.Format("20060102")
	var accountModel Account
	conn := accountModel.GetConnect()

	page := 1
	for page > 0 {
		perPage := 10000
		startLimit := (page - 1) * perPage
		accounts := make([]Account, 0)
		conn.Where(" reg_time between ? and ? ", startTimeStamp, endTimeStamp).Limit(perPage, startLimit).Find(&accounts)
		for _, accountInfo := range accounts {
			thirtyRemain(accountInfo, startDate, endDate)
		}
		if len(accounts) > 0 {
			page++
		} else {
			page = 0
		}
	}
}

func thirtyRemain(accountInfo Account, startDate string, endDate string) {
	client, err := elastic.NewSimpleClient(elastic.SetURL("http://120.78.168.40:9200"), elastic.SetBasicAuth("vpn", "vpnkibana2017"), elastic.SetScheme("http"))
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	rangeQuery := elastic.NewRangeQuery("data_day").Gte(startDate).Lte(endDate)
	matchQuey := elastic.NewMatchQuery("uid", accountInfo.Id)
	mustQuery := elastic.NewBoolQuery().Must(rangeQuery, matchQuey)
	searchResult, err := client.Search("jg_vpn_activity_log-*").Type("vpn_activity").
		//From(0).Size(10). // take documents 0-9
		Query(mustQuery).
		Pretty(true). // pretty print request and response JSON
		Do(ctx)       // execute
	var userRemainModel userRemain
	conncron := userRemainModel.GetConnect()
	//命中大于0  则在改时间段内登录过
	if searchResult.Hits.TotalHits > 0 {
		//fmt.Printf("Found a total of %d tweets\n", searchResult.Hits.TotalHits)
		var existRemain userRemain
		has, _ := conncron.Where("uid=?", accountInfo.Id).Get(&existRemain)
		//是否已经存在该留存数据
		if has {
			updateUser := new(userRemain)
			updateUser.ThirtyDay = 1
			affected, err := conncron.Id(existRemain.Id).Update(updateUser)
			fmt.Println(affected)
			fmt.Println(err)
		} else {
			insertUser := new(userRemain)
			//insertUser.RegDay = accountInfo.RegTime
			insertUser.Uid = accountInfo.Id
			regDataStr := time.Unix(accountInfo.RegTime, 0).Format("20060102")
			regIntData, err := strconv.Atoi(regDataStr)
			insertUser.RegDay = regIntData
			insertUser.ThirtyDay = 1
			affected, err := conncron.Insert(insertUser)
			fmt.Println(affected)
			fmt.Println(err)
		}

	} else {
		// No hits
		//fmt.Print("Found no tweets\n")
	}
	if err != nil {
		// Handle error
		panic(err)
	}
}
