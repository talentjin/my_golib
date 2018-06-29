package main

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/go-xorm/xorm"
	"github.com/olivere/elastic"
	"github.com/talentjin/my_golib/helper"
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
	//engine.ShowSQL(true)
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
	//engine.ShowSQL(true)
	return engine
}

type Account struct {
	Basemodel `xorm:"extends"`
	RegTime   int64 `form:"reg_time" xorm:"not null INT(11)"`
}

func (Account) TableName() string {
	return "vpn_account"
}

func main() {

	opType := flag.String("type", "second", "op type")
	dateFlag := flag.String("date", "", "op date") //time.Now().Local().Format("20060102")
	flag.Parse()
	//二日留存
	if *opType == "second" {
		if len(*dateFlag) <= 0 {
			var waitGroup = sync.WaitGroup{}
			calTime := helper.MakeDateToTime("20180504")
			timeToday := time.Now().Unix() - 8*3600
			totalDays := helper.CalDays(calTime, timeToday)

			var p helper.Pool
			p.Init(10, int(totalDays))
			for calTime < (timeToday - 86400) {
				waitGroup.Add(1)
				dateStr := helper.MakeTimeToDate(calTime)
				p.AddTask(func() error {
					return second(&dateStr, &waitGroup)
				})
				calTime = calTime + 86400
			}
			p.SetFinishCallback(RunFinish)
			p.Start()
			waitGroup.Wait()
			p.Stop()
		} else {
			var waitGroup = sync.WaitGroup{}
			second(dateFlag, &waitGroup)
			waitGroup.Wait()
		}
	}
	//七日留存
	if *opType == "seven" {
		if len(*dateFlag) <= 0 {
			var waitGroup = sync.WaitGroup{}
			calTime := helper.MakeDateToTime("20180504")
			timeToday := time.Now().Unix() - 8*3600
			totalDays := helper.CalDays(calTime, timeToday-6*86400)

			var p helper.Pool
			p.Init(10, int(totalDays))
			for calTime < (timeToday - 7*86400) {
				waitGroup.Add(1)
				dateStr := helper.MakeTimeToDate(calTime)
				p.AddTask(func() error {
					return seven(&dateStr, &waitGroup)
				})
				calTime = calTime + 86400
			}
			p.SetFinishCallback(RunFinish)
			p.Start()
			waitGroup.Wait()
			p.Stop()
		} else {
			var waitGroup = sync.WaitGroup{}
			seven(dateFlag, &waitGroup)
			waitGroup.Wait()
		}
	}
	//30日留存
	if *opType == "thirty" {
		if len(*dateFlag) <= 0 {
			var waitGroup = sync.WaitGroup{}
			calTime := helper.MakeDateToTime("20180504")
			timeToday := time.Now().Unix() - 8*3600
			totalDays := helper.CalDays(calTime, timeToday-29*86400)

			var p helper.Pool
			p.Init(10, int(totalDays))
			for calTime < (timeToday - 30*86400) {
				waitGroup.Add(1)
				dateStr := helper.MakeTimeToDate(calTime)
				p.AddTask(func() error {
					return thirty(&dateStr, &waitGroup)
				})
				calTime = calTime + 86400
			}
			p.SetFinishCallback(RunFinish)
			p.Start()
			waitGroup.Wait()
			p.Stop()
		} else {
			var waitGroup = sync.WaitGroup{}
			thirty(dateFlag, &waitGroup)
			waitGroup.Wait()
		}
	}
}

func RunFinish() {
	fmt.Println("success")
}

func second(dateAddress *string, wg *sync.WaitGroup) error {
	fmt.Println(*dateAddress)
	//当天时间戳 范围
	startTimeStamp := helper.MakeDateToTime(*dateAddress)
	endTimeStamp := startTimeStamp + 24*3600 - 1
	//elk查询日期范围
	startDate := helper.MakeTimeToDate(startTimeStamp + 86400)
	endDate := helper.MakeTimeToDate(startTimeStamp + 86400)
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
	wg.Done()
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
			conncron.Id(existRemain.Id).Update(updateUser)
		} else {
			insertUser := new(userRemain)
			insertUser.Uid = accountInfo.Id
			regDataStr := helper.MakeTimeToDate(accountInfo.RegTime)
			regIntData, _ := strconv.Atoi(regDataStr)
			insertUser.RegDay = regIntData
			insertUser.SecondDay = 1
			conncron.Insert(insertUser)
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

func seven(dateAddress *string, wg *sync.WaitGroup) error {
	fmt.Println(*dateAddress)
	startTimeStamp := helper.MakeDateToTime(*dateAddress)
	endTimeStamp := startTimeStamp + 24*3600 - 1

	startDate := helper.MakeTimeToDate(startTimeStamp + 2*86400)
	endDate := helper.MakeTimeToDate(startTimeStamp + 6*86400)
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
	wg.Done()
	return nil
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
			conncron.Id(existRemain.Id).Update(updateUser)
		} else {
			insertUser := new(userRemain)
			insertUser.Uid = accountInfo.Id
			regDataStr := time.Unix(accountInfo.RegTime, 0).Format("20060102")
			regIntData, _ := strconv.Atoi(regDataStr)
			insertUser.RegDay = regIntData
			insertUser.SevenDay = 1
			conncron.Insert(insertUser)
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

func thirty(dateAddress *string, wg *sync.WaitGroup) error {
	fmt.Println(*dateAddress)
	startTimeStamp := helper.MakeDateToTime(*dateAddress)
	endTimeStamp := startTimeStamp + 24*3600 - 1

	startDate := helper.MakeTimeToDate(startTimeStamp + 7*86400)
	endDate := helper.MakeTimeToDate(startTimeStamp + 29*86400)

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
	wg.Done()
	return nil
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
			conncron.Id(existRemain.Id).Update(updateUser)
		} else {
			insertUser := new(userRemain)
			//insertUser.RegDay = accountInfo.RegTime
			insertUser.Uid = accountInfo.Id
			regDataStr := time.Unix(accountInfo.RegTime, 0).Format("20060102")
			regIntData, _ := strconv.Atoi(regDataStr)
			insertUser.RegDay = regIntData
			insertUser.ThirtyDay = 1
			conncron.Insert(insertUser)
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
