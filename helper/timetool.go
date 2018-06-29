package helper

import "time"

//将时间转化成 日期 格式  年月日
func MakeTimeToDate(timestamp int64) string {
	dateStr := time.Unix(timestamp, 0).Format("20060102")
	return dateStr
}

//将日期转化为 时间戳
func MakeDateToTime(dateStr string) int64 {
	t, _ := time.Parse("20060102", dateStr)
	timeStamp := t.Unix() - 8*3600
	return timeStamp
}

//计算两个时间戳间相隔多少天
func CalDays(startTime int64, endTime int64) int64 {
	return (endTime - startTime) / 86400
}
