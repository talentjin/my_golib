package helper

import "time"

type TimeTool struct{}

func (*TimeTool) MakeTimeToDate(timestamp int64) string {
	dateStr := time.Unix(timestamp, 0).Format("20060102")
	return dateStr
}

func (*TimeTool) MakeDateToTime(dateStr string) int64 {
	t, _ := time.Parse("20060102", dateStr)
	timeStamp := t.Unix()
	return timeStamp
}
