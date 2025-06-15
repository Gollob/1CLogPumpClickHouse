package config

import (
	"encoding/xml"
)

type OneCLogCfg struct {
	XMLName xml.Name     `xml:"config"`
	Logs    []OneCLogRec `xml:"log"`
}
type OneCLogRec struct {
	Location string         `xml:"location,attr"`
	Events   []OneCLogEvent `xml:"event"`
}
type OneCLogEvent struct {
	Eq *OneCLogEq `xml:"eq"`
}
type OneCLogEq struct {
	Property string `xml:"property,attr"`
	Value    string `xml:"value,attr"`
}
type SimpleLogCfg struct {
	XMLName xml.Name    `xml:"logs"`
	Logs    []SimpleLog `xml:"log"`
}
type SimpleLog struct {
	Path  string `xml:"path,attr"`
	Event string `xml:"event,attr"`
}
