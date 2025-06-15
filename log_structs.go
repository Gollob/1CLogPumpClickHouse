// log_structs.go
// Определение структур Log, Event, Eq, Ge, Property для работы с config.xml

package main

type Log struct {
	History    int        `xml:"history,attr"`
	Location   string     `xml:"location,attr"`
	Events     []Event    `xml:"event"`
	Properties []Property `xml:"property"`
}

type Event struct {
	Eq *Eq `xml:"eq"`
	Ge *Ge `xml:"ge"`
}

type Eq struct {
	Property string `xml:"property,attr"`
	Value    string `xml:"value,attr"`
}

type Ge struct {
	Property string `xml:"property,attr"`
	Value    string `xml:"value,attr"`
}

type Property struct {
	Name string `xml:"name,attr"`
}
