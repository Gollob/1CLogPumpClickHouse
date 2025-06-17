package config

import (
	"encoding/xml"
	"os"
	"path/filepath"
)

// ClickHouseConfig содержит настройки подключения и маппинг таблиц по Component
// Реализует UnmarshalXML для корректной загрузки map[string]string из XML
type ClickHouseConfig struct {
	Address      string // xml тег загружается через UnmarshalXML
	Username     string
	Password     string
	Database     string
	DefaultTable string
	Protocol     string
	TableMap     map[string]string // соответствие Component->Table
}

// Config описывает основные настройки сервиса
type Config struct {
	XMLName          xml.Name         `xml:"Configuration"`
	LogCfgPath       string           `xml:"LogCfgPath"`
	BatchSize        int              `xml:"BatchSize"`
	FlushIntervalSec int              `xml:"FlushIntervalSec"`
	ClickHouse       ClickHouseConfig `xml:"ClickHouse"`
}

// Промежуточная структура для разбора XML
type chCfgXML struct {
	Address      string `xml:"Address"`
	Username     string `xml:"Username"`
	Password     string `xml:"Password"`
	Database     string `xml:"Database"`
	DefaultTable string `xml:"DefaultTable"`
	Protocol     string `xml:"Protocol"`
	TableMaps    []struct {
		Component string `xml:"Component,attr"`
		Table     string `xml:"Table,attr"`
	} `xml:"TableMap>Map"`
}
type oneCLogCfg struct {
	XMLName xml.Name     `xml:"config"`
	Logs    []oneCLogRec `xml:"log"`
}

type oneCLogRec struct {
	Location string         `xml:"location,attr"`
	Events   []oneCLogEvent `xml:"event"`
}

type oneCLogEvent struct {
	Eq *oneCLogEq `xml:"eq"`
}

type oneCLogEq struct {
	Property string `xml:"property,attr"`
	Value    string `xml:"value,attr"`
}

type LogFile struct {
	Path  string
	Event string
}

func (c Config) BatchInterval() int {
	return c.FlushIntervalSec
}

func LoadConfig(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, err
	}
	defer f.Close()
	var cfg Config
	dec := xml.NewDecoder(f)
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// LoadLogFiles ищет ВСЕ .log файлы рекурсивно по папке и подпапкам для каждого event.
func LoadLogFiles(path string) ([]LogFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var src oneCLogCfg
	dec := xml.NewDecoder(f)
	if err := dec.Decode(&src); err != nil {
		return nil, err
	}
	var files []LogFile
	for _, l := range src.Logs {
		var logPaths []string
		_ = filepath.Walk(l.Location, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return nil
			}
			if !info.IsDir() && filepath.Ext(path) == ".log" {
				logPaths = append(logPaths, path)
			}
			return nil
		})
		for _, ev := range l.Events {
			if ev.Eq != nil && ev.Eq.Property == "Name" {
				for _, logPath := range logPaths {
					files = append(files, LogFile{
						Path:  logPath,
						Event: ev.Eq.Value,
					})
				}
			}
		}
	}
	return files, nil
}

func (c *ClickHouseConfig) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var aux chCfgXML
	if err := d.DecodeElement(&aux, &start); err != nil {
		return err
	}
	c.Address = aux.Address
	c.Username = aux.Username
	c.Password = aux.Password
	c.Database = aux.Database
	c.DefaultTable = aux.DefaultTable
	c.Protocol = aux.Protocol
	// Строим map из прочитанных элементов
	c.TableMap = make(map[string]string, len(aux.TableMaps))
	for _, m := range aux.TableMaps {
		c.TableMap[m.Component] = m.Table
	}
	return nil
}
