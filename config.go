package main

import (
	"errors"
	"regexp"

	"github.com/ZipRecruiter/cloudwatching/pkg/exportcloudwatch"
)

type exportConfig struct {
	Namespace         string                 `json:"namespace"`
	Name              string                 `json:"name"`
	Dimensions        []string               `json:"dimensions"`
	Statistics        []string               `json:"statistics"`
	TagSelect         map[string]interface{} `json:"tag_select"`
	DimensionsMatch   map[string]string      `json:"dimensionsMatch"`
	DimensionsNoMatch map[string]string      `json:"dimensionsNoMatch"`
	StatDefault       string
}

type configuration struct {
	Region string
	Debug  bool

	ExportConfigs []exportConfig

	exportConfigs []exportcloudwatch.ExportConfig
}

func (c *configuration) Validate() error {
	c.exportConfigs = make([]exportcloudwatch.ExportConfig, len(c.ExportConfigs))
	for i, raw := range c.ExportConfigs {
		c.exportConfigs[i] = exportcloudwatch.ExportConfig{
			Namespace:         raw.Namespace,
			Name:              raw.Name,
			Dimensions:        raw.Dimensions,
			Statistics:        raw.Statistics,
			TagSelect:         raw.TagSelect,
			DimensionsMatch:   make(map[string]*regexp.Regexp, len(raw.DimensionsMatch)),
			DimensionsNoMatch: make(map[string]*regexp.Regexp, len(raw.DimensionsNoMatch)),
		}

		// If tag_select.tag_selections.tags has a length then we need to verify that the required
		// fields are present
		if len(raw.TagSelect) > 0 {
			if raw.TagSelect["resource_type_selection"] == nil || raw.TagSelect["resource_id_dimension"] == nil {
				return errors.New("resource_type_selection and resource_id_dimension are required with tag_select")
			}
		}

		if raw.StatDefault == "Prior" {
			c.exportConfigs[i].StatDefault = exportcloudwatch.Prior
		} else if raw.StatDefault == "Zero" {
			c.exportConfigs[i].StatDefault = exportcloudwatch.Zero
		} else if raw.StatDefault == "NaN" {
			c.exportConfigs[i].StatDefault = exportcloudwatch.NaN
		} else if raw.StatDefault != "" {
			return errors.New("StatDefault must be one of Prior, Zero, or NaN")
		}

		for k, v := range raw.DimensionsMatch {
			re, err := regexp.Compile(v)
			if err != nil {
				return err
			}
			c.exportConfigs[i].DimensionsMatch[k] = re
		}
		for k, v := range raw.DimensionsNoMatch {
			re, err := regexp.Compile(v)
			if err != nil {
				return err
			}
			c.exportConfigs[i].DimensionsNoMatch[k] = re
		}
		if err := c.exportConfigs[i].Validate(); err != nil {
			return err
		}
	}

	return nil
}
