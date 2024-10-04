package router

import (
	"bytes"
	"echo8/kafka-rest-producer/internal/model"
	"echo8/kafka-rest-producer/internal/util"
	"log/slog"
	"text/template"
)

func newTemplatedTopic(topic string) (templatedTopic, error) {
	if util.HasMsgVar(topic) {
		tmpl, err := util.ConvertToMsgTmpl(topic)
		if err != nil {
			return nil, err
		}
		return &topicWithTemplate{orig: topic, tmpl: tmpl}, nil
	} else {
		return &topicWithoutTemplate{orig: topic}, nil
	}
}

type templatedTopic interface {
	Get(msg *model.ProduceMessage) string
}

type topicWithoutTemplate struct {
	orig string
}

func (t *topicWithoutTemplate) Get(msg *model.ProduceMessage) string {
	return t.orig
}

type topicWithTemplate struct {
	orig string
	tmpl *template.Template
}

func (t *topicWithTemplate) Get(msg *model.ProduceMessage) string {
	out := new(bytes.Buffer)
	if err := t.tmpl.Execute(out, msg); err != nil {
		slog.Error("Failed to expand templated topic.", "tmpl", t.orig, "error", err)
		return ""
	}
	return out.String()
}
