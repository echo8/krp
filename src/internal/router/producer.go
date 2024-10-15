package router

import (
	"bytes"
	"log/slog"
	"text/template"

	"github.com/echo8/krp/internal/config"
	"github.com/echo8/krp/internal/util"
	"github.com/echo8/krp/model"
)

func newTemplatedProducer(pid config.ProducerId) (templatedProducer, error) {
	if util.HasMsgVar(string(pid)) {
		tmpl, err := util.ConvertToMsgTmpl(string(pid))
		if err != nil {
			return nil, err
		}
		return &producerWithTemplate{orig: pid, tmpl: tmpl}, nil
	} else {
		return &producerWithoutTemplate{orig: pid}, nil
	}
}

type templatedProducer interface {
	Get(msg *model.ProduceMessage) config.ProducerId
}

type producerWithoutTemplate struct {
	orig config.ProducerId
}

func (p *producerWithoutTemplate) Get(msg *model.ProduceMessage) config.ProducerId {
	return p.orig
}

type producerWithTemplate struct {
	orig config.ProducerId
	tmpl *template.Template
}

func (p *producerWithTemplate) Get(msg *model.ProduceMessage) config.ProducerId {
	out := new(bytes.Buffer)
	if err := p.tmpl.Execute(out, msg); err != nil {
		slog.Error("Failed to expand templated producer.", "tmpl", p.orig, "error", err)
		return ""
	}
	return config.ProducerId(out.String())
}
