package router

import (
	"echo8/kafka-rest-producer/internal/model"
	"log/slog"
	"net/http"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

func newRouteMatcher(match string) (routeMatcher, error) {
	if match != "" {
		exprFilter, err := expr.Compile(match, expr.AsBool(), expr.Env(exprEnv{}))
		if err != nil {
			return nil, err
		}
		return &exprLangMatcher{match: match, exprFilter: exprFilter}, nil
	}
	return &allMatcher{}, nil
}

type routeMatcher interface {
	Matches(msg *model.ProduceMessage, httpReq *http.Request) bool
}

type allMatcher struct{}

func (m *allMatcher) Matches(msg *model.ProduceMessage, httpReq *http.Request) bool {
	return true
}

type exprLangMatcher struct {
	match      string
	exprFilter *vm.Program
}

type exprEnv struct {
	Message    *model.ProduceMessage   `expr:"message"`
	HttpHeader func(key string) string `expr:"httpHeader"`
}

func (m *exprLangMatcher) Matches(msg *model.ProduceMessage, httpReq *http.Request) bool {
	res, err := expr.Run(m.exprFilter, &exprEnv{Message: msg, HttpHeader: httpReq.Header.Get})
	if err != nil {
		slog.Error("Failed to run match filter.", "match", m.match, "error", err)
		return false
	}
	return res.(bool)
}
