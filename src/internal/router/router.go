package router

import (
	"context"
	"log/slog"
	"net/http"
	"slices"

	"github.com/echo8/krp/internal/config"
	pmodel "github.com/echo8/krp/internal/model"
	"github.com/echo8/krp/internal/producer"
	"github.com/echo8/krp/model"
)

type Router interface {
	SendAsync(ctx context.Context, httpReq *http.Request, msgs []model.ProduceMessage) error
	SendSync(ctx context.Context, httpReq *http.Request, msgs []model.ProduceMessage) ([]model.ProduceResult, error)
}

func New(cfg *config.EndpointConfig, ps producer.Service) (Router, error) {
	tpMap := make(map[config.ProducerId][]config.Topic)
	hasTemplate := false
	hasMatcher := false
	for _, route := range cfg.Routes {
		if route.Match != "" {
			hasMatcher = true
		}
		topics := route.Topics()
		for i := range topics {
			if topics[i].HasTemplate() {
				hasTemplate = true
			}
		}
		for _, pid := range route.Producers() {
			if pid.HasTemplate() {
				hasTemplate = true
			}
			v, ok := tpMap[pid]
			if !ok {
				v = make([]config.Topic, 0)
			}
			v = append(v, topics...)
			tpMap[pid] = v
		}
	}
	var topics []config.Topic
	allSameTopics := true
	for _, v := range tpMap {
		if topics == nil {
			topics = v
		} else if !slices.Equal(topics, v) {
			allSameTopics = false
			break
		}
	}
	if (len(tpMap) == 1 || allSameTopics) && !hasTemplate && !hasMatcher {
		// use multiTPRouter
		tmplTopics := make([]templatedTopic, 0)
		for _, topic := range topics {
			t, err := newTemplatedTopic(string(topic))
			if err != nil {
				return nil, err
			}
			tmplTopics = append(tmplTopics, t)
		}
		producers := make([]producer.Producer, 0)
		for pid := range tpMap {
			producer := ps.GetProducer(pid)
			producers = append(producers, producer)
		}
		return &multiTPRouter{cfg: cfg, ps: producers, ts: tmplTopics}, nil
	} else if !hasMatcher {
		// use allMatchRouter
		pids := make([]string, 0, len(tpMap))
		for pid := range tpMap {
			pids = append(pids, string(pid))
		}
		slices.Sort(pids)
		tmplTopics := make([][]templatedTopic, 0)
		for _, pid := range pids {
			ts := make([]templatedTopic, 0)
			for _, topic := range tpMap[config.ProducerId(pid)] {
				t, err := newTemplatedTopic(string(topic))
				if err != nil {
					return nil, err
				}
				ts = append(ts, t)
			}
			tmplTopics = append(tmplTopics, ts)
		}
		producers := make([]templatedProducer, 0)
		for _, pid := range pids {
			p, err := newTemplatedProducer(config.ProducerId(pid))
			if err != nil {
				return nil, err
			}
			producers = append(producers, p)
		}
		return &allMatchRouter{cfg: cfg, ps: ps, pts: producers, ts: tmplTopics}, nil
	} else {
		// use matchingRouter
		ms := make([]routeMatcher, 0, len(cfg.Routes))
		pts := make([][]templatedProducer, 0, len(cfg.Routes))
		ts := make([][]templatedTopic, 0, len(cfg.Routes))
		for _, route := range cfg.Routes {
			matcher, err := newRouteMatcher(route.Match)
			if err != nil {
				return nil, err
			}
			ms = append(ms, matcher)
			rps := route.Producers()
			rpts := make([]templatedProducer, 0, len(rps))
			for _, rp := range rps {
				rpt, err := newTemplatedProducer(config.ProducerId(rp))
				if err != nil {
					return nil, err
				}
				rpts = append(rpts, rpt)
			}
			pts = append(pts, rpts)
			rts := route.Topics()
			rtts := make([]templatedTopic, 0, len(rts))
			for _, rt := range rts {
				rtt, err := newTemplatedTopic(string(rt))
				if err != nil {
					return nil, err
				}
				rtts = append(rtts, rtt)
			}
			ts = append(ts, rtts)
		}
		return &matchingRouter{cfg: cfg, ps: ps, pts: pts, ts: ts, ms: ms}, nil
	}
}

type multiTPRouter struct {
	cfg *config.EndpointConfig
	ps  []producer.Producer
	ts  []templatedTopic
}

func (r *multiTPRouter) SendAsync(ctx context.Context, httpReq *http.Request, msgs []model.ProduceMessage) error {
	batch := r.createBatch(msgs)
	for _, p := range r.ps {
		if err := p.SendAsync(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (r *multiTPRouter) SendSync(ctx context.Context, httpReq *http.Request, msgs []model.ProduceMessage) ([]model.ProduceResult, error) {
	batch := r.createBatch(msgs)
	resMap := make(map[int]model.ProduceResult, len(msgs))
	for _, p := range r.ps {
		results, err := p.SendSync(ctx, batch)
		if err != nil {
			return nil, err
		}
		for i := range results {
			result, ok := resMap[results[i].Pos]
			if ok {
				if result.Success {
					resMap[results[i].Pos] = results[i]
				}
			} else {
				resMap[results[i].Pos] = results[i]
			}
		}
	}
	res := make([]model.ProduceResult, 0, len(msgs))
	for i := range len(msgs) {
		res = append(res, resMap[i])
	}
	return res, nil
}

func (r *multiTPRouter) createBatch(msgs []model.ProduceMessage) *pmodel.MessageBatch {
	topicMsgs := make([]pmodel.TopicAndMessage, 0, len(r.ts)*len(msgs))
	for _, t := range r.ts {
		for i := range msgs {
			msg := &msgs[i]
			topicMsgs = append(topicMsgs, pmodel.TopicAndMessage{Topic: t.Get(msg), Message: msg, Pos: i})
		}
	}
	return &pmodel.MessageBatch{Messages: topicMsgs, Src: r.cfg.Endpoint}
}

type allMatchRouter struct {
	cfg *config.EndpointConfig
	ps  producer.Service
	pts []templatedProducer
	ts  [][]templatedTopic
}

func (r *allMatchRouter) SendAsync(ctx context.Context, httpReq *http.Request, msgs []model.ProduceMessage) error {
	batchMap := r.createBatches(msgs)
	for pid, batch := range batchMap {
		if p, ok := r.ps.LookupProducer(pid); ok {
			err := p.SendAsync(ctx, batch)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *allMatchRouter) SendSync(ctx context.Context, httpReq *http.Request, msgs []model.ProduceMessage) ([]model.ProduceResult, error) {
	batchMap := r.createBatches(msgs)
	resMap := make(map[int]model.ProduceResult, len(msgs))
	for pid, batch := range batchMap {
		p, ok := r.ps.LookupProducer(pid)
		if ok {
			results, err := p.SendSync(ctx, batch)
			if err != nil {
				return nil, err
			}
			for i := range results {
				result, ok := resMap[results[i].Pos]
				if ok {
					if result.Success {
						resMap[results[i].Pos] = results[i]
					}
				} else {
					resMap[results[i].Pos] = results[i]
				}
			}
		} else {
			for i := range batch.Messages {
				resMap[batch.Messages[i].Pos] = model.ProduceResult{Success: false}
			}
		}
	}
	res := make([]model.ProduceResult, 0, len(msgs))
	for i := range len(msgs) {
		res = append(res, resMap[i])
	}
	return res, nil
}

func (r *allMatchRouter) createBatches(msgs []model.ProduceMessage) map[config.ProducerId]*pmodel.MessageBatch {
	batchMap := make(map[config.ProducerId]*pmodel.MessageBatch)
	for i := range msgs {
		msg := &msgs[i]
		for j, pt := range r.pts {
			pid := pt.Get(msg)
			batch := batchMap[pid]
			if batch == nil {
				batch = &pmodel.MessageBatch{Messages: make([]pmodel.TopicAndMessage, 0), Src: r.cfg.Endpoint}
				batchMap[pid] = batch
			}
			for _, t := range r.ts[j] {
				batch.Messages = append(batch.Messages, pmodel.TopicAndMessage{Topic: t.Get(msg), Message: msg, Pos: i})
			}
		}
	}
	return batchMap
}

type matchingRouter struct {
	cfg *config.EndpointConfig
	ps  producer.Service
	pts [][]templatedProducer
	ts  [][]templatedTopic
	ms  []routeMatcher
}

func (r *matchingRouter) SendAsync(ctx context.Context, httpReq *http.Request, msgs []model.ProduceMessage) error {
	batchMap := r.createBatches(httpReq, msgs)
	for pid, batch := range batchMap {
		if p, ok := r.ps.LookupProducer(pid); ok {
			err := p.SendAsync(ctx, batch)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *matchingRouter) SendSync(ctx context.Context, httpReq *http.Request, msgs []model.ProduceMessage) ([]model.ProduceResult, error) {
	batchMap := r.createBatches(httpReq, msgs)
	resMap := make(map[int]model.ProduceResult, len(msgs))
	for pid, batch := range batchMap {
		p, ok := r.ps.LookupProducer(pid)
		if ok {
			results, err := p.SendSync(ctx, batch)
			if err != nil {
				return nil, err
			}
			for i := range results {
				result, ok := resMap[results[i].Pos]
				if ok {
					if result.Success {
						resMap[results[i].Pos] = results[i]
					}
				} else {
					resMap[results[i].Pos] = results[i]
				}
			}
		} else {
			for i := range batch.Messages {
				resMap[batch.Messages[i].Pos] = model.ProduceResult{Success: false}
			}
		}
	}
	results := make([]model.ProduceResult, 0, len(msgs))
	unmatchedCnt := 0
	for i := range len(msgs) {
		res, ok := resMap[i]
		if ok {
			results = append(results, res)
		} else {
			results = append(results, model.ProduceResult{Success: true, Pos: i})
			unmatchedCnt += 1
		}
	}
	if unmatchedCnt > 0 {
		slog.Warn("Some messages did not match any routes.", "count", unmatchedCnt)
	}
	return results, nil
}

func (r *matchingRouter) createBatches(httpReq *http.Request, msgs []model.ProduceMessage) map[config.ProducerId]*pmodel.MessageBatch {
	batchMap := make(map[config.ProducerId]*pmodel.MessageBatch)
	for i := range msgs {
		msg := &msgs[i]
		for j, matcher := range r.ms {
			if matcher.Matches(msg, httpReq) {
				producers := r.pts[j]
				for _, pt := range producers {
					pid := pt.Get(msg)
					batch := batchMap[pid]
					if batch == nil {
						batch = &pmodel.MessageBatch{Messages: make([]pmodel.TopicAndMessage, 0), Src: r.cfg.Endpoint}
						batchMap[pid] = batch
					}
					for _, t := range r.ts[j] {
						batch.Messages = append(batch.Messages, pmodel.TopicAndMessage{Topic: t.Get(msg), Message: msg, Pos: i})
					}
				}
			}
		}
	}
	return batchMap
}
