package router

import (
	"context"
	"echo8/kafka-rest-producer/internal/config"
	"echo8/kafka-rest-producer/internal/model"
	"echo8/kafka-rest-producer/internal/producer"
	"slices"
)

type Router interface {
	SendAsync(ctx context.Context, msgs []model.ProduceMessage) error
	SendSync(ctx context.Context, msgs []model.ProduceMessage) ([]model.ProduceResult, error)
}

func New(cfg *config.EndpointConfig, ps producer.Service) (Router, error) {
	tpMap := make(map[config.ProducerId][]config.Topic)
	hasTemplatedProducer := false
	for _, route := range cfg.Routes {
		topics := route.Topics()
		for _, pid := range route.Producers() {
			if pid.HasTemplate() {
				hasTemplatedProducer = true
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
	if (len(tpMap) == 1 || allSameTopics) && !hasTemplatedProducer {
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
	} else {
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
	}
}

type multiTPRouter struct {
	cfg *config.EndpointConfig
	ps  []producer.Producer
	ts  []templatedTopic
}

func (r *multiTPRouter) SendAsync(ctx context.Context, msgs []model.ProduceMessage) error {
	batch := r.createBatch(msgs)
	for _, p := range r.ps {
		if err := p.SendAsync(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func (r *multiTPRouter) SendSync(ctx context.Context, msgs []model.ProduceMessage) ([]model.ProduceResult, error) {
	batch := r.createBatch(msgs)
	res := make([]model.ProduceResult, 0, len(msgs))
	for i, p := range r.ps {
		results, err := p.SendSync(ctx, batch)
		if err != nil {
			return nil, err
		}
		if i == 0 {
			res = append(res, results...)
		} else {
			for j := range results {
				if res[j].Success {
					res[j] = results[j]
				}
			}
		}
	}
	return res, nil
}

func (r *multiTPRouter) createBatch(msgs []model.ProduceMessage) *model.MessageBatch {
	topicMsgs := make([]model.TopicAndMessage, 0, len(r.ts)*len(msgs))
	for _, t := range r.ts {
		for i := range msgs {
			msg := &msgs[i]
			topicMsgs = append(topicMsgs, model.TopicAndMessage{Topic: t.Get(msg), Message: msg})
		}
	}
	return &model.MessageBatch{Messages: topicMsgs, Src: r.cfg.Endpoint}
}

type allMatchRouter struct {
	cfg *config.EndpointConfig
	ps  producer.Service
	pts []templatedProducer
	ts  [][]templatedTopic
}

func (r *allMatchRouter) SendAsync(ctx context.Context, msgs []model.ProduceMessage) error {
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

func (r *allMatchRouter) SendSync(ctx context.Context, msgs []model.ProduceMessage) ([]model.ProduceResult, error) {
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

func (r *allMatchRouter) createBatches(msgs []model.ProduceMessage) map[config.ProducerId]*model.MessageBatch {
	batchMap := make(map[config.ProducerId]*model.MessageBatch)
	for i := range msgs {
		msg := &msgs[i]
		for j, pt := range r.pts {
			pid := pt.Get(msg)
			batch := batchMap[pid]
			if batch == nil {
				batch = &model.MessageBatch{Messages: make([]model.TopicAndMessage, 0), Src: r.cfg.Endpoint}
				batchMap[pid] = batch
			}
			for _, t := range r.ts[j] {
				batch.Messages = append(batch.Messages, model.TopicAndMessage{Topic: t.Get(msg), Message: msg})
			}
		}
	}
	return batchMap
}
