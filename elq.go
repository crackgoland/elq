package elq

import (
	"context"
	"fmt"
	"log"
	"os"
	"errors"
	"encoding/json"
	"github.com/olivere/elastic"
	"github.com/crackgoland/env"
)

type Elastic struct {
	ctx				context.Context
	client		*elastic.Client
}

type ElasticIndex struct {
	es 		*Elastic
	name 	string
}

var (
	ErrUnInitializedIndex = errors.New("Use of uninitialized index reference")
	ErrUnInitializedClient = errors.New("Use of uninitialized client reference")
)

func NewElastic(e *env.Env) (*Elastic, error) {
	ctx := context.Background()

	ES_HOST, _ := e.Get("ES_HOST", "1.2.3.4")
  elasticAddr := fmt.Sprintf("http://%s:9200", ES_HOST)

	client, err := elastic.NewClient(
    elastic.SetURL(elasticAddr),
		elastic.SetSniff(false),
		elastic.SetHealthcheck(false),
	  elastic.SetGzip(true),
	  elastic.SetErrorLog(log.New(os.Stderr, "ELASTIC ", log.LstdFlags)),
	  // elastic.SetInfoLog(log.New(os.Stdout, "", log.LstdFlags)),
	)
	if err != nil {
		return nil, err
	}

	// Ping the Elasticsearch server to get e.g. the version number
	info, code, err := client.Ping(elasticAddr).Do(ctx)
	if err != nil {
		return nil, err
	}

	// Getting the ES version number is quite common, so there's a shortcut
	esversion, err := client.ElasticsearchVersion(elasticAddr)
	if err != nil {
		return nil, err
	}

	info = info
	code = code
	esversion = esversion

	return &Elastic{ctx, client}, nil
}

func (es *Elastic) ForIndex(name string) ElasticIndex {
	return ElasticIndex{es: es, name: name}
}

func (es *Elastic) NewIndexMapped(name string, mapping string) (ElasticIndex, error) {
	if es.client == nil {
		return ElasticIndex{}, ErrUnInitializedClient
	}

	_, err := es.client.CreateIndex(name).BodyString(mapping).Do(es.ctx)
	if err != nil {
	    return ElasticIndex{}, err
	}

	return ElasticIndex{es: es, name: name}, nil
}

func (es *Elastic) NewIndex(name string) (ElasticIndex, error) {
	if es.client == nil {
		return ElasticIndex{}, ErrUnInitializedClient
	}

	_, err := es.client.CreateIndex(name).Do(es.ctx)
	if err != nil {
	    return ElasticIndex{}, err
	}

	return ElasticIndex{es: es, name: name}, nil
}

func (es *Elastic) GetOrCreateIndex(name string) (ElasticIndex, error)  {
	if es.client == nil {
		return ElasticIndex{}, ErrUnInitializedClient
	}

	names, err := es.client.IndexNames()
	if err != nil {
		return ElasticIndex{}, err
	}

	for _, aName := range names {
		if aName == name {
			return ElasticIndex{es: es, name: name}, nil
		}
	}

	return es.NewIndex(name)
}

func (i *ElasticIndex) PushId(id string, object interface{}) error {
	if i.es == nil {
		return ErrUnInitializedIndex
	}

	_, err := i.es.client.Index().
	    Index(i.name).
			Id(id).
	    BodyJson(object).
	    Do(i.es.ctx)

	if err != nil {
		return err
	}

	return nil
}

func (i *ElasticIndex) Push(object interface{}) (string, error) {
	if i.es == nil {
		return "", ErrUnInitializedIndex
	}

	put1, err := i.es.client.Index().
	    Index(i.name).
	    BodyJson(object).
	    Do(i.es.ctx)

	if err != nil {
		return "", err
	}

	return put1.Id, nil
}

func (i *ElasticIndex) Pull(id string, object interface{}) (found bool, err error) {
	if i.es == nil {
		return false, ErrUnInitializedIndex
	}

	get1, err := i.es.client.Get().
    Index(i.name).
    Id(id).
    Do(i.es.ctx)

	if elastic.IsNotFound(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	if !get1.Found {
		return false, nil
	}

	return true, json.Unmarshal(get1.Source, object)
}
