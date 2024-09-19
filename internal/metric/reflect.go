package metric

import (
	"fmt"
	"reflect"

	"go.opentelemetry.io/otel"
	otm "go.opentelemetry.io/otel/metric"
)

func createMeters(meters any) error {
	meter := otel.Meter("koko/kafka-rest-producer")
	mt := reflect.TypeOf(meters)
	mv := reflect.ValueOf(meters)
	for i := range mt.NumField() {
		field := mt.Field(i)
		name := field.Tag.Get("name")
		description := field.Tag.Get("description")
		unit := field.Tag.Get("unit")
		vField := mv.Field(i)
		var m any
		var err error
		switch t := vField.Interface().(type) {
		case otm.Int64Histogram:
			if m, err = meter.Int64Histogram(name, otm.WithDescription(description), otm.WithUnit(unit)); err != nil {
				return err
			}
			vField.Set(reflect.ValueOf(m))
		case otm.Int64Counter:
			if m, err = meter.Int64Counter(name, otm.WithDescription(description), otm.WithUnit(unit)); err != nil {
				return err
			}
			vField.Set(reflect.ValueOf(m))
		default:
			return fmt.Errorf("meter with type %v is not supported", t)
		}
	}
	return nil
}
