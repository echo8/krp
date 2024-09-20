package metric

import (
	"fmt"
	"reflect"

	"go.opentelemetry.io/otel"
	otm "go.opentelemetry.io/otel/metric"
)

func createMeters(meters any) error {
	meter := otel.Meter("koko/kafka-rest-producer")
	mt := reflect.TypeOf(meters).Elem()
	mv := reflect.ValueOf(meters).Elem()
	for i := range mt.NumField() {
		field := mt.Field(i)
		name := field.Tag.Get("name")
		description := field.Tag.Get("description")
		unit := field.Tag.Get("unit")
		vField := mv.Field(i)
		var m any
		var err error
		switch vField.Type() {
		case reflect.TypeOf((*otm.Int64Histogram)(nil)).Elem():
			if m, err = meter.Int64Histogram(name, otm.WithDescription(description), otm.WithUnit(unit)); err != nil {
				return err
			}
			vField.Set(reflect.ValueOf(m))
		case reflect.TypeOf((*otm.Float64Histogram)(nil)).Elem():
			if m, err = meter.Float64Histogram(name, otm.WithDescription(description), otm.WithUnit(unit)); err != nil {
				return err
			}
			vField.Set(reflect.ValueOf(m))
		case reflect.TypeOf((*otm.Int64Counter)(nil)).Elem():
			if m, err = meter.Int64Counter(name, otm.WithDescription(description), otm.WithUnit(unit)); err != nil {
				return err
			}
			vField.Set(reflect.ValueOf(m))
		case reflect.TypeOf((*otm.Float64Counter)(nil)).Elem():
			if m, err = meter.Float64Counter(name, otm.WithDescription(description), otm.WithUnit(unit)); err != nil {
				return err
			}
			vField.Set(reflect.ValueOf(m))
		case reflect.TypeOf((*otm.Int64Gauge)(nil)).Elem():
			if m, err = meter.Int64Gauge(name, otm.WithDescription(description), otm.WithUnit(unit)); err != nil {
				return err
			}
			vField.Set(reflect.ValueOf(m))
		case reflect.TypeOf((*otm.Float64Gauge)(nil)).Elem():
			if m, err = meter.Float64Gauge(name, otm.WithDescription(description), otm.WithUnit(unit)); err != nil {
				return err
			}
			vField.Set(reflect.ValueOf(m))
		default:
			return fmt.Errorf("meter with type %v is not supported", vField.Type())
		}
	}
	return nil
}
