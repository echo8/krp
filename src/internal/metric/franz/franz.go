package franz

import (
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
)

func NewMeters() (*Meters, error) {
	kotelService := kotel.NewKotel(kotel.WithMeter(kotel.NewMeter()))
	return &Meters{kotelService: kotelService}, nil
}

type Meters struct {
	kotelService *kotel.Kotel
}

func (m *Meters) GetHooks() []kgo.Hook {
	return m.kotelService.Hooks()
}
