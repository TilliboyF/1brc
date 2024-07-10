package data

import "fmt"

type Measurement struct {
	Min    int32
	Max    int32
	Sum    int64
	Amount int32
}

func NewMeasurement(val int32) *Measurement {
	return &Measurement{
		Min:    val,
		Max:    val,
		Sum:    int64(val),
		Amount: 1,
	}
}

func (m *Measurement) AddVal(val int32) {
	if val < m.Min {
		m.Min = val
	}
	if val > m.Max {
		m.Max = val
	}
	m.Sum += int64(val)
	m.Amount += 1
}

func (m Measurement) String() string {
	return fmt.Sprintf("%f/%f/%f",
		float64(m.Min)/10,
		float64(m.Max)/10,
		float64(m.Sum)/(float64(m.Amount)*10))
}
