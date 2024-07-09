package main

import "fmt"

type Measurement struct {
	Name   string
	Min    int64
	Max    int64
	Sum    int64
	Amount int64
}

func NewMeasurement(name string, val int64) *Measurement {
	return &Measurement{
		Name:   name,
		Min:    val,
		Max:    val,
		Sum:    val,
		Amount: 1,
	}
}

func (m *Measurement) addVal(val int64) {
	if val < m.Min {
		m.Min = val
	}
	if val > m.Max {
		m.Max = val
	}
	m.Sum += val
	m.Amount += 1
}

func (m Measurement) String() string {
	return fmt.Sprintf("%s=%f/%f/%f", m.Name, float64(m.Min)/10, float64(m.Max)/10, float64(m.Sum)/(float64(m.Amount)*10))
}
