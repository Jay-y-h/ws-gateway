package protocol

type Message struct {
	ID        string `json:"id"`
	Type      string `json:"type"`
	From      string `json:"from"`
	To        string `json:"to,omitempty"`
	Timestamp int64  `json:"timestamp"`
	Payload   any    `json:"payload,omitempty"`
}

type DianxinTestData struct {
	DeviceID           string  `json:"deviceId"`
	CellSN             string  `json:"cellSN"`
	TestTime           string  `json:"testTime"`
	Voltage            float64 `json:"voltage"`
	InternalResistance float64 `json:"internalResistance"`
	SortingBin         string  `json:"sortingBin"`
	Result             string  `json:"result"`
	DefectCode         string  `json:"defectCode"`
	DefectDescription  string  `json:"defectDescription"`
	UploadStatus       string  `json:"uploadStatus"`
}