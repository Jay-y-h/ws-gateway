package protocol

type Message struct {
	DevId string `json:"dev_id"`
	Msg   string `json:"msg"`
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