package dt

// Campaign data model
type Device struct {
	Os_n          string `json:"os_n"`
	Os_v          string `json:"os_v"`
	Device_id     string `json:"device_id"`
	Device_mac    string `json:"device_mac"`
	Device_type   string `json:"device_type"`
	Device_ifa    string `json:"device_ifa"`
	Device_vendor string `json:"device_vendor"`
	Device_model  string `json:"device_model"`
	Carrier_code  string `json:"carrier_code"`
	Conn_type     string `json:"conn_type"`
	Ios_ifa       string `json:"ios_ifa"`
	Android_id    string `json:"android_id"`
	Device_pid    string `json:"device_pid"`
	Operator      string `json:"operator"`
}
