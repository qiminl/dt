package dt

type Traffic struct {
	Size    string          `json:"size"`
	Counter int             `json:"counter"`
	tc      Traffic_content `json:"content"`
}

type Traffic_content struct {
	App_id   string `json:"app_id"`
	Camp_id  string `json:"camp_id"`
	Pub_id   string `json:"pub_id"`
	Pub_v_id string `json:"pub_v_id"`
	Status   string `json:"status"`
	Ext_id   string `json:"ext_id"`
}
