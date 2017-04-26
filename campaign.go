package dt

// Campaign data model
type Campaign struct {
	Id          string `json:"id"`
	Set         string `json:"set"`
	Time        string `json:"time"`
	App_id      string `json:"app_id"`
	Camp_id     string `json:"camp_id"`
	Pub_id      string `json:"pub_id"`
	Pub_v_id    string `json:"pub_v_id"`
	Status      string `json:"status"`
	Ext_id      string `json:"ext_id"`
	Bidder      string `json:"bidder"`
	Cr_type     string `json:"cr_type"`
	Adv_id      string `json:"adv_id"`
	Cr_id       string `json:"cr_id"`
	Demand_type string `json:"demand_type"`
	P_id        string `json:"p_id"`
	Bundle      string `json:"bundle"`
	Adv_v_id    string `json:"adv_v_id"`
}
