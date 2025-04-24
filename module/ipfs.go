package module

const (
	ChanResultSuccess = 0
	ChanResultFailed  = -1

	IPFSAuthInterface                       = "/api/v0/get_urchin2_token"
	IPFSAuthInterfaceResponseHeaderTokenKey = "Authorization"
)

type IPFSGetTokenReq struct {
	User       string `json:"user"`
	Pass       string `json:"pass"`
	ExpireTime int32  `json:"expireTime"`
}
