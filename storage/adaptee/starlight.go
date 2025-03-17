package adaptee

type StarLight struct {
	user     string
	pass     string
	endPoint string
}

func (o *StarLight) Upload() (err error) {
	return err
}

func (o *StarLight) Download() (err error) {
	return err
}
