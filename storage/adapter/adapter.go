package adapter

type Storage interface {
	Upload()
	Download()
	Migrate()
}

type Client struct {
}

func (c *Client) upload(s Storage) {
	s.Upload()
}

func (c *Client) Download(s Storage) {
	s.Download()
}

func (c *Client) Migrate(s Storage) {
	s.Migrate()
}
