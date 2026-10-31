package model

//go:generate go run ../cmd/gencode -input model.go -output model_gen.go -model User

type User struct {
	ID     int64   `db:"id"`
	Active bool    `db:"active"`
	Score  int     `db:"score"`
	Name   string  `db:"name"`
	Note   *string `db:"note,nullable"`
}
