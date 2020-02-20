package main

// import "sync"

var a string
var ch chan string

func setup() {
	a = "hello, world"
	ch <- a
}

func main() {
	ch = make(chan string, 1)
	go setup()
	str := <-ch
	print(str)
	close(ch)
}
