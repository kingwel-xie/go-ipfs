PB = $(wildcard *.proto)
GO = $(PB:.proto=.pb.go)

all: $(GO)

%.pb.go: %.proto
		/d/work/bin/protoc --proto_path=/d/work/src:. --gogofaster_out=. $<

clean:
		rm -f *.pb.go
		rm -f *.go
