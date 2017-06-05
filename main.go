package main

import (
	"log"
	"time"
	"os"
	//"crypto/md5"
	"encoding/gob"
	"bufio"
	"github.com/golang-iot/queue"
	"github.com/golang-iot/files"
	"path/filepath"
	"github.com/joho/godotenv"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func simulateValue(current int) int{
	current = current + 1
	return current
}

func init() {
    gob.Register(queue.Message{}) 
}

func sendFile(name string, path string, queueName string, q queue.Queue){
	log.Printf("Sending a file: %s",name)
	f, err := os.Open(filepath.Join(path,name))
	if err != nil {
		log.Printf("Could not read file")
		return
	}
	
	defer func() {
        if err := f.Close(); err != nil {
			failOnError(err, "Failed to close the file")
        }
		os.Remove(filepath.Join(path,name))
    }()
	
	chunkSize := int64(2048)
	b1 := make([]byte, chunkSize)
	current := 0
	
	fileStat, err := f.Stat()
	failOnError(err, "Failed to read the file info")
	id := time.Now().Format("20060102-150405")
	reader := bufio.NewReader(f)
	for{
		n1, err := reader.Read(b1) 
		if n1 == 0 {
            break
        }

		failOnError(err, "Failed to read the file")
		current = current + 1
		
		fc := new(queue.FileChunk)
		fc.Id = id
		fc.Name = name
		fc.Total = ( fileStat.Size() / chunkSize ) + 1
		fc.Current = int64(current)
		fc.Content = b1[:n1]
		fc.ChunkSize = chunkSize
		//log.Printf("Sending File chunk for %s: %d of %d: %s", fc.Id, fc.Current, fc.Total, md5.Sum(fc.Content))
		
		body := queue.ToGOB64(*fc)

		err = q.Send(queueName, body)

		failOnError(err, "Failed to publish a message")
	}
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}


	que := queue.Queue{}
	que.Init(os.Getenv("RABBITMQ_HOST"))
	defer que.Close()
	
	que.GetQueue("hello")
	que.GetQueue("images")
	que.GetQueue("fileComplete")
	
	watcher := files.WatchNewFiles(func(filename string){
		sendFile(filepath.Base(filename), filepath.Dir(filename), "images", que)
	})
	defer watcher.Close()	
	
	err = watcher.Add(filepath.Clean(os.Getenv("IMGS_PATH")))
	if err != nil {
		log.Fatal(err)
	}
	
	
	fileNotif := que.Consume("fileComplete")
	
	forever := make(chan bool)
		go func(){
			for d := range fileNotif {
				m := queue.FromGOB64(string(d.Body))
				
				log.Printf("File received: "+m.Message)
			}
		}()

	log.Printf(" [*] Waiting for Files to send. To exit press CTRL+C")
	<-forever
		
}
