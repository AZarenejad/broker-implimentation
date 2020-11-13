package main

import (
  "net"
  "fmt"
  "os"
  "queue"
  "strings"
  "time"
)

const (
  brokerSize int = 10
)

var queueListHead int = 0
var userQueue queue.Queue = queue.CreateQueue("UserQueue")
func main() {
  if (len(os.Args) != 2) {
    fmt.Println("You should start broker with port number (only)")
    os.Exit(1)
  }

  listeningAddress := "127.0.0.1:" + os.Args[1]
  listener, err := net.Listen("tcp", listeningAddress)
  if (err != nil) {
    fmt.Println("Could not open a listen port: ", err.Error())
    os.Exit(1)
  }

  var queueList [brokerSize]queue.Queue
  fmt.Println("Waiting for connections...")
  for {
    newConnection, err := listener.Accept()
    if (err != nil) {
      fmt.Println("Could not open a socket: ", err.Error())
      continue
    }
    go connectionHandler(newConnection, &queueList)
  }
}

func connectionHandler(connection net.Conn, queueList *[brokerSize]queue.Queue) {
  clientAddress := connection.RemoteAddr().String()
  fmt.Println("Client " + clientAddress + " connected")
  readBuffer := make([]byte, 1024)

  for {
    size, err := connection.Read(readBuffer)
    if (err != nil) {
      fmt.Println("Error while reading from client: " + clientAddress)
      fmt.Println(err.Error())
      connection.Close()
      return
    }

    commandHandler(connection, queueList, strings.Split(string(readBuffer[:size]), "#"))
  }
}

func commandHandler(connection net.Conn, queueList *[brokerSize]queue.Queue, command []string) {
  switch {
  case command[0] == "create":
    if (queueListHead != brokerSize) {
      queueList[queueListHead] = queue.CreateQueue(command[1])
      queueListHead ++
      connection.Write([]byte("0x00"))
    } else {
      connection.Write([]byte("0x04"))
    }
  case command[0] == "enter":
    queueName := command[1]
    found := false
    success := false
    for i := range queueList {
      if (queueName == queueList[i].GetName()) {
        found = true
        success = queueList[i].Push(command[2])
        break
      }
    }
    if (!found) {
      connection.Write([]byte("0x01"))
    } else if (!success) {
      connection.Write([]byte("0x02"))
    } else {
      connection.Write([]byte("0x00"))
    }
  case command[0] == "pop":
    clientAddress := connection.RemoteAddr().String()
    sleepUntilMyTurn(clientAddress)

    queueName := command[1]
    found := false
    success := false
    var message string
    for i := range queueList {
      if(queueName == queueList[i].GetName()) {
        found = true
        message, success = queueList[i].Pop()
        break
      }
    }
    if (!found) {
      connection.Write([]byte("0x01"))
    } else if (!success) {
      connection.Write([]byte("0x03"))
    } else {
      connection.Write([]byte(message))
    }
  }
}

func sleepUntilMyTurn(clientAddress string) {
  for {
    if (userQueue.Push(clientAddress)) {
      break
    }
    time.Sleep(1 * time.Millisecond)
  }

  for {
    if (clientAddress == userQueue.GetFront()) {
      userQueue.Pop()
      break
    }
    time.Sleep(1 * time.Millisecond)
  }
}
