package main

import (
  "fmt"
  "net"
  "os"
  "bufio"
  "strings"
  "runtime"
)

func main() {
  if (len(os.Args) != 2) {
    fmt.Println("You should start client with broker port number (only)")
    os.Exit(1)
  }

  brokerAddress := "127.0.0.1:" + os.Args[1]
  connection, err := net.Dial("tcp", brokerAddress)
  if (err != nil) {
    fmt.Println("Error while trying to connect: ", err.Error())
    os.Exit(1)
  }

  fmt.Println("Connected!")
  reader := bufio.NewReader(os.Stdin)
  for {
    fmt.Println("Enter deQueue to obtain a value\n")

    command, _ := reader.ReadString('\n')
    if runtime.GOOS == "windows" {
      command = strings.TrimRight(command, "\r\n")
    } else {
      command = strings.TrimRight(command, "\n")
    }
    if (command == "deQueue") {
      deQueue(connection)
    } else {
      fmt.Println("Invalid input. Try again")
    }
  }
}

func deQueue(connection net.Conn) {
  readBuffer := make([]byte, 1024)
  reader := bufio.NewReader(os.Stdin)
  fmt.Print("Please enter the queue name: ")
  queueName, _ := reader.ReadString('\n')
  if runtime.GOOS == "windows" {
    queueName = strings.TrimRight(queueName, "\r\n")
  } else {
    queueName = strings.TrimRight(queueName, "\n")
  }
  connection.Write([]byte("pop#" + queueName))
  size, err := connection.Read(readBuffer)
  if (err != nil) {
    fmt.Println("Error while reading")
    fmt.Println(err.Error())
    connection.Close()
    os.Exit(1)
  }
  message := string(readBuffer[:size])
  if (message == "0x01") {
    fmt.Println("Queue not found\n")
  } else if (message == "0x03") {
    fmt.Println("Queue was empty\n")
  } else {
    fmt.Println(message + "\n")
  }
}
