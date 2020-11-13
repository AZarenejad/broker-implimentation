package main

import (
  "fmt"
  "net"
  "os"
  "bufio"
  "strings"
  "time"
  "runtime"
)

const (
  synchronous bool = true
)

func main() {
  if (len(os.Args) != 2) {
    fmt.Println("You should start master with broker port number (only)")
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
    fmt.Println("Enter createQueue to create a queue\n" +
      "Enter eQueue to enter a value into a queue\nEnter deQueue to pop a message\n")

    command, _ := reader.ReadString('\n')
    if runtime.GOOS == "windows" {
      command = strings.TrimRight(command, "\r\n")
    } else {
      command = strings.TrimRight(command, "\n")
	}
	if (command == "deQueue") {
    deQueue(connection)
	  }else if (command == "createQueue") {
      createQueue(connection)
    } else if (command == "eQueue") {
      enterQueue(connection)
    } else {
      fmt.Println("Invalid input. Try again\n")
    }
  }
}

func createQueue(connection net.Conn) {
  reader := bufio.NewReader(os.Stdin)
  readBuffer := make([]byte, 1024)
  fmt.Print("Please enter queue name: ")
  queueName, _ := reader.ReadString('\n')
  if runtime.GOOS == "windows" {
    queueName = strings.TrimRight(queueName, "\r\n")
  } else {
    queueName = strings.TrimRight(queueName, "\n")
  }
  connection.Write([]byte("create#" + queueName))
  if (synchronous) {
    size, err := connection.Read(readBuffer)
    if (err != nil) {
      fmt.Println("Error while waiting for broker response")
      fmt.Println(err.Error())
      os.Exit(1)
    }
    response := string(readBuffer[:size])
    if (response == "0x04") {
      fmt.Println("Broker has reached queue limit\n")
    } else if (response == "0x00") {
      fmt.Println(queueName + " was added successfully\n")
    }
  }
}

func enterQueue(connection net.Conn) {
  reader := bufio.NewReader(os.Stdin)
  readBuffer := make([]byte, 1024)
  fmt.Print("Please enter the queue name: ")
  queueName, _ := reader.ReadString('\n')
  if runtime.GOOS == "windows" {
    queueName = strings.TrimRight(queueName, "\r\n")
  } else {
    queueName = strings.TrimRight(queueName, "\n")
  }
  fmt.Print("Please enter the message: ")
  message, _ := reader.ReadString('\n')
  if runtime.GOOS == "windows" {
    message = strings.TrimRight(message, "\r\n")
  } else {
    message = strings.TrimRight(message, "\n")
  }
  connection.Write([]byte("enter#" + queueName + "#" + message))
  if (synchronous) {
    size, err := connection.Read(readBuffer)
    if (err != nil) {
      fmt.Println("Error while waiting for broker response")
      fmt.Println(err.Error())
      os.Exit(1)
    }
    response := string(readBuffer[:size])
    if (response == "0x01") {
      fmt.Println(queueName + " not found.\n")
    } else if (response == "0x02"){
      for {
        fmt.Println(queueName + " is full. Trying again in 2 secs.\n")
        time.Sleep(2 * time.Second)
        connection.Write([]byte("enter#" + queueName + "#" + message))
        size, err := connection.Read(readBuffer)
        if (err != nil) {
          fmt.Println("Error while waiting for broker response")
          fmt.Println(err.Error())
          os.Exit(1)
      }
      response = string(readBuffer[:size])
      if (response == "0x00") {
        fmt.Println("Message successfully added to " + queueName + "\n")
        break
      }
    }
  } else {
    fmt.Println("Message successfully added to " + queueName + "\n")
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
