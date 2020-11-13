package queue

const (
  capacity int = 2
)

type Queue struct {
  queueName string
  queueContent [capacity]string
  queueFront int
  queueRear int
  queueSize int
}

func (q Queue) isEmpty() bool {
  return q.queueSize == 0
}

func (q Queue) isFull() bool {
  return q.queueSize == capacity
}

func (q *Queue) Push(message string) bool {
  if (q.isFull()) {
    return false
  }
  q.queueContent[q.queueRear] = message
  q.queueRear = (q.queueRear + 1) % capacity
  q.queueSize ++
  return true
}

func (q *Queue) Pop() (string, bool) {
  if (q.isEmpty()) {
    return "", false
  }
  message := q.queueContent[q.queueFront]
  q.queueFront = (q.queueFront + 1) % capacity
  q.queueSize --
  return message, true
}

func (q Queue) GetName() string {
  return q.queueName
}

func CreateQueue(name string) Queue {
  return Queue{queueName: name, queueFront: 0, queueRear: 0, queueSize: 0}
}

func (q Queue) GetFront() string {
  return q.queueContent[q.queueFront]
}
