package main

import (
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/mr51m0n/gorc"
)

var accumulator int
var svc *sns.SNS
var gorc0 gorc.Gorc

func sendMessage() {
	defer gorc0.Dec() // decrease counter when finished

	// params will be sent to the publish call included here is the bare minimum params to send a message.
	params := &sns.PublishInput{
		Message:  aws.String("{\"channel\":\"buu\",\"name\":\"john\", \"msg\":\"doe\"}"), // This is the message itself (can be XML / JSON / Text - anything you want)
		TopicArn: aws.String("arn:aws:sns:us-west-2:523672979447:pub-seed"),              // Get this from the Topic in the AWS console.
	}

	_, err := svc.Publish(params) // Call to publish the message

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		log.Println(err.Error())
	}

	accumulator++

	fmt.Printf("Sent %d ", accumulator)
}

func init() {
	gorc0.Init()
}

func main() {
	accumulator = 0

	// Create a session object to talk to SNS (also make sure you have your key and secret setup in your .aws/credentials file)
	// Setup SNS
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	svc = sns.New(sess)

	// with gorc this time
	for i := 0; i < 100; i++ {
		gorc0.Inc() // increase either before invoking a goroutine or within it
		go sendMessage()
		gorc0.WaitLow(100) // no more then five goroutines governed by gorc0 are allowed at the same time
	}

	var input string
	fmt.Scanln(&input)
	fmt.Println("done")
}
