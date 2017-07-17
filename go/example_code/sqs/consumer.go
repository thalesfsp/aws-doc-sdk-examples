/*
 * Author: Thales Pinheiro <thales@rets.ly>
 * Since: 07/2017
 *
 * Consumes messages (envelopes) from the queue
 *
 * TODO:
 * - Add CLI
 * - Add filter feature
 */

package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	. "github.com/tj/go-debug"
)

// Global vars

var (
	countReceived = 0
	countDeleted  = 0
	countBytes    = int64(0)

	awsRegion  = "us-west-2"
	awsProfile = "bulkd"

	lock sync.Mutex

	debug = Debug("consumer")

	queue *sqs.SQS
	qURL  = "https://sqs.us-west-2.amazonaws.com/523672979447/remine-pub-seed"

	directoryToStorePayload = "messages"

	interval = 1
)

// Data structures

// Payload represents the content of one message
type Payload struct {
	Channel string `json:"channel"`
	Name    string `json:"name"`
	Msg     string `json:"msg"`
}

// Message represents one message from the envelope
type Message struct {
	Type             string    `json:"Type"`
	MessageID        string    `json:"MessageId"`
	TopicArn         string    `json:"TopicArn"`
	Message          string    `json:"Message"`
	Timestamp        time.Time `json:"Timestamp"`
	SignatureVersion string    `json:"SignatureVersion"`
	Signature        string    `json:"Signature"`
	SigningCertURL   string    `json:"SigningCertURL"`
	UnsubscribeURL   string    `json:"UnsubscribeURL"`
}

// Helpers

// Setup AWS
func setupAWS() {
	// Setup SNS
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config:            aws.Config{Region: aws.String(awsRegion)},
		Profile:           awsProfile,
		SharedConfigState: session.SharedConfigEnable,
	}))

	queue = sqs.New(sess)

	debug("AWS configurated! Connecting to queue %s @ %s using %s as profile ", qURL, awsRegion, awsProfile)
}

// Create the specified directory
//   Path can be relative (will be converted to absolute) or absolute
func mkdirp(path string) string {
	absolutePath, absError := filepath.Abs(directoryToStorePayload)

	if absError != nil {
		log.Fatalln("Failed to get absolute path of "+path+":", absError.Error())
	}

	// Create the directory to store the payload of the processed messages
	if mkdirAllError := os.MkdirAll(absolutePath, os.ModePerm); mkdirAllError != nil {
		log.Fatalln("Error creating directory: "+absolutePath+":", mkdirAllError.Error())
	}

	debug("Using %s directory as storage. Created if don't existed!", absolutePath)

	return absolutePath
}

// Marshal is a function that marshals the object into an io.Reader.
//   By default, it uses the JSON marshaller.
func Marshal(v interface{}) io.Reader {
	marshalledInterface, marshalIndentError := json.MarshalIndent(v, "", "\t")

	if marshalIndentError != nil {
		log.Fatalln("Error marshalling interface:", v, marshalIndentError.Error())
	}

	return bytes.NewReader(marshalledInterface)
}

// Save saves a representation of v to the file at path.
func writeToDisk(path string, v interface{}) string {
	lock.Lock()
	defer lock.Unlock()

	file, createError := os.Create(path)

	// Validate error
	if createError != nil {
		log.Fatalln("Error creating file:", createError.Error())
	}

	defer file.Close()

	writtenBytes, copyError := io.Copy(file, Marshal(v))

	// Validate error
	if copyError != nil {
		log.Fatalln("Error creating file:", copyError.Error())
	}

	// Sum up digested bytes
	countBytes = countBytes + writtenBytes

	return bytefmt.ByteSize(uint64(writtenBytes))
}

// Converts any JSON to any structured data
func convertJSONToStruct(input string, dataStructure interface{}) {
	// Conversion
	unmarshalError := json.Unmarshal([]byte(input), dataStructure)

	// Validate error
	if unmarshalError != nil {
		log.Fatalln("Failed to convert the body of the message from JSON to struct. "+
			"Is it a valid JSON?", unmarshalError.Error())
	}
}

// Delete messages from the queue
func deleteMessage(receiptHandle *string) {
	// Delete message
	_, deleteMIError := queue.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      &qURL,
		ReceiptHandle: receiptHandle,
	})

	// Validate error
	if deleteMIError != nil {
		log.Fatalln("Error while trying to delete message:", deleteMIError.Error())
	}
}

// Get messages from the queue
//   @TODO extract the processing part to another function
func getMessage() {
	// Get messages
	receivedMI, receivedMIError := queue.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
			aws.String(sqs.MessageSystemAttributeNameSequenceNumber),
			aws.String(sqs.MessageSystemAttributeNameSenderId),
		},
		MessageAttributeNames: []*string{
			aws.String(sqs.QueueAttributeNameAll),
		},
		QueueUrl:            &qURL,
		MaxNumberOfMessages: aws.Int64(1),
		VisibilityTimeout:   aws.Int64(0),
		WaitTimeSeconds:     aws.Int64(0),
	})

	// Validate error
	if receivedMIError != nil {
		log.Fatalln("Error while trying to get message:", receivedMIError.Error())
	}

	// Check if there's messages
	if len(receivedMI.Messages) != 0 { // Yes
		// Increment received messages
		countReceived++

		// Iterate over messages sent
		//   Note that SNS sent only one message per publish
		for _, receivedMessage := range receivedMI.Messages {
			var message Message
			convertJSONToStruct(*receivedMessage.Body, &message)

			// Get the payload
			//   Payload can contain one or more documents
			var payload Payload
			convertJSONToStruct(message.Message, &payload)

			// Do something with the payload
			filename := message.MessageID + ".json"
			bytesWritten := writeToDisk(filepath.Join(directoryToStorePayload, filename), payload)

			// Delete message, so the queue don't grow
			deleteMessage(receivedMI.Messages[0].ReceiptHandle)

			// Increment deleted messages
			//   Note that at this point the number of received and delete messages should match
			countDeleted++

			// Notifies to stdout
			debug("Envelope %s received | Contains %d message(s) | Total received: %d | Total deleted: %d | Saved %s as %s | Total amount processed: %s",
				string(*receivedMessage.MessageId),
				len(receivedMI.Messages),
				countReceived,
				countDeleted,
				bytesWritten,
				filename,
				bytefmt.ByteSize(uint64(countBytes)),
			)
		}
	}
}

// Starts here

// Tear up:
//
// - Setup AWS
// - Create directory to store processed envelopes
func init() {
	setupAWS()
	mkdirp(directoryToStorePayload)
}

// Entrypoint
//   Where eveything starts, and happens
func main() {
	// Set interval pattern
	t := time.NewTicker(time.Duration(interval) * time.Millisecond)

	for {
		// Read a new message every 10ms
		getMessage()
		<-t.C
	}
}
