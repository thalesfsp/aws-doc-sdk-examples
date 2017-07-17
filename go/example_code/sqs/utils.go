/*
Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.

This file is licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License. A copy of
the License is located at

http://aws.amazon.com/apache2.0/

This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
*/

package main

// ConvertJSONToStruct converts JSON to strucutred data
func ConvertJSONToStruct(input string, dataStructure interface) {
	// Convert from bytes (JSON) to struct
	err := json.Unmarshal([]byte(input), &dataStructure)

	if err != nil {
		log.Fatal(`
			Failed to convert the body of the message from JSON to struct.
			Is it a valid JSON?
		`, err.Error())
	}
}
