package openapi

import (
	"bytes"
	"fmt"
	"strings"
	"text/template"

	"github.com/echo8/krp/internal/config"
)

func Convert(cfgPath string) error {
	cfg, err := config.Load(cfgPath)
	if err != nil {
		return fmt.Errorf("failed to load configuration: %w", err)
	}
	syncPathTmpl, err := template.New("sync").Parse(specSyncPath)
	if err != nil {
		return err
	}
	asyncPathTmpl, err := template.New("async").Parse(specAsyncPath)
	if err != nil {
		return err
	}
	spec := specHeader
	for path, endpoint := range cfg.Endpoints {
		data := struct {
			Path        string
			OperationId string
		}{
			Path:        specPath(path),
			OperationId: specOperationId(path),
		}
		out := new(bytes.Buffer)
		if endpoint.Async {
			if err := asyncPathTmpl.Execute(out, data); err != nil {
				return err
			}
		} else {
			if err := syncPathTmpl.Execute(out, data); err != nil {
				return err
			}
		}
		spec = spec + out.String()
	}
	spec = spec + specSchemas
	fmt.Print(spec)
	return nil
}

func specPath(path config.EndpointPath) string {
	if strings.HasPrefix(string(path), "/") {
		return string(path)
	}
	return "/" + string(path)
}

func specOperationId(path config.EndpointPath) string {
	opId, _ := strings.CutPrefix(string(path), "/")
	return strings.ReplaceAll(opId, "/", "-")
}

var specHeader = `openapi: 3.1.0
info:
  title: KRP API
  description: KRP is a REST service that makes sending data to Kafka easy!
  version: 0.1.0
  contact:
    url: https://github.com/echo8/krp
  license:
    name: Apache 2.0
    url: https://www.apache.org/licenses/LICENSE-2.0.html
paths:
`
var specSyncPath = `  {{.Path}}:
    post:
      summary: Send messages
      description: Send messages to the {{.Path}} endpoint.
      operationId: send-{{.OperationId}}
      tags:
        - KRP
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Request'
      responses:
        '200':
          description: Send complete. See body for results.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Response'
        '400':
          description: Invalid request or data schema.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
        '500':
          description: An unexpected error occurred.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
`
var specAsyncPath = `  {{.Path}}:
    post:
      summary: Send messages
      description: Send messages to the {{.Path}} endpoint.
      operationId: send-{{.OperationId}}
      tags:
        - KRP
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/Request'
      responses:
        '204':
          description: Send complete.
        '400':
          description: Invalid request or data schema.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
        '500':
          description: An unexpected error occurred.
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
`
var specSchemas = `components:
  schemas:
    Request:
      type: object
      properties:
        messages:
          type: array
          items:
            $ref: '#/components/schemas/Message'
      required:
        - messages
    Message:
      type: object
      properties:
        key:
          $ref: '#/components/schemas/MessageData'
        value:
          $ref: '#/components/schemas/MessageData'
        headers:
          type: object
          additionalProperties:
            type: string
        timestamp:
          type: string
      required:
        - value
    MessageData:
      type: object
      properties:
        string:
          type: string
        bytes:
          type: string
        schemaRecordName:
          type: string
        schemaId:
          type: integer
        schemaMetadata:
          type: object
          additionalProperties:
            type: string
      oneOf:
        - required:
            - string
        - required:
            - bytes
    Response:
      type: object
      properties:
        results:
          type: array
          items:
            $ref: '#/components/schemas/Result'
    Result:
      type: object
      properties:
        success:
          type: boolean
    Error:
      type: object
      properties:
        error:
          type: string
`
