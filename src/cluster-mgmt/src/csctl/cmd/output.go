package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	pb "cerebras/pb/workflow/appliance/cluster_mgmt/csctl"
	csv1 "cerebras/pb/workflow/appliance/cluster_mgmt/csctl/v1"

	"github.com/spf13/cobra"
	"google.golang.org/protobuf/proto"
	"sigs.k8s.io/yaml"
)

type OutErr interface {
	Out() io.Writer
	Err() io.Writer
}

type CmdOutErr struct {
	cmd *cobra.Command
}

func (c CmdOutErr) Out() io.Writer {
	return c.cmd.OutOrStdout()
}

func (c CmdOutErr) Err() io.Writer {
	return c.cmd.ErrOrStderr()
}

type GenericMessage interface {
	GetContentType() pb.SerializationMethod
	GetRaw() []byte
}

type OutputMarshaller func(messages []GenericMessage, outErr OutErr, verbosity int) error

func TableMarshaller(messages []GenericMessage, outErr OutErr, _ int) error {
	table := &csv1.Table{}
	for idx, msg := range messages {
		if msg.GetContentType() != pb.SerializationMethod_PROTOBUF_METHOD {
			return fmt.Errorf("attempted to serialize non-protobuf table, msg idx %d", idx)
		}
		tableChunk := &csv1.Table{}
		err := proto.Unmarshal(msg.GetRaw(), tableChunk)
		if err != nil {
			return fmt.Errorf("unparsable response (idx %d) from server, %v", idx, err)
		}
		if table.Columns == nil {
			table.Columns = tableChunk.Columns
		} else if !reflect.DeepEqual(table.Columns, tableChunk.Columns) {
			return fmt.Errorf("table column definition of msg idx %d (%v) differ from 1st msg (%v)",
				idx, tableChunk.Columns, table.Columns)
		}
		table.Rows = append(table.Rows, tableChunk.GetRows()...)
	}
	return DisplayTable(table, nil, outErr)
}

func DisplayTable(table *csv1.Table, notes []string, outErr OutErr) error {
	colLengths := make([]int, len(table.Columns))
	var header []interface{}

	for i, val := range table.Columns {
		colLengths[i] = len(val.Name)
		header = append(header, strings.ToUpper(val.Name))
	}

	for _, row := range table.Rows {
		for i, val := range row.Cells {
			if len(val) > colLengths[i] {
				colLengths[i] = len(val)
			}
		}
	}

	fmts := ""
	for i, length := range colLengths {
		if i < len(colLengths)-1 {
			fmts += "%-" + fmt.Sprintf("%d", length+2) + "s"
		} else {
			fmts += "%s"
		}
	}

	lines := []string{fmt.Sprintf(fmts, header...)}
	for _, row := range table.Rows {
		// TODO: there a way around this extra copy for typing?
		var a []interface{}
		for _, ele := range row.Cells {
			a = append(a, ele)
		}
		lines = append(lines, fmt.Sprintf(fmts, a...))
	}

	for _, line := range append(lines, notes...) {
		if _, err := fmt.Fprintln(outErr.Out(), line); err != nil {
			return err
		}
	}
	return nil
}

type jsonObject map[string]interface{}

// maskField removes field specified by path to object. If it doesn't exist or the path
// isn't objects all throughout the path, then the mask is not applied.
func (j jsonObject) maskField(mask []string) {
	if len(mask) == 0 {
		return
	}
	key := mask[0]
	if ele, ok := j[key]; ok {
		if len(mask) == 1 {
			delete(j, key)
		} else {
			if obj, isObj := ele.(map[string]interface{}); isObj {
				jsonObject(obj).maskField(mask[1:])
			}
			// don't handle arrays, eventually might with syntax foo.*.
		}
	}
}

func (j jsonObject) applyFieldPriority(verbosity int) {
	// hack to support lists - decode `items` field as if entries were json objects containing meta.fieldPriorities
	if items, hasItems := j["items"]; hasItems {
		if arr, isArr := items.([]interface{}); isArr {
			for _, item := range arr {
				if o, isObj := item.(map[string]interface{}); isObj {
					jsonObject(o).applyFieldPriority(verbosity)
				}
			}
		}
		return
	}
	// apply field priorities
	if meta, hasMeta := j["meta"]; hasMeta {
		if m, isObj := meta.(map[string]interface{}); isObj {
			if fp, hasFP := m["fieldPriority"]; hasFP {
				if fpm, isFPM := fp.(map[string]interface{}); isFPM {
					for k, v := range fpm {
						if priority, isInt := v.(float64); isInt && int(priority) > verbosity {
							j.maskField(strings.Split(k, "."))
						}
					}
				}
			}
		}
	}
}

func JsonMarshaller(messages []GenericMessage, outErr OutErr, verbosity int) error {
	var allItems []any
	for idx, msg := range messages {
		if msg.GetContentType() != pb.SerializationMethod_JSON_METHOD {
			return fmt.Errorf(
				"content-type '%s' in msg (idx %d) is not json-serializable",
				msg.GetContentType().String(), idx)
		}

		var obj jsonObject
		if err := json.Unmarshal(msg.GetRaw(), &obj); err != nil {
			return fmt.Errorf("failed to deserialize response (idx %d) from server, %v", idx, err)
		}
		obj.applyFieldPriority(verbosity)

		if items, hasItems := obj["items"]; hasItems {
			// append to accumulator
			if arr, isArr := items.([]interface{}); isArr {
				allItems = append(allItems, arr...)
			}
		} else {
			// This is a single resource from a get response. Do not accumulate, directly marshall and return it
			if idx != 0 || len(messages) > 1 {
				return fmt.Errorf("failed to find 'items' array from server list responses (idx %d, total %d)",
					idx, len(messages))
			}
			maskedJson, err := json.MarshalIndent(&obj, "", "  ")
			if err != nil {
				return fmt.Errorf("failed to re-serialize response from server, %v", err)
			}

			_, err = fmt.Fprintln(outErr.Out(), string(maskedJson))
			return err
		}
	}
	merged := jsonObject{"items": allItems}
	maskedJson, err := json.MarshalIndent(&merged, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to re-serialize response from server, %v", err)
	}
	_, err = fmt.Fprintln(outErr.Out(), string(maskedJson))
	return err
}

func YamlMarshaller(messages []GenericMessage, outErr OutErr, verbosity int) error {
	var allItems []any
	for idx, msg := range messages {
		if msg.GetContentType() != pb.SerializationMethod_JSON_METHOD {
			return fmt.Errorf(
				"content-type '%s' in msg (idx %d) is not json-serializable",
				msg.GetContentType().String(), idx)
		}

		var obj jsonObject
		if err := json.Unmarshal(msg.GetRaw(), &obj); err != nil {
			return fmt.Errorf("failed to deserialize response (idx %d) from server, %v", idx, err)
		}
		obj.applyFieldPriority(verbosity)

		if items, hasItems := obj["items"]; hasItems {
			// append to accumulator
			if arr, isArr := items.([]interface{}); isArr {
				allItems = append(allItems, arr...)
			}
		} else {
			// This is a single resource from a get response. Do not accumulate, directly marshall and return it
			if idx != 0 || len(messages) > 1 {
				return fmt.Errorf("failed to find 'items' array from server list responses (idx %d, total %d)",
					idx, len(messages))
			}
			yamlOut, err := yaml.Marshal(obj)
			if err != nil {
				return fmt.Errorf("failed to format response from server, %v", err)
			}
			_, err = fmt.Fprintln(outErr.Out(), string(yamlOut))
			return err
		}
	}
	merged := jsonObject{"items": allItems}
	yamlOut, err := yaml.Marshal(merged)
	if err != nil {
		return fmt.Errorf("failed to format response from server, %v", err)
	}
	_, err = fmt.Fprintln(outErr.Out(), string(yamlOut))
	return err
}

func AsJson(response proto.Message, outErr OutErr) error {
	res, err := protojson.MarshalOptions{EmitUnpopulated: true, Multiline: true, Indent: "  "}.Marshal(response)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(outErr.Out(), string(res))
	return err
}

func AsYaml(response proto.Message, outErr OutErr) error {
	res, err := protojson.MarshalOptions{EmitUnpopulated: true, Multiline: true, Indent: "  "}.Marshal(response)
	if err != nil {
		return err
	}
	res, err = yaml.JSONToYAML(res)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(outErr.Out(), string(res))
	return err
}

func arrayCol(vals []string, length int) string {
	rv := strings.Join(vals, ",")
	if len(rv) > length {
		rv = rv[0:length-3] + "..."
	}
	return rv
}

func arrayColWithCount(vals []string, length int, wideMode bool) string {
	if !wideMode {
		return fmt.Sprintf("%d", len(vals))
	}
	sort.Strings(vals)
	rv := strings.Join(vals, ",")
	rv = fmt.Sprintf("(%d) %s", len(vals), rv)
	if len(rv) > length {
		rv = rv[0:length-3] + "..."
	}
	return rv
}

var outputHandler map[string]OutputMarshaller

func init() {
	outputHandler = map[string]OutputMarshaller{}
	outputHandler["table"] = TableMarshaller
	outputHandler["json"] = JsonMarshaller
	outputHandler["yaml"] = YamlMarshaller
}
