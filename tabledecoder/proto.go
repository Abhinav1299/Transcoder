package tabledecoder

import (
	_ "embed"
	"fmt"
	"strings"
	"sync"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

//go:embed descriptors.binpb
var descriptorSetBytes []byte

var (
	resolverOnce sync.Once
	resolver     *protoResolver
	resolverErr  error
)

type protoResolver struct {
	types *dynamicpb.Types
	msgs  map[protoreflect.FullName]protoreflect.MessageDescriptor
}

func getResolver() (*protoResolver, error) {
	resolverOnce.Do(func() {
		fds := &descriptorpb.FileDescriptorSet{}
		if err := proto.Unmarshal(descriptorSetBytes, fds); err != nil {
			resolverErr = fmt.Errorf("unmarshaling descriptor set: %w", err)
			return
		}

		files, err := protodesc.NewFiles(fds)
		if err != nil {
			resolverErr = fmt.Errorf("building file registry: %w", err)
			return
		}

		types := dynamicpb.NewTypes(files)

		msgs := make(map[protoreflect.FullName]protoreflect.MessageDescriptor)
		files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
			m := fd.Messages()
			for i := 0; i < m.Len(); i++ {
				collectMessages(msgs, m.Get(i))
			}
			return true
		})

		resolver = &protoResolver{types: types, msgs: msgs}
	})
	return resolver, resolverErr
}

func collectMessages(out map[protoreflect.FullName]protoreflect.MessageDescriptor, md protoreflect.MessageDescriptor) {
	out[md.FullName()] = md
	nested := md.Messages()
	for i := 0; i < nested.Len(); i++ {
		collectMessages(out, nested.Get(i))
	}
}

func (r *protoResolver) findMessage(fullName string) (protoreflect.MessageDescriptor, error) {
	md, ok := r.msgs[protoreflect.FullName(fullName)]
	if !ok {
		return nil, fmt.Errorf("message type %q not found in descriptor set", fullName)
	}
	return md, nil
}

// FindMessageByName implements protoregistry.MessageTypeResolver (used by
// protojson to resolve google.protobuf.Any type URLs).
func (r *protoResolver) FindMessageByName(name protoreflect.FullName) (protoreflect.MessageType, error) {
	return r.types.FindMessageByName(name)
}

// FindMessageByURL implements protoregistry.MessageTypeResolver.
// Type URLs have the form "type.googleapis.com/<full_name>".
func (r *protoResolver) FindMessageByURL(url string) (protoreflect.MessageType, error) {
	return r.types.FindMessageByURL(url)
}

// FindExtensionByName implements protoregistry.ExtensionTypeResolver.
func (r *protoResolver) FindExtensionByName(field protoreflect.FullName) (protoreflect.ExtensionType, error) {
	return r.types.FindExtensionByName(field)
}

// FindExtensionByNumber implements protoregistry.ExtensionTypeResolver.
func (r *protoResolver) FindExtensionByNumber(message protoreflect.FullName, field protoreflect.FieldNumber) (protoreflect.ExtensionType, error) {
	return r.types.FindExtensionByNumber(message, field)
}

// MakeProtoColumnParser returns a ColumnParserFn that decodes a hex-encoded
// protobuf column into a JSON string using the given fully qualified proto
// message name. The proto schema is loaded from the embedded descriptor set.
//
// google.protobuf.Any fields are resolved using the same descriptor set, so
// nested types within the compiled schemas are decoded correctly.
func MakeProtoColumnParser(fullName string) ColumnParserFn {
	return func(s string) (string, error) {
		b, ok := interpretString(s)
		if !ok {
			return "", fmt.Errorf("failed to interpret proto column value: %.40s", s)
		}

		r, err := getResolver()
		if err != nil {
			return "", fmt.Errorf("loading proto descriptors: %w", err)
		}

		md, err := r.findMessage(fullName)
		if err != nil {
			return "", err
		}

		msg := dynamicpb.NewMessage(md)
		if err := proto.Unmarshal(b, msg); err != nil {
			return "", fmt.Errorf("unmarshaling %s: %w", fullName, err)
		}

		marshaler := protojson.MarshalOptions{
			UseProtoNames:   true,
			EmitUnpopulated: false,
			Resolver:        r,
		}

		jsonBytes, err := marshaler.Marshal(msg)
		if err != nil {
			// If JSON marshaling fails (e.g. unresolvable Any types), fall back
			// to a textual representation showing the raw field data.
			return marshalFallback(msg, fullName), nil
		}

		return string(jsonBytes), nil
	}
}

// marshalFallback produces a best-effort JSON representation when protojson
// fails (typically because a google.protobuf.Any wraps a type not in our
// descriptor set). It re-marshals with EmitUnpopulated disabled.
func marshalFallback(msg proto.Message, typeName string) string {
	fallback := protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: false,
	}
	b, err := fallback.Marshal(msg)
	if err != nil {
		return fmt.Sprintf("[%s: decode error: %v]", strings.TrimPrefix(typeName, "cockroach."), err)
	}
	return string(b)
}
