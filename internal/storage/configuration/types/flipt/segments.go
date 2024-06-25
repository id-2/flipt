package flipt

import (
	"context"
	"fmt"
	"os"
	"path"

	"github.com/go-git/go-billy/v5"
	"go.flipt.io/flipt/internal/ext"
	"go.flipt.io/flipt/internal/server/configuration"
	rpcconfig "go.flipt.io/flipt/rpc/configuration"
	"go.flipt.io/flipt/rpc/flipt/core"
	"google.golang.org/protobuf/types/known/anypb"
	"gopkg.in/yaml.v3"
)

type SegmentStorage struct{}

func (f *SegmentStorage) GetResource(ctx context.Context, fs billy.Filesystem, namespace string, key string) (*rpcconfig.Resource, error) {
	docs, err := parseNamespace(ctx, fs, namespace)
	if err != nil {
		return nil, err
	}

	for _, doc := range docs {
		if doc.Namespace != namespace {
			continue
		}

		for _, s := range doc.Segments {
			if s.Key == key {
				payload, err := payloadFromSegment(s)
				if err != nil {
					return nil, err
				}

				return &rpcconfig.Resource{
					Type:      payload.TypeUrl,
					Namespace: namespace,
					Key:       s.Key,
					Payload:   payload,
				}, nil
			}
		}
	}
	return nil, fmt.Errorf("flag %s/%s: %w", namespace, key, configuration.ErrNotFound)
}

func (f *SegmentStorage) ListResources(ctx context.Context, fs billy.Filesystem, namespace string) (rs []*rpcconfig.Resource, err error) {
	docs, err := parseNamespace(ctx, fs, namespace)
	if err != nil {
		return nil, err
	}

	for _, doc := range docs {
		if doc.Namespace != namespace {
			continue
		}

		for _, s := range doc.Segments {
			payload, err := payloadFromSegment(s)
			if err != nil {
				return nil, err
			}

			rs = append(rs, &rpcconfig.Resource{
				Type:      payload.TypeUrl,
				Namespace: namespace,
				Key:       s.Key,
				Payload:   payload,
			})
		}
	}

	return
}

func (f *SegmentStorage) PutResource(ctx context.Context, fs billy.Filesystem, rs *rpcconfig.Resource) error {
	segment, err := resourceToSegment(rs)
	if err != nil {
		return err
	}

	docs, err := parseNamespace(ctx, fs, rs.Namespace)
	if err != nil {
		return err
	}

	if len(docs) == 0 {
		docs = append(docs, &ext.Document{Namespace: rs.Namespace})
	}

	var found bool
	for _, doc := range docs {
		if doc.Namespace != rs.Namespace {
			continue
		}

		for i, s := range doc.Segments {
			if found = s.Key == string(segment.Key); found {
				doc.Segments[i] = segment
				break
			}
		}
	}

	if !found {
		docs[len(docs)-1].Segments = append(docs[len(docs)-1].Segments, segment)
	}

	fi, err := fs.OpenFile(path.Join(rs.Namespace, "features.yaml"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer fi.Close()

	enc := yaml.NewEncoder(fi)
	for _, doc := range docs {
		if err := enc.Encode(doc); err != nil {
			return err
		}
	}

	return nil
}

func (f *SegmentStorage) DeleteResource(ctx context.Context, fs billy.Filesystem, namespace string, key string) error {
	docs, err := parseNamespace(ctx, fs, namespace)
	if err != nil {
		return err
	}

	if len(docs) == 0 {
		return nil
	}

	var found bool
	for _, doc := range docs {
		if doc.Namespace != namespace {
			continue
		}

		for i, f := range doc.Segments {
			if found = f.Key == key; found {
				// remove entry from list
				doc.Segments = append(doc.Segments[:i], doc.Segments[i+1:]...)
			}
		}
	}

	// file contents remains unchanged
	if !found {
		return nil
	}

	fi, err := fs.OpenFile(path.Join(namespace, "features.yaml"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer fi.Close()

	enc := yaml.NewEncoder(fi)
	for _, doc := range docs {
		if err := enc.Encode(doc); err != nil {
			return err
		}
	}

	return nil
}

func payloadFromSegment(segment *ext.Segment) (*anypb.Any, error) {
	dst := &core.Segment{
		MatchType:   core.MatchType(core.MatchType_value[segment.MatchType]),
		Name:        segment.Name,
		Description: segment.Description,
	}

	for _, constraint := range segment.Constraints {
		dst.Constraints = append(dst.Constraints, &core.Constraint{
			Type:        core.ComparisonType(core.ComparisonType_value[constraint.Type]),
			Description: constraint.Description,
			Property:    constraint.Property,
			Operator:    constraint.Operator,
			Value:       constraint.Value,
		})
	}

	return newAny(dst)
}

func resourceToSegment(r *rpcconfig.Resource) (*ext.Segment, error) {
	var s core.Segment
	if err := r.Payload.UnmarshalTo(&s); err != nil {
		return nil, err
	}

	segment := &ext.Segment{
		MatchType:   s.MatchType.String(),
		Key:         r.Key,
		Name:        s.Name,
		Description: s.Description,
	}

	for _, constraint := range s.Constraints {
		c := &ext.Constraint{
			Type:        constraint.Type.String(),
			Description: constraint.Description,
			Property:    constraint.Property,
			Operator:    constraint.Operator,
			Value:       constraint.Value,
		}

		segment.Constraints = append(segment.Constraints, c)
	}

	return segment, nil
}
