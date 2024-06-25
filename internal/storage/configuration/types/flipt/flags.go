package flipt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/go-git/go-billy/v5"
	"go.flipt.io/flipt/internal/ext"
	"go.flipt.io/flipt/internal/server/configuration"
	rpcconfig "go.flipt.io/flipt/rpc/configuration"
	"go.flipt.io/flipt/rpc/flipt/core"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/structpb"
	"gopkg.in/yaml.v3"
)

type FlagStorage struct{}

func (f *FlagStorage) GetResource(ctx context.Context, fs billy.Filesystem, namespace string, key string) (*rpcconfig.Resource, error) {
	docs, err := parseNamespace(ctx, fs, namespace)
	if err != nil {
		return nil, err
	}

	for _, doc := range docs {
		if doc.Namespace != namespace {
			continue
		}

		for _, f := range doc.Flags {
			if f.Key == key {
				payload, err := payloadFromFlag(f)
				if err != nil {
					return nil, err
				}

				return &rpcconfig.Resource{
					Type:      payload.TypeUrl,
					Namespace: namespace,
					Key:       f.Key,
					Payload:   payload,
				}, nil
			}
		}
	}
	return nil, fmt.Errorf("flag %s/%s: %w", namespace, key, configuration.ErrNotFound)
}

func (f *FlagStorage) ListResources(ctx context.Context, fs billy.Filesystem, namespace string) (rs []*rpcconfig.Resource, err error) {
	docs, err := parseNamespace(ctx, fs, namespace)
	if err != nil {
		return nil, err
	}

	for _, doc := range docs {
		if doc.Namespace != namespace {
			continue
		}

		for _, f := range doc.Flags {
			payload, err := payloadFromFlag(f)
			if err != nil {
				return nil, err
			}

			rs = append(rs, &rpcconfig.Resource{
				Type:      payload.TypeUrl,
				Namespace: namespace,
				Key:       f.Key,
				Payload:   payload,
			})
		}
	}

	return
}

func (f *FlagStorage) PutResource(ctx context.Context, fs billy.Filesystem, rs *rpcconfig.Resource) error {
	flag, err := resourceToFlag(rs)
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

		for i, f := range doc.Flags {
			if found = f.Key == string(flag.Key); found {
				doc.Flags[i] = flag
				break
			}
		}
	}

	if !found {
		docs[len(docs)-1].Flags = append(docs[len(docs)-1].Flags, flag)
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

func (f *FlagStorage) DeleteResource(ctx context.Context, fs billy.Filesystem, namespace string, key string) error {
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

		for i, f := range doc.Flags {
			if found = f.Key == key; found {
				// remove entry from list
				doc.Flags = append(doc.Flags[:i], doc.Flags[i+1:]...)
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

func payloadFromFlag(flag *ext.Flag) (*anypb.Any, error) {
	dst := &core.Flag{
		Name:        flag.Name,
		Type:        core.FlagType(core.FlagType_value[flag.Type]),
		Description: flag.Description,
		Enabled:     flag.Enabled,
	}

	for _, variant := range flag.Variants {
		attach, err := structpb.NewStruct(variant.Attachment.(map[string]any))
		if err != nil {
			return nil, err
		}

		dst.Variants = append(dst.Variants, &core.Variant{
			Key:         variant.Key,
			Name:        variant.Name,
			Description: variant.Description,
			Attachment:  attach,
		})
	}

	for _, rule := range flag.Rules {
		r := &core.Rule{}

		switch s := rule.Segment.IsSegment.(type) {
		case *ext.Segments:
			r.SegmentOperator = core.SegmentOperator(core.SegmentOperator_value[s.SegmentOperator])
			r.Segments = s.Keys
		case *ext.SegmentKey:
			r.Segments = []string{string(*s)}
		}

		for _, dist := range rule.Distributions {
			r.Distributions = append(r.Distributions, &core.Distribution{
				Rollout: dist.Rollout,
				Variant: dist.VariantKey,
			})
		}
		dst.Rules = append(dst.Rules, r)
	}

	for _, rollout := range flag.Rollouts {
		r := &core.Rollout{
			Description: rollout.Description,
		}

		if rollout.Segment != nil {
			r.Type = core.RolloutType_SEGMENT_ROLLOUT_TYPE
			r.Rule = &core.Rollout_Segment{
				Segment: &core.RolloutSegment{
					SegmentOperator: core.SegmentOperator(core.SegmentOperator_value[rollout.Segment.Operator]),
					Segments:        rollout.Segment.Keys,
					Value:           rollout.Segment.Value,
				},
			}
		} else if rollout.Threshold != nil {
			r.Type = core.RolloutType_THRESHOLD_ROLLOUT_TYPE
			r.Rule = &core.Rollout_Threshold{
				Threshold: &core.RolloutThreshold{
					Percentage: rollout.Threshold.Percentage,
					Value:      rollout.Threshold.Value,
				},
			}
		} else {
			return nil, fmt.Errorf("unknown rollout type")
		}

		dst.Rollouts = append(dst.Rollouts, r)
	}
	return anypb.New(dst)
}

func resourceToFlag(r *rpcconfig.Resource) (*ext.Flag, error) {
	var f core.Flag
	if err := r.Payload.UnmarshalTo(&f); err != nil {
		return nil, err
	}

	flag := &ext.Flag{
		Key:         r.Key,
		Name:        f.Name,
		Description: f.Description,
		Enabled:     f.Enabled,
	}

	for _, variant := range f.Variants {
		v := &ext.Variant{
			Key:         variant.Key,
			Name:        variant.Name,
			Description: variant.Description,
			Attachment:  variant.Attachment.AsMap(),
		}

		flag.Variants = append(flag.Variants, v)
	}

	for _, rule := range f.Rules {
		r := &ext.Rule{
			Segment: &ext.SegmentEmbed{
				IsSegment: &ext.Segments{
					Keys:            rule.Segments,
					SegmentOperator: rule.SegmentOperator.String(),
				},
			},
		}

		for _, dist := range rule.Distributions {
			r.Distributions = append(r.Distributions, &ext.Distribution{
				Rollout:    dist.Rollout,
				VariantKey: dist.Variant,
			})
		}

		flag.Rules = append(flag.Rules, r)
	}

	for _, rollout := range f.Rollouts {
		r := &ext.Rollout{
			Description: rollout.Description,
		}

		switch rollout.Type {
		case core.RolloutType_SEGMENT_ROLLOUT_TYPE:
			r.Segment = &ext.SegmentRule{
				Keys:     rollout.GetSegment().Segments,
				Operator: rollout.GetSegment().SegmentOperator.String(),
				Value:    rollout.GetSegment().Value,
			}
		case core.RolloutType_THRESHOLD_ROLLOUT_TYPE:
			r.Threshold = &ext.ThresholdRule{
				Percentage: rollout.GetThreshold().Percentage,
				Value:      rollout.GetThreshold().Value,
			}
		default:
			return nil, fmt.Errorf("unexpected rollout type: %q", rollout.Type)
		}

		flag.Rollouts = append(flag.Rollouts, r)
	}

	return flag, nil
}

func parseNamespace(_ context.Context, fs billy.Basic, namespace string) (docs []*ext.Document, err error) {
	fi, err := fs.Open(path.Join(namespace, "features.yaml"))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}

		return nil, err
	}
	defer fi.Close()

	decoder := yaml.NewDecoder(fi)
	for {
		doc := &ext.Document{}
		if err = decoder.Decode(doc); err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
				break
			}

			return nil, err
		}

		// set namespace to default if empty in document
		if doc.Namespace == "" {
			doc.Namespace = "default"
		}

		docs = append(docs, doc)
	}

	return
}
