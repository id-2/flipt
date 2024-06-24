package configuration

import (
	"context"
	"errors"
	"fmt"

	"go.flipt.io/flipt/rpc/configuration"
)

type Server struct {
	source Source

	configuration.UnimplementedConfigurationServiceServer
}

func NewServer(source Source) *Server {
	return &Server{source: source}
}

func (s *Server) GetNamespace(ctx context.Context, meta *configuration.NamespaceMeta) (ns *configuration.Namespace, err error) {
	return ns, s.source.View(ctx, func(ctx context.Context, sv StoreView) error {
		ns, err = sv.GetNamespace(ctx, meta.Key)
		return err
	}, WithReference(meta.Reference))
}

func (s *Server) ListNamespaces(ctx context.Context, r *configuration.ListNamespacesRequest) (nl *configuration.NamespaceList, err error) {
	return nl, s.source.View(ctx, func(ctx context.Context, sv StoreView) error {
		nl, err = sv.ListNamespaces(ctx)
		return err
	}, WithReference(r.Reference))
}

func (s *Server) CreateNamespace(ctx context.Context, ns *configuration.Namespace) (*configuration.UpdateNamespaceResponse, error) {
	resp := &configuration.UpdateNamespaceResponse{}
	if err := s.source.Update(ctx, func(ctx context.Context, sv Store) error {
		_, err := sv.GetNamespace(ctx, ns.Key)
		if err == nil {
			return fmt.Errorf("namespace %q: %w", ns.Key, ErrAlreadyExists)
		}

		if !errors.Is(err, ErrNotFound) {
			return err
		}

		return sv.PutNamespace(ctx, ns)
	}, WithReference(ns.Reference)); err != nil {
		return nil, err
	}

	resp.Metadata = &configuration.NamespaceMeta{
		Key:       ns.Key,
		Reference: ns.Reference,
	}

	return resp, nil
}

func (s *Server) UpdateNamespace(ctx context.Context, ns *configuration.Namespace) (*configuration.UpdateNamespaceResponse, error) {
	resp := &configuration.UpdateNamespaceResponse{}
	if err := s.source.Update(ctx, func(ctx context.Context, sv Store) error {
		_, err := sv.GetNamespace(ctx, ns.Key)
		if err != nil {
			return err
		}

		return sv.PutNamespace(ctx, ns)
	}, WithReference(ns.Reference)); err != nil {
		return nil, err
	}

	resp.Metadata = &configuration.NamespaceMeta{
		Key:       ns.Key,
		Reference: ns.Reference,
	}

	return resp, nil
}

func (s *Server) DeleteNamespace(ctx context.Context, meta *configuration.NamespaceMeta) (*configuration.UpdateNamespaceResponse, error) {
	resp := &configuration.UpdateNamespaceResponse{Metadata: meta}
	if err := s.source.Update(ctx, func(ctx context.Context, sv Store) error {
		return sv.DeleteNamespace(ctx, meta.Key)
	}, WithReference(meta.Reference)); err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *Server) GetResource(ctx context.Context, meta *configuration.ResourceMeta) (r *configuration.Resource, err error) {
	return r, s.source.View(ctx, func(ctx context.Context, sv StoreView) (err error) {
		r, err = sv.GetResource(ctx, meta.Type, meta.Namespace, meta.Key)
		if err != nil {
			return err
		}
		return err
	}, WithReference(meta.Reference))
}

func (s *Server) ListResources(ctx context.Context, r *configuration.ListResourcesRequest) (*configuration.ResourceList, error) {
	rl := &configuration.ResourceList{
		Reference: r.Reference,
	}

	if err := s.source.View(ctx, func(ctx context.Context, sv StoreView) (err error) {
		rl.Resources, err = sv.ListResources(ctx, r.Type, r.Namespace)
		if err != nil {
			return err
		}

		return err
	}, WithReference(r.Reference)); err != nil {
		return nil, err
	}

	return rl, nil
}

func (s *Server) CreateResource(ctx context.Context, r *configuration.Resource) (*configuration.UpdateResourceResponse, error) {
	resp := &configuration.UpdateResourceResponse{}
	if err := s.source.Update(ctx, func(ctx context.Context, sv Store) error {
		_, err := sv.GetResource(ctx, r.Type, r.Namespace, r.Key)
		if err == nil {
			return fmt.Errorf(`resource %s/%s/%s: %w`, r.Type, r.Namespace, r.Key, ErrAlreadyExists)
		}

		if !errors.Is(err, ErrNotFound) {
			return err
		}

		return sv.PutResource(ctx, r)
	}, WithReference(r.Reference)); err != nil {
		return nil, err
	}

	resp.Metadata = &configuration.ResourceMeta{
		Type:      r.Type,
		Namespace: r.Namespace,
		Key:       r.Key,
		Reference: r.Reference,
	}

	return resp, nil
}

func (s *Server) UpdateResource(ctx context.Context, r *configuration.Resource) (*configuration.UpdateResourceResponse, error) {
	resp := &configuration.UpdateResourceResponse{}
	if err := s.source.Update(ctx, func(ctx context.Context, sv Store) error {
		_, err := sv.GetResource(ctx, r.Type, r.Namespace, r.Key)
		if err != nil {
			return err
		}

		return sv.PutResource(ctx, r)
	}, WithReference(r.Reference)); err != nil {
		return nil, err
	}

	resp.Metadata = &configuration.ResourceMeta{
		Type:      r.Type,
		Namespace: r.Namespace,
		Key:       r.Key,
		Reference: r.Reference,
	}

	return resp, nil
}

func (s *Server) DeleteResource(ctx context.Context, meta *configuration.ResourceMeta) (*configuration.UpdateResourceResponse, error) {
	resp := &configuration.UpdateResourceResponse{Metadata: meta}
	if err := s.source.Update(ctx, func(ctx context.Context, sv Store) error {
		return sv.DeleteResource(ctx, meta.Type, meta.Namespace, meta.Key)
	}, WithReference(meta.Reference)); err != nil {
		return nil, err
	}

	return resp, nil
}
