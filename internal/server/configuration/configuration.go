package configuration

import (
	"context"
	"errors"
	"fmt"

	"go.flipt.io/flipt/rpc/configuration"
)

var _ configuration.ConfigurationServiceServer = (*Server)(nil)

type Server struct {
	source Source

	configuration.UnimplementedConfigurationServiceServer
}

func NewServer(source Source) *Server {
	return &Server{source: source}
}

func (s *Server) GetNamespace(ctx context.Context, req *configuration.GetNamespaceRequest) (ns *configuration.NamespaceResponse, err error) {
	return s.source.GetNamespace(ctx, req.Key)
}

func (s *Server) ListNamespaces(ctx context.Context, r *configuration.ListNamespacesRequest) (nl *configuration.ListNamespacesResponse, err error) {
	return s.source.ListNamespaces(ctx)
}

func (s *Server) CreateNamespace(ctx context.Context, ns *configuration.UpdateNamespaceRequest) (*configuration.NamespaceResponse, error) {
	_, err := s.source.GetNamespace(ctx, ns.Key)
	if err == nil {
		return nil, fmt.Errorf("create namespace %q: %w", ns.Key, ErrAlreadyExists)
	}

	if !errors.Is(err, ErrNotFound) {
		return nil, err
	}

	resp := &configuration.NamespaceResponse{
		Namespace: &configuration.Namespace{
			Key:         ns.Key,
			Name:        ns.Name,
			Description: ns.Description,
			Protected:   ns.Protected,
		},
	}

	resp.Revision, err = s.source.PutNamespace(ctx, ns.GetRevision(), resp.Namespace)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *Server) UpdateNamespace(ctx context.Context, ns *configuration.UpdateNamespaceRequest) (*configuration.NamespaceResponse, error) {
	_, err := s.source.GetNamespace(ctx, ns.Key)
	if err != nil {
		return nil, fmt.Errorf("update namespace %q: %w", ns.Key, ErrAlreadyExists)
	}

	resp := &configuration.NamespaceResponse{
		Namespace: &configuration.Namespace{
			Key:         ns.Key,
			Name:        ns.Name,
			Description: ns.Description,
			Protected:   ns.Protected,
		},
	}

	resp.Revision, err = s.source.PutNamespace(ctx, ns.GetRevision(), resp.Namespace)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *Server) DeleteNamespace(ctx context.Context, req *configuration.DeleteNamespaceRequest) (_ *configuration.DeleteNamespaceResponse, err error) {
	resp := &configuration.DeleteNamespaceResponse{}

	resp.Revision, err = s.source.DeleteNamespace(ctx, req.Revision, req.Key)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *Server) GetResource(ctx context.Context, req *configuration.GetResourceRequest) (r *configuration.ResourceResponse, err error) {
	return r, s.source.View(ctx, req.Type, func(ctx context.Context, sv ResourceStoreView) (err error) {
		r, err = sv.GetResource(ctx, req.Namespace, req.Key)
		if err != nil {
			return err
		}
		return err
	})
}

func (s *Server) ListResources(ctx context.Context, r *configuration.ListResourcesRequest) (rl *configuration.ListResourcesResponse, err error) {
	if err := s.source.View(ctx, r.Type, func(ctx context.Context, sv ResourceStoreView) (err error) {
		rl, err = sv.ListResources(ctx, r.Namespace)
		if err != nil {
			return err
		}

		return err
	}); err != nil {
		return nil, err
	}

	return rl, nil
}

func (s *Server) CreateResource(ctx context.Context, r *configuration.UpdateResourceRequest) (*configuration.ResourceResponse, error) {
	resp := &configuration.ResourceResponse{
		Resource: &configuration.Resource{
			Type:      r.Type,
			Namespace: r.Namespace,
			Key:       r.Key,
			Payload:   r.Payload,
		},
	}

	if err := s.source.Update(ctx, r.Revision, r.Type, func(ctx context.Context, sv ResourceStore) error {
		_, err := sv.GetResource(ctx, r.Namespace, r.Key)
		if err == nil {
			return fmt.Errorf(`create resource "%s/%s/%s": %w`, r.Type, r.Namespace, r.Key, ErrAlreadyExists)
		}

		if !errors.Is(err, ErrNotFound) {
			return err
		}

		resp.Revision, err = sv.PutResource(ctx, resp.Resource)
		return err
	}); err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *Server) UpdateResource(ctx context.Context, r *configuration.UpdateResourceRequest) (*configuration.ResourceResponse, error) {
	resp := &configuration.ResourceResponse{
		Resource: &configuration.Resource{
			Type:      r.Type,
			Namespace: r.Namespace,
			Key:       r.Key,
			Payload:   r.Payload,
		},
	}

	if err := s.source.Update(ctx, r.Revision, r.Type, func(ctx context.Context, sv ResourceStore) error {
		_, err := sv.GetResource(ctx, r.Namespace, r.Key)
		if err != nil {
			return fmt.Errorf(`update resource "%s/%s/%s": %w`, r.Type, r.Namespace, r.Key, err)
		}

		resp.Revision, err = sv.PutResource(ctx, resp.Resource)
		return err
	}); err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *Server) DeleteResource(ctx context.Context, req *configuration.DeleteResourceRequest) (*configuration.DeleteResourceResponse, error) {
	resp := &configuration.DeleteResourceResponse{}
	if err := s.source.Update(ctx, req.Revision, req.Type, func(ctx context.Context, sv ResourceStore) (err error) {
		resp.Revision, err = sv.DeleteResource(ctx, req.Namespace, req.Key)
		return err
	}); err != nil {
		return nil, err
	}

	return resp, nil
}
