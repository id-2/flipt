package local

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/go-git/go-billy/v5"
	serverconfig "go.flipt.io/flipt/internal/server/configuration"
	"go.flipt.io/flipt/internal/storage/configuration"
	rpcconfig "go.flipt.io/flipt/rpc/configuration"
)

type Source struct {
	mu     sync.RWMutex
	src    billy.Filesystem
	stores map[string]configuration.ResourceTypeStorage
}

func NewSource(src billy.Filesystem, stores map[string]configuration.ResourceTypeStorage) *Source {
	return &Source{src: src, stores: stores}
}

func (s *Source) GetNamespace(_ context.Context, key string) (*rpcconfig.NamespaceResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	info, err := s.src.Stat(key)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, serverconfig.ErrNotFound
		}

		return nil, err
	}

	if !info.IsDir() {
		return nil, serverconfig.ErrNotFound
	}

	return &rpcconfig.NamespaceResponse{
		Namespace: namespaceForKey(key),
	}, nil
}

func (s *Source) ListNamespaces(_ context.Context) (*rpcconfig.ListNamespacesResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nl := &rpcconfig.ListNamespacesResponse{}

	entries, err := s.src.ReadDir(".")
	if err != nil {
		return nil, err
	}

	for _, info := range entries {
		if !info.IsDir() {
			continue
		}

		nl.Items = append(nl.Items, namespaceForKey(info.Name()))
	}

	return nl, nil
}

func namespaceForKey(key string) *rpcconfig.Namespace {
	return &rpcconfig.Namespace{
		Key:         key,
		Name:        key,
		Description: ptr(fmt.Sprintf("The %s namespace", key)),
		Protected:   ptr(key == "default"),
	}
}

func (s *Source) PutNamespace(_ context.Context, rev string, ns *rpcconfig.Namespace) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.src.Stat(ns.Key)
	if err != nil {
		// does not exist yet so we're going to accept the write
		if errors.Is(err, os.ErrNotExist) {
			return s.createDir(ns.Key)
		}

		return "", err
	}

	return s.createDir(ns.Key)
}

func (s *Source) createDir(key string) (string, error) {
	if err := s.src.MkdirAll(key, 0755); err != nil {
		return "", err
	}

	_, err := s.src.Stat(key)
	if err != nil {
		return "", err
	}

	return "", nil
}

func (s *Source) DeleteNamespace(_ context.Context, rev, key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.src.Stat(key)
	if err != nil {
		// already removed
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}

		return "", err
	}

	if err := s.src.Remove(key); err != nil {
		return "", err
	}

	return "", nil
}

func (s *Source) View(ctx context.Context, typ string, fn serverconfig.ViewFunc) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rstore, ok := s.stores[typ]
	if !ok {
		return fmt.Errorf("resource type %q not recognized", typ)
	}

	return fn(ctx, &store{src: s.src, rstore: rstore})
}

func (s *Source) Update(ctx context.Context, rev, typ string, fn serverconfig.UpdateFunc) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rstore, ok := s.stores[typ]
	if !ok {
		return fmt.Errorf("resource type %q not recognized", typ)
	}

	return fn(ctx, &store{src: s.src, rstore: rstore})
}

type store struct {
	src    billy.Filesystem
	rstore configuration.ResourceTypeStorage
}

func (s *store) GetResource(ctx context.Context, namespace string, key string) (*rpcconfig.ResourceResponse, error) {
	resource, err := s.rstore.GetResource(ctx, s.src, namespace, key)
	if err != nil {
		return nil, err
	}

	return &rpcconfig.ResourceResponse{
		Resource: resource,
	}, nil
}

func (s *store) ListResources(ctx context.Context, namespace string) (*rpcconfig.ListResourcesResponse, error) {
	rs, err := s.rstore.ListResources(ctx, s.src, namespace)
	if err != nil {
		return nil, err
	}

	return &rpcconfig.ListResourcesResponse{
		Resources: rs,
	}, nil
}

func (s *store) PutResource(ctx context.Context, r *rpcconfig.Resource) (string, error) {
	if r.Payload.TypeUrl == "" {
		r.Payload.TypeUrl = r.Type
	}

	return "", s.rstore.PutResource(ctx, s.src, r)
}

func (s *store) DeleteResource(ctx context.Context, namespace string, key string) (string, error) {
	return "", s.rstore.DeleteResource(ctx, s.src, namespace, key)
}

func ptr[T any](t T) *T {
	return &t
}
