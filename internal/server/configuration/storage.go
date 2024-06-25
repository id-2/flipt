package configuration

import (
	"context"
	"errors"

	rpcconfig "go.flipt.io/flipt/rpc/configuration"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
	ErrConflict      = errors.New("conflict")
)

type ViewFunc func(context.Context, ResourceStoreView) error

type UpdateFunc func(context.Context, ResourceStore) error

type Source interface {
	GetNamespace(_ context.Context, key string) (*rpcconfig.NamespaceResponse, error)
	ListNamespaces(context.Context) (*rpcconfig.ListNamespacesResponse, error)
	PutNamespace(_ context.Context, rev string, _ *rpcconfig.Namespace) (string, error)
	DeleteNamespace(_ context.Context, rev, key string) (string, error)

	View(_ context.Context, typ string, fn ViewFunc) error
	Update(_ context.Context, rev, typ string, fn UpdateFunc) error
}

type ResourceStoreView interface {
	GetResource(_ context.Context, namespace, key string) (*rpcconfig.ResourceResponse, error)
	ListResources(_ context.Context, namespace string) (*rpcconfig.ListResourcesResponse, error)
}

type ResourceStore interface {
	ResourceStoreView

	PutResource(context.Context, *rpcconfig.Resource) (string, error)
	DeleteResource(_ context.Context, namespace, key string) (string, error)
}
