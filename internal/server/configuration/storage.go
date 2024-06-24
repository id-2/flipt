package configuration

import (
	"context"
	"errors"

	"go.flipt.io/flipt/rpc/configuration"
)

var (
	ErrNotFound      = errors.New("not found")
	ErrAlreadyExists = errors.New("already exists")
)

type SourceOptions struct {
	Reference *string
}

type SourceOption func(*SourceOptions)

func WithReference(ref *string) SourceOption {
	return func(so *SourceOptions) {
		so.Reference = ref
	}
}

type ViewFunc func(context.Context, StoreView) error

type UpdateFunc func(context.Context, Store) error

type Source interface {
	View(_ context.Context, fn ViewFunc, _ ...SourceOption) error
	Update(_ context.Context, fn UpdateFunc, _ ...SourceOption) error
}

type StoreView interface {
	GetNamespace(_ context.Context, key string) (*configuration.Namespace, error)
	ListNamespaces(context.Context) (*configuration.NamespaceList, error)

	GetResource(_ context.Context, typ, namespace, key string) (*configuration.Resource, error)
	ListResources(_ context.Context, typ, namespace string) ([]*configuration.Resource, error)
}

type Store interface {
	StoreView

	PutNamespace(context.Context, *configuration.Namespace) error
	DeleteNamespace(_ context.Context, key string) error

	PutResource(context.Context, *configuration.Resource) error
	DeleteResource(_ context.Context, typ, namespace, key string) error
}
