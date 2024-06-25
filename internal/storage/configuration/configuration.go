package configuration

import (
	"context"

	"github.com/go-git/go-billy/v5"
	rpcconfig "go.flipt.io/flipt/rpc/configuration"
)

type ResourceTypeStorage interface {
	GetResource(_ context.Context, _ billy.Filesystem, namespace, key string) (*rpcconfig.Resource, error)
	ListResources(_ context.Context, _ billy.Filesystem, namespace string) ([]*rpcconfig.Resource, error)
	PutResource(context.Context, billy.Filesystem, *rpcconfig.Resource) error
	DeleteResource(_ context.Context, _ billy.Filesystem, namespace, key string) error
}
