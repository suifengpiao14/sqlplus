package sqlplus

import (
	"context"

	"github.com/pkg/errors"
	"github.com/spf13/cast"
	"github.com/suifengpiao14/stream"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type ContextKey string

var (
	CONTEXT_NOT_FOUND_KEY = errors.New("not found key")
)

// SetKeyValue 记录key value到请求上下文
func SetKeyValue(ctx context.Context, key ContextKey, value string) (newCtx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx = context.WithValue(ctx, key, value)
	return ctx
}

func GetKeyValue(ctx context.Context, key ContextKey) (value string, err error) {
	v := ctx.Value(key)
	if v == nil {
		err = errors.WithMessagef(CONTEXT_NOT_FOUND_KEY, "key:%s", key)
		return "", err
	}
	value = cast.ToString(v)
	return value, nil
}

type GetValueFn func(ctx context.Context, key ContextKey, input []byte) (value string, err error)
type SetValueFn func(ctx context.Context, key ContextKey, value string, input []byte) (out []byte, err error)
type SetContext struct {
	Key   ContextKey
	GetFn GetValueFn
	SetFn SetValueFn
}

// SqlPlusPackHandlerSetContent 从输入流中提取指定key,value 到ctx中，在输出流中自动添加对应的key
func SqlPlusPackHandlerSetContent(setContexts ...SetContext) (packHandler stream.PackHandler, err error) {
	packHandler = stream.NewPackHandlerWithSetContext(func(ctx context.Context, input []byte) (newCtx context.Context, out []byte, err error) {
		for _, setContext := range setContexts {
			if setContext.GetFn != nil {
				value, err := setContext.GetFn(ctx, setContext.Key, input)
				if err != nil {
					return nil, nil, err
				}
				newCtx = SetKeyValue(ctx, setContext.Key, value)
			}
		}
		return newCtx, input, nil
	}, nil, func(ctx context.Context, input []byte) (out []byte, err error) {
		for _, setContext := range setContexts {
			if setContext.SetFn != nil {
				value, err := GetKeyValue(ctx, setContext.Key)
				if err != nil {
					return nil, err
				}
				out, err = setContext.SetFn(ctx, setContext.Key, value, input)
				if err != nil {
					return nil, err
				}
			}
		}

		return out, nil
	})
	return packHandler, nil
}

var (
	ERROR_Stream_Tenant_Not_Found = errors.New("not found tenant id")
)

// GetKeyValueJsonFn 从json字符串中获取指定key value
func GetKeyValueJsonFn(ctx context.Context, key string, input []byte) (tendId string, err error) {
	tenantId := gjson.GetBytes(input, key).String()
	if tenantId == "" {
		return "", ERROR_Stream_Tenant_Not_Found
	}
	return tenantId, nil
}

// SetKeyValueJsonFn 将 key value 设置到输出json流中
func SetKeyValueJsonFn(ctx context.Context, key string, value string, input []byte) (out []byte, err error) {
	out, err = sjson.SetBytes(input, key, value)
	if err != nil {
		return nil, err
	}
	return out, nil
}
