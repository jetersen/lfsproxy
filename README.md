# LFS Proxy

A pull-through S3 cache for [Git LFS](https://git-lfs.com/) that caches upstream LFS objects on S3 to reduce bandwidth costs on hosted LFS services such as GitHub LFS.

Supports multiple repositories — the org and repo are inferred from the request path. Objects are stored by OID (content hash), so identical files across repos are deduplicated automatically.

Cached objects are served directly from S3 using presigned URLs for maximum performance.

Requests are cached in-memory using [bigcache](https://github.com/allegro/bigcache) to reduce HTTP calls to S3.

## Configuration

All configuration is loaded from environment variables using [envconfig](https://github.com/kelseyhightower/envconfig).

| Environment Variable             | Default | Description                                                        |
|----------------------------------|---------|--------------------------------------------------------------------|
| `APP_UPSTREAM_HOST`              |         | Upstream Git host (e.g. `https://github.com`)                      |
| `APP_ALLOWED_ORGS`              |         | Comma-separated list of allowed orgs (empty = allow all)           |
| `APP_S3_BUCKET`                  |         | S3 bucket name                                                     |
| `APP_S3_USE_ACCELERATE`          | `false` | Use S3 Transfer Acceleration                                       |
| `APP_S3_PRESIGN_ENABLED`         | `true`  | Use S3 presigned URLs                                              |
| `APP_S3_PRESIGN_EXPIRATION`      | `24h`   | Presigned URL expiration                                            |
| `APP_CACHE_EVICTION`             | `23h`   | In-memory cache eviction interval                                  |
| `APP_ENABLE_PROMETHEUS_EXPORTER` | `false` | Enable Prometheus metrics endpoint (`/metrics`)                    |
| `APP_DEBUG_MODE`                 | `false` | Enable gin debug mode                                              |

### Example

```bash
APP_UPSTREAM_HOST=https://github.com
APP_ALLOWED_ORGS=jetersen
APP_S3_BUCKET=my-lfs-cache
```

This configuration caches LFS objects for all repositories under the `jetersen` GitHub org.

## CI Setup

Use Git's environment-based config to route LFS requests through the proxy without any per-repo configuration:

```bash
export GIT_CONFIG_COUNT=1
export GIT_CONFIG_KEY_0=lfs.url
export GIT_CONFIG_VALUE_0=http://lfsproxy:9999/${ORG}/${REPO}.git/info/lfs
```

### TeamCity

Add these as build parameters (using existing `GIT_OWNER` and `GIT_REPO_NAME` parameters):

```
env.GIT_CONFIG_COUNT=1
env.GIT_CONFIG_KEY_0=lfs.url
env.GIT_CONFIG_VALUE_0=http://lfsproxy:9999/%GIT_OWNER%/%GIT_REPO_NAME%.git/info/lfs
```

### Jenkins

```groovy
environment {
    GIT_CONFIG_COUNT = '1'
    GIT_CONFIG_KEY_0 = 'lfs.url'
    GIT_CONFIG_VALUE_0 = "http://lfsproxy:9999/${env.GIT_URL.replaceAll('.*github.com[:/]', '').replaceAll('\\.git$', '')}.git/info/lfs"
}
```

### Per-repo (optional)

If you prefer per-repo config, add a `.lfsconfig` to the repository root:

```ini
[lfs]
    url = http://lfsproxy:9999/jetersen/lfs-test.git/info/lfs
```

## Installing

A [Helm chart](install/helm/lfsproxy) is provided for Kubernetes deployment. The service account must have an IAM role with S3 access (EKS Pod Identity or IRSA).
