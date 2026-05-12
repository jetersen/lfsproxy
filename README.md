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

Route LFS requests through the proxy using Git's environment-based config (`GIT_CONFIG_COUNT`, available since Git 2.31). This requires no per-repo configuration.

### Single VCS root

For builds with one repo, set env vars directly:

```bash
export GIT_CONFIG_COUNT=1
export GIT_CONFIG_KEY_0=lfs.url
export GIT_CONFIG_VALUE_0=http://lfsproxy:9999/${ORG}/${REPO}.git/info/lfs
```

### Multiple VCS roots

When a build checks out multiple repos that use LFS, `GIT_CONFIG_*` env vars can't differentiate between them (they set a single global `lfs.url`).

The solution: disable automatic LFS checkout and pull manually with per-repo config.

**TeamCity example** with two VCS roots (`my-app` and `my-assets`):

1. Disable LFS during checkout — add agent parameter:
   ```
   teamcity.git.lfs.enabled=false
   ```

2. Add a build step (before any steps that need LFS content) to pull LFS for each repo:
   ```bash
   git -C "%system.teamcity.build.checkoutDir%" config lfs.url http://lfsproxy:9999/%GIT_OWNER%/%GIT_REPO_NAME%.git/info/lfs
   git -C "%system.teamcity.build.checkoutDir%" lfs pull

   git -C "%system.teamcity.build.checkoutDir%/path/to/assets" config lfs.url http://lfsproxy:9999/%GIT_OWNER%/%GIT_ASSETS_REPO%.git/info/lfs
   git -C "%system.teamcity.build.checkoutDir%/path/to/assets" lfs pull
   ```

### Jenkins

```groovy
environment {
    GIT_CONFIG_COUNT = '1'
    GIT_CONFIG_KEY_0 = 'lfs.url'
    GIT_CONFIG_VALUE_0 = "http://lfsproxy:9999/${env.GIT_URL.replaceAll('.*github.com[:/]', '').replaceAll('\\.git$', '')}.git/info/lfs"
}
```

## Installing

A [Helm chart](install/helm/lfsproxy) is provided for Kubernetes deployment. The service account must have an IAM role with S3 access (EKS Pod Identity or IRSA).
